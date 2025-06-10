import os
import psycopg2
from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import time
import logging
from pyspark.sql.functions import col, when

# Configuração de logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# Configurações do banco de dados via variáveis de ambiente
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Diretórios da camada Trusted via variáveis de ambiente
INPUT_CSV_DIR = os.getenv("INPUT_CSV_DIR")
TRUSTED_CSV_DIR = os.getenv("TRUSTED_CSV_DIR")

# Variáveis de controle de etapa
DATA_PROCESSAMENTO = date.today()
ETAPA_ATUAL = "trusted_ingestao_csv"
ETAPA_DEPENDENTE = "ingestao_raw"

# Parâmetros de tentativa
INTERVALO_ESPERA = 10  # segundos
MAX_TENTATIVAS = 30

def get_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )

def verificar_status(conn, etapa, data_processamento):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT status FROM pipeline_status
            WHERE etapa = %s AND data_processamento = %s
        """, (etapa, data_processamento))
        resultado = cur.fetchone()
        return resultado[0] if resultado else None

def registrar_status(conn, etapa, data_processamento, status):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO pipeline_status (etapa, data_processamento, status)
            VALUES (%s, %s, %s)
            ON CONFLICT (etapa, data_processamento)
            DO UPDATE SET status = EXCLUDED.status, updated_at = CURRENT_TIMESTAMP
        """, (etapa, data_processamento, status))
        conn.commit()

def aguardar_dependencia(conn, etapa_dependente, data_processamento):
    logging.info(f"Iniciando verificação da dependência: {etapa_dependente}")
    for tentativa in range(1, MAX_TENTATIVAS + 1):
        status = verificar_status(conn, etapa_dependente, data_processamento)
        if status == "COMPLETED":
            logging.info(f"Etapa dependente '{etapa_dependente}' concluída com sucesso.")
            return
        elif status == "FAILED":
            raise RuntimeError(f"Etapa dependente '{etapa_dependente}' falhou. Abortando execução.")
        else:
            logging.info(f"[Tentativa {tentativa}/{MAX_TENTATIVAS}] Etapa '{etapa_dependente}' ainda não concluída (Status: {status}). Aguardando {INTERVALO_ESPERA}s...")
            time.sleep(INTERVALO_ESPERA)
    raise TimeoutError(f"A etapa '{etapa_dependente}' não foi concluída após {MAX_TENTATIVAS} tentativas.")

def executar_ingestao_trusted_csv():
    spark = SparkSession.builder \
        .appName("TrustedIngestaoCSV") \
        .config("spark.memory.fraction", "0.6") \
        .config("spark.memory.storageFraction", "0.5") \
        .getOrCreate()

    logging.info(f"Lendo arquivos de: {INPUT_CSV_DIR}")

    schema = StructType([
        StructField("id", StringType(), True),
        StructField("tituloPincipal", StringType(), True),
        StructField("tituloOriginal", StringType(), True),
        StructField("anoLancamento", IntegerType(), True),
        StructField("tempoMinutos", StringType(), True),
        StructField("genero", StringType(), True),
        StructField("notaMedia", DoubleType(), True),
        StructField("numeroVotos", IntegerType(), True),
        StructField("generoArtista", StringType(), True),
        StructField("personagem", StringType(), True),
        StructField("nomeArtista", StringType(), True),
        StructField("anoNascimento", StringType(), True),
        StructField("anoFalecimento", StringType(), True),
        StructField("profissao", StringType(), True),
        StructField("titulosMaisConhecidos", StringType(), True)
    ])

    df_csv = spark.read \
        .option("header", True) \
        .option("recursiveFileLookup", "true") \
        .option("sep", "|") \
        .schema(schema) \
        .csv(INPUT_CSV_DIR)

    logging.info("Pré-visualização do schema de entrada dos dados:")
    df_csv.printSchema()

    logging.info("Pré-visualização dos dados lidos do CSV:")
    df_csv.show(5, truncate=False)

    colunas_necessarias = ["id", "tituloPincipal", "tituloOriginal", "anoLancamento", "tempoMinutos", "genero", "notaMedia"]
    colunas_renomeadas = {
        "id": "imdb_id",
        "tituloPincipal": "title",
        "tituloOriginal": "original_title",
        "anoLancamento": "release_year",
        "tempoMinutos": "runtime",
        "genero": "genre",
        "notaMedia": "vote_average"
    }

    df_selected = df_csv.select(*colunas_necessarias)

    for antiga, nova in colunas_renomeadas.items():
        df_selected = df_selected.withColumnRenamed(antiga, nova)

    # Tratar \N e valores zero
    df_final = df_selected.withColumn(
        "runtime",
        when(col("runtime") != "\\N", col("runtime").cast(IntegerType())).otherwise(None)
    ).withColumn(
        "genre",
        when(col("genre") != "\\N", col("genre")).otherwise("Unknown")
    ).withColumn(
        "vote_average",
        when((col("vote_average") != "\\N") & (col("vote_average") != 0.0), col("vote_average")).otherwise(None)
    ).filter(
        (col("release_year") >= 2015) & (col("release_year") <= 2022)
    ).dropDuplicates(["imdb_id"])

    output_path = os.path.join(
        TRUSTED_CSV_DIR,
        str(DATA_PROCESSAMENTO.year),
        str(DATA_PROCESSAMENTO.month).zfill(2),
        str(DATA_PROCESSAMENTO.day).zfill(2)
    )

    if df_final.rdd.isEmpty():
        logging.warning("Nenhum dado encontrado após o processamento. Verifique a origem.")
    else:
        logging.info(f"Gravando arquivos no diretório Trusted: {output_path}")
        df_final.write.mode("overwrite").partitionBy("release_year").parquet(output_path)
        logging.info("Ingestão CSV para camada Trusted concluída com sucesso.")

    spark.stop()

def main():
    conn = None
    try:
        logging.info("Início da Etapa Trusted: Ingestão de CSV")
        conn = get_connection()
        logging.info("Conectado ao banco de dados.")
        logging.info(f"Verificando status da etapa '{ETAPA_DEPENDENTE}'...")

        aguardar_dependencia(conn, ETAPA_DEPENDENTE, DATA_PROCESSAMENTO)

        registrar_status(conn, ETAPA_ATUAL, DATA_PROCESSAMENTO, "RUNNING")

        executar_ingestao_trusted_csv()

        registrar_status(conn, ETAPA_ATUAL, DATA_PROCESSAMENTO, "COMPLETED")
    except Exception as e:
        logging.error(f"Erro na execução da etapa '{ETAPA_ATUAL}': {str(e)}")
        if conn:
            registrar_status(conn, ETAPA_ATUAL, DATA_PROCESSAMENTO, "FAILED")
        raise
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()