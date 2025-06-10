import os
import logging
import time
import psycopg2
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, ArrayType

# Configuração de logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Configurações do banco de dados via variáveis de ambiente
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Diretórios de entrada/saída
RAW_TMDB_DIR = os.getenv("RAW_TMDB_DIR")
TRUSTED_TMDB_DIR = os.getenv("TRUSTED_TMDB_DIR")

# Nome da etapa atual e dependente
ETAPA_ATUAL = "trusted_ingestao_tmdb"
ETAPA_DEPENDENTE = "enriquecimento_tmdb"

# Data de processamento
DATA_PROCESSAMENTO = datetime.today()
INPUT_JSON_DIR = os.path.join(
    RAW_TMDB_DIR,
    str(DATA_PROCESSAMENTO.year),
    str(DATA_PROCESSAMENTO.month).zfill(2),
    str(DATA_PROCESSAMENTO.day).zfill(2)
)

def get_db_connection():
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )

def verificar_status_etapa_anterior(conn, etapa_anterior, data_processamento):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT status FROM pipeline_status
            WHERE etapa = %s AND data_processamento = %s
        """, (etapa_anterior, data_processamento))
        result = cur.fetchone()
        return result[0] if result else None

def atualizar_status(conn, etapa, data_processamento, status):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO pipeline_status (etapa, data_processamento, status)
            VALUES (%s, %s, %s)
            ON CONFLICT (etapa, data_processamento)
            DO UPDATE SET status = EXCLUDED.status, updated_at = CURRENT_TIMESTAMP
        """, (etapa, data_processamento, status))
        conn.commit()

def aguardar_dependencia(conn, etapa_dependente, data_processamento, max_tentativas=30, intervalo_segundos=10):
    logging.info(f"Consultando status da etapa '{etapa_dependente}' para a data {data_processamento}...")
    for tentativa in range(max_tentativas):
        status = verificar_status_etapa_anterior(conn, etapa_dependente, data_processamento)
        logging.info(f"Status retornado da etapa '{etapa_dependente}': {status}")
        if status == "COMPLETED":
            return
        elif status == "FAILED":
            raise RuntimeError(f"Etapa dependente '{etapa_dependente}' falhou. Abortando execução.")
        logging.info(f"Aguardando etapa '{etapa_dependente}'... Tentativa {tentativa + 1}")
        time.sleep(intervalo_segundos)
    raise TimeoutError(f"Tempo de espera excedido para etapa '{etapa_dependente}'.")

def executar_ingestao_tmdb():
    logging.info("Iniciando ingestão TMDB para camada Trusted...")
    spark = SparkSession.builder.appName("TrustedIngestaoTMDB").getOrCreate()

    schema = StructType([
        StructField("imdb_id", StringType(), True),
        StructField("id", IntegerType(), True),
        StructField("title", StringType(), True),
        StructField("original_title", StringType(), True),
        StructField("overview", StringType(), True),
        StructField("release_date", StringType(), True),
        StructField("genre_ids", ArrayType(IntegerType(), True), True),
        StructField("popularity", DoubleType(), True),
        StructField("vote_average", DoubleType(), True),
        StructField("original_language", StringType(), True),
        StructField("runtime", IntegerType(), True),
        StructField("revenue", DoubleType(), True),
        StructField("budget", DoubleType(), True),
        StructField("erro", StringType(), True)
    ])

    try:
        # Listar arquivos JSON para diagnóstico
        json_files = spark.sparkContext.wholeTextFiles(INPUT_JSON_DIR).keys().collect()
        logging.info(f"Arquivos JSON encontrados em {INPUT_JSON_DIR}: {json_files}")

        # Leitura do JSON com multiline e tolerância a erros
        df_tmdb = spark.read.option("multiline", "true").option("mode", "PERMISSIVE").schema(schema).json(INPUT_JSON_DIR)
    except Exception as e:
        logging.error(f"Erro ao ler arquivos JSON de {INPUT_JSON_DIR}: {e}")
        spark.stop()
        raise

    logging.info(f"Número de registros lidos do JSON (antes da filtragem): {df_tmdb.count()}")
    logging.info("Amostra dos dados brutos lidos do JSON:")
    df_tmdb.show(5, truncate=False)

    # Filtrar registros com erro e fora de 2015-2022
    df_tmdb_valid = df_tmdb.filter(col("erro").isNull()) \
        .filter(col("release_date").cast("string").substr(1, 4).cast("int").between(2015, 2022))

    logging.info(f"Número de registros válidos após filtragem de erros e período 2015-2022: {df_tmdb_valid.count()}")

    if df_tmdb_valid.rdd.isEmpty():
        logging.error(f"Nenhum dado válido encontrado em {INPUT_JSON_DIR}. Verifique o formato dos arquivos JSON e o schema.")
        spark.stop()
        raise RuntimeError("DataFrame vazio após filtragem. Abortando ingestão.")

    df_tmdb_valid = df_tmdb_valid.withColumn("release_date", col("release_date").cast(DateType())) \
        .withColumn("release_year", col("release_date").cast("string").substr(1, 4).cast(IntegerType())) \
        .withColumn("revenue", when(col("revenue") != 0.0, col("revenue")).otherwise(None)) \
        .withColumn("budget", when(col("budget") != 0.0, col("budget")).otherwise(None)) \
        .withColumn("year", col("release_date").cast("string").substr(1, 4).cast(IntegerType())) \
        .withColumn("month", col("release_date").cast("string").substr(6, 2).cast(IntegerType())) \
        .withColumn("day", col("release_date").cast("string").substr(9, 2).cast(IntegerType()))

    logging.info("Amostra dos dados TMDB processados:")
    df_tmdb_valid.show(5, truncate=False)

    output_path = os.path.join(
        TRUSTED_TMDB_DIR,
        str(DATA_PROCESSAMENTO.year),
        str(DATA_PROCESSAMENTO.month).zfill(2),
        str(DATA_PROCESSAMENTO.day).zfill(2)
    )

    logging.info(f"Gravando arquivos Parquet em: {output_path}")
    df_tmdb_valid.write.mode("overwrite").parquet(output_path)

    spark.stop()
    logging.info("Ingestão TMDB para camada Trusted concluída com sucesso.")

def main():
    conn = get_db_connection()
    try:
        logging.info(f"Verificando dependência: {ETAPA_DEPENDENTE}")
        aguardar_dependencia(conn, ETAPA_DEPENDENTE, DATA_PROCESSAMENTO.date())
        atualizar_status(conn, ETAPA_ATUAL, DATA_PROCESSAMENTO.date(), "RUNNING")
        executar_ingestao_tmdb()
        atualizar_status(conn, ETAPA_ATUAL, DATA_PROCESSAMENTO.date(), "COMPLETED")
    except Exception as e:
        logging.error(f"Erro durante a ingestão TMDB para Trusted: {str(e)}")
        atualizar_status(conn, ETAPA_ATUAL, DATA_PROCESSAMENTO.date(), "FAILED")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    main()