import os
import logging
from datetime import datetime
from pyspark.sql import SparkSession

# Configuração de logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Configurações via variáveis de ambiente
REFINED_DIM_DIR = os.getenv("REFINED_DIM_DIR")
REFINED_FACT_DIR = os.getenv("REFINED_FACT_DIR")

# Data de processamento
DATA_PROCESSAMENTO = datetime.today()
INPUT_DIM_PATH = os.path.join(REFINED_DIM_DIR, str(DATA_PROCESSAMENTO.year), str(DATA_PROCESSAMENTO.month).zfill(2), str(DATA_PROCESSAMENTO.day).zfill(2))
INPUT_FACT_PATH = os.path.join(REFINED_FACT_DIR, str(DATA_PROCESSAMENTO.year), str(DATA_PROCESSAMENTO.month).zfill(2), str(DATA_PROCESSAMENTO.day).zfill(2))

def executar_validacao():
    logging.info("Iniciando validação da modelagem dimensional...")
    spark = SparkSession.builder.appName("ValidacaoModelagem").getOrCreate()

    try:
        # Leitura das tabelas
        df_dim = spark.read.parquet(INPUT_DIM_PATH)
        df_fact = spark.read.parquet(INPUT_FACT_PATH)
    except Exception as e:
        logging.error(f"Erro ao ler dados Refined: {e}")
        spark.stop()
        raise

    # Criar visões temporárias
    df_dim.createOrReplaceTempView("dim_movie")
    df_fact.createOrReplaceTempView("fact_financials")

    # Validação: Contagem de registros
    dim_count = spark.sql("SELECT COUNT(*) AS count FROM dim_movie").collect()[0]["count"]
    fact_count = spark.sql("SELECT COUNT(*) AS count FROM fact_financials").collect()[0]["count"]
    logging.info(f"Total de registros em dim_movie: {dim_count}")
    logging.info(f"Total de registros em fact_financials: {fact_count}")

    # Validação: Período 2015-2022
    dim_years = spark.sql("SELECT DISTINCT release_year FROM dim_movie WHERE release_year IS NOT NULL").collect()
    invalid_years = [row["release_year"] for row in dim_years if row["release_year"] not in range(2015, 2023)]
    if not invalid_years:  
        logging.info("Todos os anos em dim_movie estão entre 2015 e 2022.")
    else:
        logging.error(f"Anos fora do intervalo 2015-2022 encontrados em dim_movie: {invalid_years}")

    # Validação: Integridade referencial
    invalid_keys = spark.sql("""
        SELECT f.movie_id
        FROM fact_financials f
        LEFT JOIN dim_movie d ON f.movie_id = d.movie_id
        WHERE d.movie_id IS NULL
    """).count()
    if invalid_keys == 0:
        logging.info("Integridade referencial válida entre fact_financials e dim_movie.")
    else:
        logging.error(f"{invalid_keys} chaves em fact_financials não existem em dim_movie.")

    spark.stop()
    logging.info("Validação concluída com sucesso.")

def main():
    try:
        executar_validacao()
    except Exception as e:
        logging.error(f"Erro durante a validação: {str(e)}")
        raise

if __name__ == "__main__":
    main()