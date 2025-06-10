import os
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType

# Configuração de logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Configurações via variáveis de ambiente
TRUSTED_CSV_DIR = os.getenv("TRUSTED_CSV_DIR")
TRUSTED_TMDB_DIR = os.getenv("TRUSTED_TMDB_DIR")
REFINED_DIM_DIR = os.getenv("REFINED_DIM_DIR")
REFINED_FACT_DIR = os.getenv("REFINED_FACT_DIR")

# Data de processamento
DATA_PROCESSAMENTO = datetime.today()
OUTPUT_PATH_DIM = os.path.join(REFINED_DIM_DIR, str(DATA_PROCESSAMENTO.year), str(DATA_PROCESSAMENTO.month).zfill(2), str(DATA_PROCESSAMENTO.day).zfill(2))
OUTPUT_PATH_FACT = os.path.join(REFINED_FACT_DIR, str(DATA_PROCESSAMENTO.year), str(DATA_PROCESSAMENTO.month).zfill(2), str(DATA_PROCESSAMENTO.day).zfill(2))

def executar_modelagem_dimensional():
    logging.info("Iniciando modelagem dimensional para camada Refined...")
    spark = SparkSession.builder.appName("RefinedModelagemDimensional").getOrCreate()

    # Leitura dos dados Trusted
    input_csv_path = os.path.join(TRUSTED_CSV_DIR, str(DATA_PROCESSAMENTO.year), str(DATA_PROCESSAMENTO.month).zfill(2), str(DATA_PROCESSAMENTO.day).zfill(2))
    input_tmdb_path = os.path.join(TRUSTED_TMDB_DIR, str(DATA_PROCESSAMENTO.year), str(DATA_PROCESSAMENTO.month).zfill(2), str(DATA_PROCESSAMENTO.day).zfill(2))

    try:
        df_csv = spark.read.parquet(input_csv_path)
        df_tmdb = spark.read.parquet(input_tmdb_path)
    except Exception as e:
        logging.error(f"Erro ao ler dados Trusted: {e}")
        spark.stop()
        raise

    # Inspecionar schemas
    logging.info("Schema de df_csv:")
    df_csv.printSchema()
    logging.info("Schema de df_tmdb:")
    df_tmdb.printSchema()

    # Criação da dimensão dim_movie
    df_dim_movie = df_tmdb.join(df_csv, df_tmdb.imdb_id == df_csv.imdb_id, "inner") \
        .select(
            df_tmdb.imdb_id.alias("movie_id"),
            df_tmdb.title,
            df_tmdb.original_title,
            df_tmdb.release_date,
            df_tmdb.release_year,
            df_tmdb.genre_ids,
            df_tmdb.popularity,
            df_tmdb.vote_average,
            df_tmdb.original_language,
            df_tmdb.runtime,
            df_csv.genre.alias("genres_csv"),
            df_csv.runtime.alias("runtime_csv")
        ) \
        .filter(col("release_year").between(2015, 2022))

    logging.info(f"Número de registros em dim_movie: {df_dim_movie.count()}")
    df_dim_movie.show(5, truncate=False)

    # Criação da fato fact_financials
    df_fact_financials = df_tmdb.select(
        df_tmdb.imdb_id.alias("movie_id"),
        df_tmdb.revenue,
        df_tmdb.budget,
        lit(DATA_PROCESSAMENTO.date()).alias("processing_date")
    ).filter(col("release_year").between(2015, 2022))

    logging.info(f"Número de registros em fact_financials: {df_fact_financials.count()}")
    df_fact_financials.show(5, truncate=False)

    # Gravação dos resultados
    logging.info(f"Gravando dim_movie em: {OUTPUT_PATH_DIM}")
    df_dim_movie.write.mode("overwrite").parquet(OUTPUT_PATH_DIM)

    logging.info(f"Gravando fact_financials em: {OUTPUT_PATH_FACT}")
    df_fact_financials.write.mode("overwrite").parquet(OUTPUT_PATH_FACT)

    spark.stop()
    logging.info("Modelagem dimensional concluída com sucesso.")

def main():
    try:
        executar_modelagem_dimensional()
    except Exception as e:
        logging.error(f"Erro durante a modelagem dimensional: {str(e)}")
        raise

if __name__ == "__main__":
    main()