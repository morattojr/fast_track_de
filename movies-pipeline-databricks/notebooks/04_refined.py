from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_date
from datetime import datetime

# Inicializar Spark
spark = SparkSession.builder.appName("Refined").getOrCreate()

# Configurações
trusted_csv_path = "dbfs:/data-lake/Trusted/Delta/Movies/*/*/*/"
trusted_tmdb_path = "dbfs:/data-lake/Trusted/Delta/TMDB/*/*/*/"
dim_path = f"dbfs:/data-lake/Refined/Delta/Dim/Movies/{datetime.now().strftime('%Y/%m/%d')}/"
fact_path = f"dbfs:/data-lake/Refined/Delta/Fact/Financials/{datetime.now().strftime('%Y/%m/%d')}/"
log_path = "dbfs:/data-lake/Logs/Status/"

# Criar dim_movie
csv_df = spark.read.format("delta").load(trusted_csv_path)
tmdb_df = spark.read.format("delta").load(trusted_tmdb_path)
dim_movie_df = csv_df.join(tmdb_df, csv_df.id == tmdb_df.id, "inner") \
                     .select(
                         col("id").alias("movie_id"),
                         col("tituloPrincipal").alias("title"),
                         col("tmdb_data.genre_ids"),
                         col("tmdb_data.vote_average"),
                         col("tmdb_data.release_date")
                     )
dim_movie_df.write.mode("overwrite").format("delta").partitionBy("year", "month", "day").save(dim_path)

# Criar fact_financials
fact_financials_df = tmdb_df.select(
    col("id").alias("movie_id"),
    col("tmdb_data.budget").alias("budget"),
    col("tmdb_data.revenue").alias("revenue"),
    current_date().alias("processing_date")
)
fact_financials_df.write.mode("overwrite").format("delta").partitionBy("year", "month", "day").save(fact_path)

# Registrar status
status_df = spark.createDataFrame([{
    "stage": "Refined",
    "status": "Success",
    "timestamp": datetime.now()
}])
status_df.write.mode("append").format("delta").save(log_path)