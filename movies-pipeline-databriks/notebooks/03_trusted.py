from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year
from datetime import datetime

# Inicializar Spark
spark = SparkSession.builder.appName("Trusted").getOrCreate()

# Configurações
raw_csv_path = "dbfs:/data-lake/Raw/Local/Delta/Movies/*/*/*/"
raw_tmdb_path = "dbfs:/data-lake/Raw/API/Delta/TMDB/*/*/*/"
trusted_csv_path = f"dbfs:/data-lake/Trusted/Delta/Movies/{datetime.now().strftime('%Y/%m/%d')}/"
trusted_tmdb_path = f"dbfs:/data-lake/Trusted/Delta/TMDB/{datetime.now().strftime('%Y/%m/%d')}/"
log_path = "dbfs:/data-lake/Logs/Status/"

# Processar CSV
csv_df = spark.read.format("delta").load(raw_csv_path)
trusted_csv_df = csv_df.withColumn("anoLancamento", col("anoLancamento").cast("integer")) \
                       .filter(col("anoLancamento").between(2015, 2022))
trusted_csv_df.write.mode("overwrite").format("delta").partitionBy("year", "month", "day").save(trusted_csv_path)

# Processar TMDB
tmdb_df = spark.read.format("delta").load(raw_tmdb_path)
trusted_tmdb_df = tmdb_df.filter(col("tmdb_data").isNotNull()) \
                         .withColumn("year", year(col("tmdb_data.release_date"))) \
                         .filter(col("year").between(2015, 2022))
trusted_tmdb_df.write.mode("overwrite").format("delta").partitionBy("year", "month", "day").save(trusted_tmdb_path)

# Registrar status
status_df = spark.createDataFrame([{
    "stage": "Trusted",
    "status": "Success",
    "timestamp": datetime.now()
}])
status_df.write.mode("append").format("delta").save(log_path)