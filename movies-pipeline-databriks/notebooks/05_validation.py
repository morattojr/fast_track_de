from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from datetime import datetime

# Inicializar Spark
spark = SparkSession.builder.appName("Validation").getOrCreate()

# Configurações
dim_path = "dbfs:/data-lake/Refined/Delta/Dim/Movies/*/*/*/"
fact_path = "dbfs:/data-lake/Refined/Delta/Fact/Financials/*/*/*/"
log_path = "dbfs:/data-lake/Logs/Status/"

# Ler tabelas
dim_movie_df = spark.read.format("delta").load(dim_path)
fact_financials_df = spark.read.format("delta").load(fact_path)

# Contagem de registros
dim_count = dim_movie_df.count()
fact_count = fact_financials_df.count()
print(f"Registros em dim_movie: {dim_count}, fact_financials: {fact_count}")

# Validar anos
invalid_years = dim_movie_df.filter(~col("year").between(2015, 2022)).count()
if invalid_years > 0:
    print(f"Erro: {invalid_years} registros com anos inválidos")

# Validar integridade referencial
invalid_refs = fact_financials_df.join(dim_movie_df, "movie_id", "left_anti").count()
if invalid_refs > 0:
    print(f"Erro: {invalid_refs} movie_ids sem correspondência em dim_movie")

# Registrar status
status = "Success" if invalid_years == 0 and invalid_refs == 0 else "Failed"
status_df = spark.createDataFrame([{
    "stage": "Validation",
    "status": status,
    "timestamp": datetime.now()
}])
status_df.write.mode("append").format("delta").save(log_path)