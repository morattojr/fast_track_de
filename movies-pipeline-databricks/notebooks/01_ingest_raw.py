from pyspark.sql import SparkSession
import zipfile
import os
from datetime import datetime
from pyspark.sql.functions import lit

# Inicializar Spark
spark = SparkSession.builder.appName("IngestRaw").getOrCreate()

# Configurações
zip_path = "dbfs:/FileStore/input/movies.zip"
extract_path = "/tmp/movies"
output_path = f"dbfs:/data-lake/Raw/Local/Delta/Movies/{datetime.now().strftime('%Y/%m/%d')}/"
log_path = "dbfs:/data-lake/Logs/Status/"

# Criar diretório temporário
dbutils.fs.mkdirs("file:/tmp/movies")

# Descompactar ZIP
with zipfile.ZipFile(zip_path.replace("dbfs:/", "/dbfs/"), "r") as zip_ref:
    zip_ref.extractall(extract_path)

# Ler CSV
df = spark.read.option("delimiter", "|").option("header", "true").csv(f"file://{extract_path}/movies.csv")

# Salvar como Delta Table
df.write.mode("overwrite").format("delta").partitionBy("year", "month", "day").save(output_path)

# Registrar status
status_df = spark.createDataFrame([{
    "stage": "Raw",
    "status": "Success",
    "timestamp": datetime.now()
}])
status_df.write.mode("append").format("delta").save(log_path)

# Limpar temporários
dbutils.fs.rm("file:/tmp/movies", recurse=True)