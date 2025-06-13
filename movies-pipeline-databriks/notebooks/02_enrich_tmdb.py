from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, pandas_udf
from pyspark.sql.types import StructType, StructField, StringType, FloatType
import pandas as pd
import requests
from datetime import datetime

# Inicializar Spark
spark = SparkSession.builder.appName("EnrichTMDB").getOrCreate()

# Configurações
input_path = "dbfs:/data-lake/Raw/Local/Delta/Movies/*/*/*/"
output_path = f"dbfs:/data-lake/Raw/API/Delta/TMDB/{datetime.now().strftime('%Y/%m/%d')}/"
cache_path = "dbfs:/data-lake/Cache/Delta/TMDB/"
error_path = "dbfs:/data-lake/Errors/Delta/TMDB/"
log_path = "dbfs:/data-lake/Logs/Status/"
api_key = dbutils.secrets.get("movies-pipeline", "tmdb-api-key")
FILTER_FINANCIAL = True

# Schema para dados TMDB
schema = StructType([
    StructField("tmdb_id", StringType()),
    StructField("title", StringType()),
    StructField("release_date", StringType()),
    StructField("budget", FloatType()),
    StructField("revenue", FloatType()),
    StructField("vote_average", FloatType()),
    StructField("genre_ids", StringType())
])

# Pandas UDF para chamar API TMDB
@pandas_udf(schema)
def tmdb_enrich(imdb_ids: pd.Series) -> pd.DataFrame:
    results = []
    for imdb_id in imdb_ids:
        try:
            response = requests.get(
                f"https://api.tmdb.org/3/find/{imdb_id}?api_key={api_key}&external_source=imdb_id"
            )
            if response.status_code == 200:
                data = response.json()
                movie = data.get("movie_results", [{}])[0]
                if movie:
                    tmdb_id = movie.get("id")
                    details = requests.get(
                        f"https://api.tmdb.org/3/movie/{tmdb_id}?api_key={api_key}"
                    ).json()
                    results.append({
                        "tmdb_id": str(tmdb_id),
                        "title": details.get("title"),
                        "release_date": details.get("release_date"),
                        "budget": float(details.get("budget", 0)),
                        "revenue": float(details.get("revenue", 0)),
                        "vote_average": float(details.get("vote_average", 0)),
                        "genre_ids": str(details.get("genres", []))
                    })
                else:
                    results.append(None)
            else:
                results.append(None)
        except Exception:
            results.append(None)
    return pd.DataFrame(results)

# Ler dados brutos
raw_df = spark.read.format("delta").load(input_path)

# Aplicar validações iniciais
filtered_df = raw_df.dropDuplicates(["id"]) \
                    .filter(col("id").startswith("tt")) \
                    .filter(col("anoLancamento").isNotNull() & (col("anoLancamento") != "\\N") & \
                            col("anoLancamento").cast("int").between(2015, 2022))

# Enriquecer com TMDB
enriched_df = filtered_df.withColumn("tmdb_data", tmdb_enrich(col("id")))

# Aplicar filtros adicionais
enriched_df = enriched_df.filter(col("tmdb_data").isNotNull()) \
                         .filter(year(col("tmdb_data.release_date")).between(2015, 2022))
if FILTER_FINANCIAL:
    enriched_df = enriched_df.filter((col("tmdb_data.budget") > 0) | (col("tmdb_data.revenue") > 0))

# Salvar dados enriquecidos
enriched_df.write.mode("overwrite").format("delta").partitionBy("year", "month", "day").save(output_path)

# Salvar erros
errors_df = filtered_df.join(enriched_df, "id", "left_anti").select("id")
errors_df.write.mode("append").format("delta").save(error_path)

# Atualizar cache
enriched_df.select("id", "tmdb_data").write.mode("overwrite").format("delta").save(cache_path)

# Registrar status
status_df = spark.createDataFrame([{
    "stage": "EnrichTMDB",
    "status": "Success",
    "timestamp": datetime.now()
}])
status_df.write.mode("append").format("delta").save(log_path)