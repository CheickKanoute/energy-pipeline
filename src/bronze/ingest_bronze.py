"""
Couche Bronze — ingestion des données brutes de compteurs vers une table Delta.

Lit les fichiers Parquet du raw (exposés via un Volume Unity Catalog) et les
matérialise en table Delta externe, sans transformation : Bronze est une copie
fidèle et immuable du raw, anomalies comprises. Le nettoyage est le travail de Silver.

Deux tables produites :
  - bronze_smart_meters : les 14,4M mesures
  - site_reference      : le référentiel des 5000 sites (dimension)
"""
from pyspark.sql import SparkSession

CATALOG = "energy_pipeline_ws"
SCHEMA = "raw"
BUCKET = "s3://energy-pipeline-ckanoute"
VOLUME_RAW = f"/Volumes/{CATALOG}/{SCHEMA}/smart_meters"


def run(spark: SparkSession):
    # 1. Mesures : lecture des partitions Parquet depuis le Volume gouverné
   df = spark.read.parquet(f"{VOLUME_RAW}/year=*/")
    (df.write.format("delta").mode("overwrite")
       .option("path", f"{BUCKET}/bronze/smart_meters/")
       .option("overwriteSchema", "true")
       .saveAsTable(f"{CATALOG}.{SCHEMA}.bronze_smart_meters"))

    # 2. Referentiel des sites (copie sans le prefixe "_" cote S3 pour que Spark le lise)
    sites = spark.read.parquet(f"{VOLUME_RAW}/site_reference.parquet")
    (sites.write.format("delta").mode("overwrite")
       .option("path", f"{BUCKET}/bronze/site_reference/")
       .option("overwriteSchema", "true")
       .saveAsTable(f"{CATALOG}.{SCHEMA}.site_reference"))

    print("Bronze OK : bronze_smart_meters + site_reference")


if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    run(spark)