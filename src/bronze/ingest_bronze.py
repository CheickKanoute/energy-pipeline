"""
Ingestion Bronze — Databricks Auto Loader depuis S3.

Points volumétrie à démontrer en entretien :
- Auto Loader (cloudFiles) plutôt qu'un simple spark.read : découverte incrémentale
  des fichiers via notification S3 (SQS) au lieu du listing complet — indispensable
  dès que le raw contient des dizaines de milliers de fichiers.
- schemaEvolutionMode="addNewColumns" + schemaLocation pour survivre à l'ajout de
  colonnes côté source sans casser le pipeline.
- maxFilesPerTrigger pour contrôler la taille des micro-batchs et éviter un cluster
  sous-dimensionné qui OOM sur un backlog important.
"""
from pyspark.sql import SparkSession, functions as F

CATALOG = "energy"
SCHEMA = "bronze"
TABLE = "smart_meter_readings"

RAW_PATH = "s3://energy-pipeline-demo/raw/smart-meters/"
CHECKPOINT_PATH = "s3://energy-pipeline-demo/checkpoints/bronze_smart_meters/"
SCHEMA_LOCATION = "s3://energy-pipeline-demo/schemas/bronze_smart_meters/"


def run(spark: SparkSession):
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", SCHEMA_LOCATION)
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.maxFilesPerTrigger", 1000)  # ajuster selon la taille du cluster
        .load(RAW_PATH)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
    )

    query = (
        df.writeStream.format("delta")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)  # batch incrémental — passer à processingTime="5 minutes" en continu
        .toTable(f"{CATALOG}.{SCHEMA}.{TABLE}")
    )
    query.awaitTermination()


if __name__ == "__main__":
    spark = SparkSession.builder.appName("bronze_ingest_smart_meters").getOrCreate()
    run(spark)
