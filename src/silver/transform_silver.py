"""
Couche Silver — nettoyage, déduplication, enrichissement.

- Filtrage qualité : on isole les mesures valides (non nulles, positives, < 500 kW).
  Les lignes rejetées sont conservées dans une table dédiée (traçabilité).
- Déduplication : une seule ligne par (site_id, reading_ts).
- Enrichissement : jointure avec le référentiel des sites (broadcast, petite dimension).
"""
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

CATALOG = "energy_pipeline_ws"
SCHEMA = "raw"
BUCKET = "s3://energy-pipeline-ckanoute"


def run(spark: SparkSession):
    bronze = spark.table(f"{CATALOG}.{SCHEMA}.bronze_smart_meters")
    sites = spark.table(f"{CATALOG}.{SCHEMA}.site_reference")

    # 1. Filtrage qualité
    valid = bronze.filter(
        F.col("consumption_kw").isNotNull() &
        (F.col("consumption_kw") >= 0) &
        (F.col("consumption_kw") < 500)
    )

    # Lignes rejetées, tracées à part
    rejected = bronze.subtract(valid)
    (rejected.write.format("delta").mode("overwrite")
       .option("path", f"{BUCKET}/silver/rejected/")
       .option("overwriteSchema", "true")
       .saveAsTable(f"{CATALOG}.{SCHEMA}.silver_rejected"))

    # 2. Déduplication par fenêtre
    w = Window.partitionBy("site_id", "reading_ts").orderBy(F.col("consumption_kw"))
    deduped = (valid
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn"))

    # 3. Enrichissement (broadcast join sur la dimension sites)
    enriched = deduped.join(F.broadcast(sites), on="site_id", how="left")

    (enriched.write.format("delta").mode("overwrite")
       .option("path", f"{BUCKET}/silver/smart_meters/")
       .option("overwriteSchema", "true")
       .partitionBy("year", "month", "day")
       .saveAsTable(f"{CATALOG}.{SCHEMA}.silver_smart_meters"))

    print("Silver OK : silver_smart_meters + silver_rejected")


if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    run(spark)
