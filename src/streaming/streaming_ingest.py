"""
Ingestion Structured Streaming — variante "temps réel" pour compléter le batch Auto Loader.

Deux façons de tester ça en conditions de charge sans vrai flux IoT :
  1. Source `rate` de Spark pour générer un débit contrôlé (ex: 50 000 events/sec)
     et valider que le pipeline tient la charge.
  2. Rejeu du raw S3 en continu (Trigger.AvailableNow en boucle, ou fichiers déposés
     progressivement) pour simuler l'arrivée réelle de fichiers compteurs.

Ici : option 1, pour se concentrer sur le comportement du moteur streaming
(watermarking, fenêtrage, backpressure) indépendamment du débit d'écriture S3.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

CATALOG = "energy"


def run(spark: SparkSession, rows_per_second: int = 20000):
    raw_stream = spark.readStream.format("rate").option("rowsPerSecond", rows_per_second).load()

    # On dérive un flux de mesures synthétique à partir de la source `rate`
    simulated = (
        raw_stream
        .withColumn("site_id", F.concat(F.lit("SITE_"), (F.col("value") % 500000).cast("string")))
        .withColumn("reading_ts", F.col("timestamp"))
        .withColumn("consumption_kw", (F.rand() * 10 + 1))
        .withWatermark("reading_ts", "10 minutes")
    )

    # Agrégation fenêtrée — 1 minute glissante, pour un dashboard quasi temps réel
    windowed = (
        simulated.groupBy(F.window("reading_ts", "1 minute"), "site_id")
        .agg(F.avg("consumption_kw").alias("avg_consumption_kw"))
    )

    query = (
        windowed.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", "s3://energy-pipeline-demo/checkpoints/streaming_realtime/")
        .trigger(processingTime="30 seconds")
        .toTable(f"{CATALOG}.silver.realtime_consumption_windowed")
    )
    query.awaitTermination()


if __name__ == "__main__":
    spark = SparkSession.builder.appName("streaming_smart_meters").getOrCreate()
    run(spark)
