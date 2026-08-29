"""
Couche Gold — agrégats métier + features pour le forecasting.

Deux tables :
  - gold_consumption_hourly : consommation agrégée par site et par heure (reporting)
  - gold_forecast_features  : features temporelles (lags, moyenne mobile) pour la prévision
"""
from pyspark.sql import SparkSession, functions as F, Window

CATALOG = "energy_pipeline_ws"
SCHEMA = "raw"
BUCKET = "s3://energy-pipeline-ckanoute"


def run(spark: SparkSession):
    silver = spark.table(f"{CATALOG}.{SCHEMA}.silver_smart_meters")

    # === Table 1 : agrégat horaire par site ===
    hourly = (
        silver
        .withColumn("reading_hour", F.date_trunc("hour", "reading_ts"))
        .groupBy("site_id", "site_type", "region", "reading_hour")
        .agg(
            F.avg("consumption_kw").alias("avg_consumption_kw"),
            F.max("consumption_kw").alias("peak_consumption_kw"),
            F.count("*").alias("nb_readings")
        )
    )
    (hourly.write.format("delta").mode("overwrite")
       .option("path", f"{BUCKET}/gold/consumption_hourly/")
       .option("overwriteSchema", "true")
       .partitionBy("region")
       .saveAsTable(f"{CATALOG}.{SCHEMA}.gold_consumption_hourly"))

    # === Table 2 : features pour le forecasting ===
    w = Window.partitionBy("site_id").orderBy("reading_hour")
    features = (
        hourly
        .withColumn("hour_of_day", F.hour("reading_hour"))
        .withColumn("day_of_week", F.dayofweek("reading_hour"))
        .withColumn("lag_1h", F.lag("avg_consumption_kw", 1).over(w))
        .withColumn("lag_24h", F.lag("avg_consumption_kw", 24).over(w))
        .withColumn("rolling_avg_24h", F.avg("avg_consumption_kw").over(w.rowsBetween(-24, -1)))
    )
    (features.write.format("delta").mode("overwrite")
       .option("path", f"{BUCKET}/gold/forecast_features/")
       .option("overwriteSchema", "true")
       .partitionBy("region")
       .saveAsTable(f"{CATALOG}.{SCHEMA}.gold_forecast_features"))

    print("Gold OK : gold_consumption_hourly + gold_forecast_features")


if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    run(spark)
