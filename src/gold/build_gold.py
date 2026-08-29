"""
Couche Gold — agrégats métier + features pour le modèle de prévision.

Deux tables Gold :
  - gold.consumption_hourly_by_site : agrégat horaire par site (reporting)
  - gold.forecast_features : features temporelles pour le training MLflow
"""
from pyspark.sql import SparkSession, functions as F

CATALOG = "energy"


def run(spark: SparkSession):
    silver = spark.table(f"{CATALOG}.silver.smart_meter_readings")

    hourly = (
        silver.withColumn("reading_hour", F.date_trunc("hour", "reading_ts"))
        .groupBy("site_id", "site_type", "region", "reading_hour")
        .agg(
            F.avg("consumption_kw").alias("avg_consumption_kw"),
            F.max("consumption_kw").alias("peak_consumption_kw"),
            F.count("*").alias("nb_readings"),
        )
    )
    (
        hourly.write.mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("region")
        .saveAsTable(f"{CATALOG}.gold.consumption_hourly_by_site")
    )

    # Features temporelles simples : lags + moyennes mobiles, calculées par window
    # function plutôt que par self-join (beaucoup plus économe à grande échelle).
    from pyspark.sql import Window

    w = Window.partitionBy("site_id").orderBy("reading_hour")
    features = (
        hourly.withColumn("hour_of_day", F.hour("reading_hour"))
        .withColumn("day_of_week", F.dayofweek("reading_hour"))
        .withColumn("lag_1h", F.lag("avg_consumption_kw", 1).over(w))
        .withColumn("lag_24h", F.lag("avg_consumption_kw", 24).over(w))
        .withColumn("rolling_avg_24h", F.avg("avg_consumption_kw").over(w.rowsBetween(-24, -1)))
    )
    (
        features.write.mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("region")
        .saveAsTable(f"{CATALOG}.gold.forecast_features")
    )


if __name__ == "__main__":
    spark = SparkSession.builder.appName("gold_build_smart_meters").getOrCreate()
    run(spark)
