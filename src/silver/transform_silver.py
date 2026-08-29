"""
Transformation Silver — nettoyage, déduplication, jointure référentiel, merge Delta.

Points volumétrie à démontrer en entretien :
- MERGE INTO plutôt qu'un overwrite complet : indispensable dès que Bronze contient
  des mois d'historique — on ne veut réécrire que les partitions concernées.
- Dédup par fenêtre (row_number) sur (site_id, reading_ts) avant le merge, pour
  gérer les doublons liés aux retries d'ingestion.
- Broadcast join explicite sur le référentiel sites (petite dimension) pour éviter
  un shuffle inutile sur une table de faits qui peut peser plusieurs To.
- Partitionnement par date (year/month/day) hérité de Bronze, cohérent avec les
  requêtes downstream (agrégats journaliers/horaires).
"""
from pyspark.sql import SparkSession, functions as F, Window
from delta.tables import DeltaTable

CATALOG = "energy"


def run(spark: SparkSession):
    bronze = spark.table(f"{CATALOG}.bronze.smart_meter_readings")
    sites_ref = spark.table(f"{CATALOG}.bronze.site_reference")  # petite table (dimension)

    # 1. Filtrage qualité : on isole plutôt qu'on supprime silencieusement
    #    (traçabilité — un vrai client demandera toujours "où sont passées mes lignes rejetées ?")
    valid = bronze.filter(
        F.col("consumption_kw").isNotNull() & (F.col("consumption_kw") >= 0) & (F.col("consumption_kw") < 500)
    )
    rejected = bronze.subtract(valid)
    rejected.write.mode("append").saveAsTable(f"{CATALOG}.silver.rejected_readings")

    # 2. Déduplication par fenêtre — garde la dernière ingestion en cas de doublon
    w = Window.partitionBy("site_id", "reading_ts").orderBy(F.col("_ingested_at").desc())
    deduped = (
        valid.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    # 3. Jointure référentiel avec broadcast explicite (dimension petite)
    enriched = deduped.join(F.broadcast(sites_ref), on="site_id", how="left")

    # 4. Merge idempotent — ne réécrit que les partitions touchées
    target_table = f"{CATALOG}.silver.smart_meter_readings"
    if spark.catalog.tableExists(target_table):
        target = DeltaTable.forName(spark, target_table)
        (
            target.alias("t")
            .merge(enriched.alias("s"), "t.site_id = s.site_id AND t.reading_ts = s.reading_ts")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        (
            enriched.write.format("delta")
            .partitionBy("year", "month", "day")
            .saveAsTable(target_table)
        )

    # 5. Maintenance — Z-Ordering sur la colonne de filtrage la plus fréquente (site_id)
    #    à planifier périodiquement (pas à chaque run), typiquement une fois par jour.
    spark.sql(f"OPTIMIZE {target_table} ZORDER BY (site_id)")


if __name__ == "__main__":
    spark = SparkSession.builder.appName("silver_transform_smart_meters").getOrCreate()
    run(spark)
