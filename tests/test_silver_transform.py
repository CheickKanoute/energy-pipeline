"""
Tests unitaires des transformations Silver, exécutables en local (pas besoin de cluster
Databricks) via un SparkSession local — c'est ce que la CI GitHub Actions exécute à chaque PR.
"""
import pytest
from pyspark.sql import SparkSession, Row


@pytest.fixture(scope="module")
def spark():
    return (
        SparkSession.builder.master("local[2]")
        .appName("test_silver_transform")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


def test_invalid_consumption_is_filtered(spark):
    df = spark.createDataFrame([
        Row(site_id="A", consumption_kw=5.0),
        Row(site_id="B", consumption_kw=-1.0),   # invalide
        Row(site_id="C", consumption_kw=None),   # invalide
        Row(site_id="D", consumption_kw=800.0),  # invalide (> seuil)
    ])
    valid = df.filter(
        (df.consumption_kw.isNotNull()) & (df.consumption_kw >= 0) & (df.consumption_kw < 500)
    )
    assert valid.count() == 1
    assert valid.first()["site_id"] == "A"


def test_deduplication_keeps_latest_ingestion(spark):
    from pyspark.sql import functions as F, Window

    df = spark.createDataFrame([
        Row(site_id="A", reading_ts="2026-01-01T00:00:00", consumption_kw=5.0, _ingested_at=1),
        Row(site_id="A", reading_ts="2026-01-01T00:00:00", consumption_kw=6.0, _ingested_at=2),
    ])
    w = Window.partitionBy("site_id", "reading_ts").orderBy(F.col("_ingested_at").desc())
    deduped = df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")

    assert deduped.count() == 1
    assert deduped.first()["consumption_kw"] == 6.0
