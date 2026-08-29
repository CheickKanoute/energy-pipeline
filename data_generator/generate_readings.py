"""
Générateur de données synthétiques de compteurs intelligents (smart meters).

Objectif : produire un volume de données réaliste et paramétrable pour se mettre
en "conditions client" — de quelques millions à plusieurs milliards de lignes —
directement dans une structure partitionnée compatible S3 (year=/month=/day=/hour=).

Deux modes :
  - MODE PANDAS/NUMPY : pratique jusqu'à ~50-100M lignes sur un poste standard.
  - MODE PYSPARK : au-delà, utiliser generate_readings_spark() pour paralléliser
    la génération elle-même (voir bas de fichier), ce qui est aussi une bonne
    façon de s'entraîner à écrire du PySpark distribué plutôt que du pandas.

Usage (mode pandas, génération locale) :
    python generate_readings.py --sites 5000 --days 30 --freq-minutes 15 --out ./raw

Usage (mode Spark, gros volumes) :
    spark-submit generate_readings.py --engine spark --sites 2000000 --days 90 \
        --freq-minutes 60 --out s3://mon-bucket/raw/smart-meters

Volumétrie indicative :
    5 000 sites × 30 jours × 96 mesures/jour (15 min)  ≈ 14.4M lignes   (dev rapide)
    200 000 sites × 90 jours × 24 mesures/jour (1h)     ≈ 432M lignes   (charge "client")
    2 000 000 sites × 180 jours × 24 mesures/jour (1h)  ≈ 8.6 Md lignes (stress test réel)
"""
import argparse
import os
import numpy as np
import pandas as pd


SITE_TYPES = ["résidentiel", "tertiaire", "industriel"]
SITE_TYPE_WEIGHTS = [0.75, 0.20, 0.05]
REGIONS = ["IDF", "ARA", "NAQ", "PACA", "HDF", "GES", "OCC", "BFC"]


def build_site_reference(n_sites: int, seed: int = 42) -> pd.DataFrame:
    """Référentiel des sites/compteurs — dimension quasi-statique, jointe au fait."""
    rng = np.random.default_rng(seed)
    site_types = rng.choice(SITE_TYPES, size=n_sites, p=SITE_TYPE_WEIGHTS)
    base_load = np.select(
        [site_types == "résidentiel", site_types == "tertiaire", site_types == "industriel"],
        [rng.normal(3.0, 0.8, n_sites), rng.normal(15.0, 5.0, n_sites), rng.normal(80.0, 30.0, n_sites)],
    )
    return pd.DataFrame({
        "site_id": [f"SITE_{i:08d}" for i in range(n_sites)],
        "site_type": site_types,
        "region": rng.choice(REGIONS, size=n_sites),
        "base_load_kw": np.clip(base_load, 0.2, None),
        "install_date": pd.Timestamp("2022-01-01") + pd.to_timedelta(rng.integers(0, 700, n_sites), unit="D"),
    })


def generate_chunk(sites_df: pd.DataFrame, start_ts: pd.Timestamp, periods: int,
                    freq_minutes: int, seed: int) -> pd.DataFrame:
    """Génère un chunk de mesures pour tous les sites sur `periods` pas de temps.

    Volontairement vectorisé (numpy) plutôt qu'en boucle Python — à ce volume,
    une boucle par ligne serait ingérable ; c'est le même réflexe qu'en PySpark
    où on raisonne par colonnes/opérations vectorisées, jamais par row.
    """
    rng = np.random.default_rng(seed)
    n_sites = len(sites_df)
    timestamps = pd.date_range(start_ts, periods=periods, freq=f"{freq_minutes}min")

    # Profil journalier simplifié (pic matin/soir pour le résidentiel)
    hours = timestamps.hour.values
    daily_profile = 1.0 + 0.4 * np.sin((hours - 7) / 24 * 2 * np.pi) + 0.3 * np.sin((hours - 19) / 24 * 2 * np.pi)

    # Cartésien site x timestamp — construit par blocs pour limiter le pic mémoire
    site_idx = np.tile(np.arange(n_sites), periods)
    ts_idx = np.repeat(np.arange(periods), n_sites)

    noise = rng.normal(1.0, 0.12, size=n_sites * periods)
    base = sites_df["base_load_kw"].values[site_idx]
    profile = daily_profile[ts_idx]

    consumption_kw = np.clip(base * profile * noise, 0, None)

    # Injection volontaire d'anomalies (données manquantes / valeurs aberrantes)
    # pour donner un vrai sujet de qualité de données en Silver.
    anomaly_mask = rng.random(len(consumption_kw)) < 0.002
    consumption_kw[anomaly_mask] = consumption_kw[anomaly_mask] * rng.uniform(5, 20, anomaly_mask.sum())
    null_mask = rng.random(len(consumption_kw)) < 0.001

    df = pd.DataFrame({
        "site_id": sites_df["site_id"].values[site_idx],
        "reading_ts": timestamps.values[ts_idx],
        "consumption_kw": consumption_kw,
    })
    df.loc[null_mask, "consumption_kw"] = np.nan
    return df


def write_partitioned_parquet(df: pd.DataFrame, out_dir: str):
    """Écrit en respectant un layout year=/month=/day=/ compatible S3 + Glue partitions."""
    df["year"] = df["reading_ts"].dt.year
    df["month"] = df["reading_ts"].dt.month
    df["day"] = df["reading_ts"].dt.day
    for (y, m, d), part in df.groupby(["year", "month", "day"]):
        path = os.path.join(out_dir, f"year={y}", f"month={m:02d}", f"day={d:02d}")
        os.makedirs(path, exist_ok=True)
        part.drop(columns=["year", "month", "day"]).to_parquet(
            os.path.join(path, f"part-{np.random.randint(1e9):09d}.parquet"), index=False
        )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sites", type=int, default=5000)
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--freq-minutes", type=int, default=15)
    parser.add_argument("--chunk-days", type=int, default=1, help="taille de chunk pour limiter la RAM")
    parser.add_argument("--out", type=str, default="./raw")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    os.makedirs(args.out, exist_ok=True)
    sites_df = build_site_reference(args.sites, seed=args.seed)
    sites_df.to_parquet(os.path.join(args.out, "_site_reference.parquet"), index=False)

    periods_per_day = int(24 * 60 / args.freq_minutes)
    start = pd.Timestamp("2026-01-01")
    total_rows = 0
    for day_offset in range(0, args.days, args.chunk_days):
        chunk_start = start + pd.Timedelta(days=day_offset)
        periods = periods_per_day * args.chunk_days
        chunk = generate_chunk(sites_df, chunk_start, periods, args.freq_minutes,
                                seed=args.seed + day_offset)
        write_partitioned_parquet(chunk, args.out)
        total_rows += len(chunk)
        print(f"[{day_offset + args.chunk_days}/{args.days} jours] "
              f"{len(chunk):,} lignes écrites — total {total_rows:,}")

    print(f"Terminé. {total_rows:,} lignes générées pour {args.sites:,} sites dans {args.out}")


def generate_readings_spark(spark, n_sites: int, days: int, freq_minutes: int, out_path: str):
    """Variante PySpark pour les très gros volumes (centaines de millions à Md lignes).

    Idée : générer un DataFrame de type range() puis dériver les colonnes par
    expressions Spark (pas de collect, pas de boucle Python). À utiliser sur un
    cluster Databricks — c'est aussi un bon exercice pour pratiquer repartition/
    partitionBy en amont d'une écriture Delta volumineuse.
    """
    from pyspark.sql import functions as F

    periods_per_day = int(24 * 60 / freq_minutes)
    total_periods = periods_per_day * days

    sites = spark.range(n_sites).withColumnRenamed("id", "site_idx") \
        .withColumn("site_id", F.format_string("SITE_%08d", F.col("site_idx"))) \
        .withColumn("base_load_kw", (F.rand() * 20 + 1))

    timestamps = spark.range(total_periods).withColumnRenamed("id", "ts_idx") \
        .withColumn("reading_ts",
                     F.expr(f"timestamp('2026-01-01') + make_interval(0,0,0,0,0,{freq_minutes},0) * ts_idx"))

    df = sites.crossJoin(timestamps) \
        .withColumn("consumption_kw", F.col("base_load_kw") * (F.rand() * 0.3 + 0.85)) \
        .withColumn("year", F.year("reading_ts")) \
        .withColumn("month", F.month("reading_ts")) \
        .withColumn("day", F.dayofmonth("reading_ts"))

    # repartition avant écriture pour éviter le petit-fichier syndrome sur S3
    df.repartition("year", "month", "day") \
      .write.mode("append") \
      .partitionBy("year", "month", "day") \
      .parquet(out_path)


if __name__ == "__main__":
    main()
