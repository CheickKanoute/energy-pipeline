# Energy Pipeline — Databricks / AWS / Azure (portfolio project)

Pipeline data bout en bout simulant un cas d'usage **secteur énergie** :
ingestion de données de compteurs intelligents (smart meters) à grande échelle,
traitement Bronze/Silver/Gold sur Databricks, prévision de consommation avec MLflow,
CI/CD, orchestration, et gouvernance Unity Catalog.

Conçu pour se préparer à une mission type "Ingénieur Data Databricks AWS/Azure — secteur énergie".

## Objectif volumétrie

Le projet est pensé pour être testé à deux échelles :
- **Échelle dev/local** : ~1 à 5 millions de lignes, pour itérer vite (quelques minutes).
- **Échelle "client"** : 200M à plusieurs milliards de lignes simulant plusieurs
  millions de compteurs sur plusieurs mois à fréquence horaire/15min. C'est cette
  échelle qui force à traiter les vrais sujets clients : partitionnement, skew,
  Z-Ordering, dimensionnement de clusters, coûts (FinOps), et Auto Loader avec
  schema evolution sur des milliers de fichiers.

Le générateur (`data_generator/generate_readings.py`) est paramétrable en nombre
de compteurs × nombre de jours × granularité, et écrit du Parquet partitionné
directement dans la structure attendue par S3 (`year=/month=/day=`), pour pouvoir
être uploadé tel quel.

## Architecture

```
Générateur (Python/PySpark)
        │
        ▼
   S3 (raw zone)  ──Glue Crawler──▶ Glue Data Catalog
        │
        ▼  (Databricks Auto Loader)
   Bronze (Delta) ─▶ Silver (Delta, dédup/merge) ─▶ Gold (Delta, agrégats + features)
        │                                                 │
        ▼                                                 ▼
  Structured Streaming                              MLflow (training,
  (flux continu compteurs)                          registry, prod)
        │
        ▼
  Databricks Workflows (orchestration Bronze→Silver→Gold→Training)
        │
        ▼
  Power BI / Databricks SQL (reporting quasi temps réel)
```

CI/CD : Databricks Asset Bundles + GitHub Actions (lint, tests, déploiement).
Qualité : Great Expectations sur Silver/Gold. Gouvernance : Unity Catalog (catalog/schema/grants).

## Structure du repo

```
data_generator/    → génération de données synthétiques à volume paramétrable
infra/              → squelette Terraform (S3, Glue Catalog, IAM)
src/bronze/         → ingestion Auto Loader (batch)
src/streaming/       → ingestion Structured Streaming (flux continu)
src/silver/          → nettoyage, dédup, merge/upsert Delta
src/gold/            → agrégats métier + feature engineering
src/mlops/           → training + MLflow tracking/registry
workflows/           → définition de job Databricks Workflows (JSON)
tests/               → pytest + Great Expectations
.github/workflows/   → pipeline CI/CD
docs/                → notes d'architecture et de dimensionnement
```

## Ordre de mise en œuvre

1. `data_generator` → générer le jeu de données à l'échelle voulue, uploader sur S3
2. `infra` → provisionner S3 + Glue Catalog (Terraform ou console)
3. `src/bronze` → Auto Loader S3 → Delta Bronze (Unity Catalog)
4. `src/silver` → nettoyage + merge
5. `src/gold` → agrégats + features
6. `src/streaming` → variante temps réel du flux compteurs
7. `src/mlops` → training + registry MLflow
8. `workflows/job.json` → orchestrer le tout
9. `.github/workflows/ci.yml` → déploiement automatisé via Databricks Asset Bundles
10. `tests/` → qualité de données et non-régression

Voir `docs/ARCHITECTURE.md` pour le détail des choix de dimensionnement (partitionnement,
Z-Ordering, gestion du skew, sizing de clusters).
