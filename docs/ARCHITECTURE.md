# Notes d'architecture et de dimensionnement

Ce document sert de support pour argumenter les choix en entretien — chaque
décision ci-dessous correspond à un sujet que la fiche de poste évoque
explicitement (FinOps, CI/CD, observabilité, gouvernance).

## Partitionnement

- Bronze/Silver/Gold partitionnées par `year/month/day` (voire `region` en Gold) :
  aligné sur le pattern d'accès dominant (requêtes sur une plage de dates récente).
- Éviter le sur-partitionnement (`site_id` en clé de partition serait une erreur —
  des millions de petites partitions = "small file problem"). `site_id` est géré
  via **Z-Ordering**, pas via le partitioning physique.

## Gestion du volume à l'ingestion

- Auto Loader (`cloudFiles`) plutôt que `spark.read` : listing incrémental via
  notifications S3, essentiel dès qu'on dépasse quelques dizaines de milliers de
  fichiers dans le raw.
- `maxFilesPerTrigger` pour contrôler la taille des micro-batchs et éviter un
  cluster sous-dimensionné qui se fait déborder par un backlog.

## Gestion du skew

- Le référentiel sites (petite dimension) est joint en `broadcast` explicite pour
  éviter un shuffle inutile sur la table de faits.
- Si un jour de test révèle un skew réel (ex: quelques sites industriels avec un
  volume disproportionné), la parade est le **salting** : ajouter une clé
  aléatoire artificielle sur les clés très fréquentes avant le join, à documenter
  même si pas implémenté ici — c'est une question fréquente en entretien.

## Idempotence et fiabilité

- `MERGE INTO` (upsert) en Silver plutôt qu'un overwrite complet : permet de
  relancer le job sans dupliquer ni tout recalculer.
- `trigger(availableNow=True)` pour l'ingestion batch incrémentale (traite tout le
  backlog disponible puis s'arrête) — pattern recommandé par Databricks au lieu
  d'un `trigger(once=True)` déprécié.

## FinOps

- Autoscaling des clusters de job (min/max workers) plutôt qu'un cluster fixe.
- `optimizeWrite` et `autoCompact` activés pour limiter le small-file problem sans
  job `OPTIMIZE` manuel à chaque run.
- `OPTIMIZE ... ZORDER BY` planifié séparément (pas à chaque exécution) pour ne
  pas payer le coût de compaction à chaque micro-batch.

## Gouvernance (Unity Catalog)

- Structure `catalog.schema.table` (`energy.bronze/silver/gold.*`) — à documenter
  avec les grants prévus (lecture Gold pour les analystes, écriture Bronze/Silver
  réservée au service principal du pipeline).
- Table des lignes rejetées (`silver.rejected_readings`) : traçabilité qualité,
  souvent oubliée mais très valorisée en entretien ("où vont les données qui ne
  passent pas vos contrôles ?").

## Ce qui reste à approfondir si le temps le permet

- Rejouer une partie du pipeline côté Azure (ADF + ADLS Gen2 + Key Vault) pour
  pouvoir parler des deux clouds, comme demandé dans la fiche de poste.
- Ajouter un dashboard de coût (DBU consommés par job/jour) pour illustrer le
  volet FinOps au-delà de la configuration de cluster.
