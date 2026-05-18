# Azure-ETL-Factory - Notes pour Entretien

## Pitch (30 secondes)
"J'ai construit un pipeline ETL batch pour des donnees de vente retail,
orchestre par Azure Data Factory. ADF copie les donnees depuis Azure SQL
vers le Data Lake en Parquet, puis declenche des notebooks Databricks
pour les transformations Silver/Gold. Les resultats alimentent des
dashboards Power BI via Synapse Serverless SQL."

## Questions frequentes

### "Pourquoi ADF plutot qu'Airflow pour l'orchestration ?"
ADF est natif Azure avec des connecteurs integres (SQL, Blob, Databricks).
Son UI de monitoring est excellent pour les equipes ops non-techniques.
Airflow serait preferable en multi-cloud ou pour des DAGs complexes
avec beaucoup de logique Python custom. En full Azure, ADF est le choix naturel.

### "Comment gerez-vous les echecs et les retry ?"
Trois niveaux : (1) retry au niveau activite (2 retries, 5min interval),
(2) alertes email via ADF Monitor sur failure,
(3) idempotence du pipeline grace au MERGE Delta Lake - on peut relancer
sans creer de doublons.

### "Comment le pipeline est-il declenche ?"
Trigger schedule quotidien a 6h UTC. Peut aussi etre declenche manuellement
ou par un event trigger (quand un fichier arrive dans le Data Lake).
Le parametre run_date permet de reprocesser des dates historiques (backfill).

### "Comment securisez-vous les credentials ?"
Azure Key Vault pour tous les secrets (connection strings, tokens).
Les linked services ADF utilisent Managed Identity quand possible.
Jamais de credentials en dur dans le code ou les ARM templates.

### "Quelle difference entre Copy Activity et Data Flow ?"
Copy Activity = deplacement simple (SQL -> Parquet, Blob -> Blob).
Data Flow = transformations visuelles (ETL sans code, executes sur Spark).
Pour des transformations complexes, je prefere Databricks notebooks
car plus de controle et meilleure testabilite.

### "Comment monitorer le pipeline ?"
ADF Monitor natif (dashboard des runs, durees, statuts).
Azure Monitor + Log Analytics pour les alertes (latence > seuil, failures).
Metriques custom dans les notebooks (row counts, DQ scores) logguees
dans Delta Lake pour audit trail.
