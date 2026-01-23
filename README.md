# NYC_Transportation_Data_Pipeline
Pipeline ELT moderne analysant les données de transport de New York City (2024). Extrait quotidiennement 8 sources (APIs NYC 311, Traffic, Weather + fichiers Taxi Parquet), stocke dans GCS, transforme avec dbt Core dans BigQuery, et visualise via Power BI.
