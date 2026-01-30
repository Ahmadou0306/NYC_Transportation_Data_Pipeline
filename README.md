# NYC Transportation Data Pipeline

## Vue d'ensemble

Pipeline ELT end-to-end pour l'analyse des données de transport de New York City utilisant une stack moderne de Data Engineering. Ce projet extrait, transforme et visualise plus de 50 millions d'enregistrements mensuels provenant de multiples sources (APIs, fichiers Parquet) pour fournir des insights sur les patterns de transport urbain, l'impact météorologique et les plaintes citoyennes.

## Objectifs du projet

- **Centraliser** les données de transport NYC provenant de 8 sources différentes
- **Analyser** les corrélations entre météo, trafic, courses de taxi et plaintes 311
- **Visualiser** les patterns de mobilité urbaine via des dashboards interactifs
- **Démontrer** les compétences en Data Engineering moderne (ELT, dbt, BigQuery, Airflow)

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                       DATA SOURCES                               │
│  • NYC 311 API (JSON) - Quotidien                               │
│  • NYC Traffic Volume API (CSV) - Quotidien                     │
│  • NYC Traffic Speed API (JSON) - Horaire                       │
│  • NOAA Weather API (JSON) - Quotidien                          │
│  • TLC Taxi Files (Parquet) - Mensuel (4 types)                │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                   AIRFLOW (Orchestration)                        │
│  • hourly_pipeline   : Traffic speed data                       │
│  • daily_pipeline    : 311, Traffic volume, Weather             │
│  • monthly_pipeline  : Taxi trips (Yellow, Green, FHV, HVFHV)   │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│              GOOGLE CLOUD STORAGE (Data Lake)                    │
│  gs://nyc-data-lake/                                            │
│  └── raw/                    (Bronze Layer)                     │
│      ├── traffic_volume/     Partitionné par date              │
│      ├── 311_requests/                                          │
│      ├── traffic_speed/                                         │
│      ├── weather/                                               │
│      └── taxi/                                                  │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                    BIGQUERY (Data Warehouse)                     │
│                                                                   │
│  staging/           (Silver Layer - Données nettoyées)          │
│  ├── stg_311_requests                                           │
│  ├── stg_traffic_volume                                         │
│  ├── stg_traffic_speed                                          │
│  ├── stg_weather                                                │
│  └── stg_taxi_trips                                             │
│                                                                   │
│  intermediate/      (Gold Layer - Logique business)             │
│  ├── int_trips_enriched        (Trips + Weather)               │
│  ├── int_311_by_zone                                            │
│  └── int_traffic_metrics                                        │
│                                                                   │
│  marts/             (Gold Layer - Tables analytiques)           │
│  ├── dim_zones              (Zones géographiques NYC)           │
│  ├── dim_dates              (Calendrier)                        │
│  ├── fact_taxi_trips        (Courses taxi enrichies)            │
│  └── fact_311_complaints    (Plaintes citoyennes)               │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                  DBT CORE (Transformation SQL)                   │
│  • 15+ modèles SQL versionnés                                   │
│  • 30+ tests de qualité de données                              │
│  • Documentation auto-générée                                   │
│  • Incremental loading pour optimisation coûts                  │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                   POWER BI (Visualisation)                       │
│  • Dashboard Vue d'ensemble (KPIs transport)                    │
│  • Dashboard Météo & Mobilité (Corrélations)                   │
│  • Dashboard 311 (Heatmap plaintes par zone)                   │
│  • Dashboard Taxi Analytics (Patterns temporels)                │
└─────────────────────────────────────────────────────────────────┘
```

## Stack technique

| Catégorie | Technologie | Usage |
|-----------|-------------|-------|
| **Orchestration** | Apache Airflow | Scheduling et monitoring des pipelines |
| **Data Lake** | Google Cloud Storage | Stockage brut des données (Bronze) |
| **Data Warehouse** | BigQuery | Stockage et requêtes SQL (Silver/Gold) |
| **Transformation** | dbt Core | Transformations SQL, tests, documentation |
| **Visualisation** | Power BI | Dashboards interactifs |
| **Langages** | Python 3.9+, SQL | Scripts extraction, transformations |
| **CI/CD** | GitHub Actions | Tests automatisés (optionnel) |

## Sources de données

### Données horaires (24x/jour)
- **NYC Real-Time Traffic Speed** : Vitesse du trafic en temps réel par segment de rue -> https://data.cityofnewyork.us/resource/i4gi-tjb9.json?$limit=10000&$where=data_as_of between '2024-01-15T00:00:00' and '2024-01-15T23:59:59'

### Données quotidiennes (1x/jour)
- **NYC Traffic Volume Counts** : Comptages de véhicules par intersection -> https://data.cityofnewyork.us/resource/btm5-ppia.csv?$limit=50000&$where=yr=2024 AND m=1 AND d=15
 
- **NYC 311 Service Requests** : Plaintes et demandes de service non-urgentes -> https://data.cityofnewyork.us/resource/erm2-nwe9.json?$limit=50000&$where=created_date between '2024-01-15T00:00:00' and '2024-01-15T23:59:59'

- **NOAA Weather Data** : Données météo (précipitations, température, neige) -> https://www.ncei.noaa.gov/access/services/data/v1?dataset=daily-summaries&dataTypes=PRCP,TMAX,TMIN,SNOW&stations=USW00094728&startDate=2024-01-15&endDate=2024-01-15&format=json


### Données mensuelles (1x/mois)
- **Yellow Taxi Trips** : Courses de taxis jaunes -> https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet
- **Green Taxi Trips** : Courses de taxis verts (banlieue) -> https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2024-01.parquet
- **For-Hire Vehicle Trips** : Courses de véhicules de location -> https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_2024-01.parquet
- **High Volume FHV Trips** : Courses haute fréquence (Uber, Lyft) -> https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2024-01.parquet

## Métriques du projet

- **Volume de données** : ~50M+ lignes/mois (taxi seul)
- **Fréquence de rafraîchissement** : Horaire, quotidien, mensuel
- **Latence** : < 2 heures pour données du jour
- **Coût estimé GCP** : ~$10-15/mois (avec optimisations)
- **Période couverte** : 2024 (MVP) → 2025-2026 (scale)

## Installation & Setup

### Prérequis

```bash
# Logiciels requis
- Python 3.9+
- Docker & Docker Compose (pour Airflow)
- gcloud CLI
- git

# Comptes nécessaires
- Compte GCP 
- Compte Power BI
```


## Structure du projet

```
nyc-transport-pipeline/
│
├── airflow/                        # Orchestration Airflow
│   ├── dags/
│   │   ├── hourly_traffic_speed.py
│   │   ├── daily_pipeline.py
│   │   └── monthly_taxi_data.py
│   ├── plugins/
│   ├── scripts/
│   │   ├── extract_311.py
│   │   ├── extract_traffic.py
│   │   ├── extract_weather.py
│   │   └── extract_taxi.py
│   └── docker-compose.yml
│
├── dbt/                            # Transformations dbt
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_311_requests.sql
│   │   │   ├── stg_traffic_volume.sql
│   │   │   ├── stg_traffic_speed.sql
│   │   │   ├── stg_weather.sql
│   │   │   └── stg_taxi_trips.sql
│   │   ├── intermediate/
│   │   │   ├── int_trips_enriched.sql
│   │   │   ├── int_311_by_zone.sql
│   │   │   └── int_traffic_metrics.sql
│   │   └── marts/
│   │       ├── dim_zones.sql
│   │       ├── dim_dates.sql
│   │       ├── fact_taxi_trips.sql
│   │       └── fact_311_complaints.sql
│   ├── tests/
│   ├── macros/
│   ├── dbt_project.yml
│   └── packages.yml
│
├── powerbi/                        # Dashboards Power BI
│   ├── nyc_transport_dashboard.pbix
│   └── screenshots/
│
├── docs/                           # Documentation
│   ├── architecture_diagram.png
│   ├── data_dictionary.md
│   └── setup_guide.md
│
├── tests/                          # Tests unitaires
│   └── test_extraction.py
│
├── .github/
│   └── workflows/
│       └── dbt_ci.yml             # CI/CD (optionnel)
│
├── .gitignore
├── requirements.txt
├── README.md
└── credentials.json.example       # Template credentials
```

### Exécuter le pipeline complet

```bash
# 1. Démarrer Airflow
cd airflow
docker-compose up -d

# 2. Activer les DAGs depuis l'UI Airflow
# http://localhost:8080

# 3. Monitorer l'exécution
# Les logs sont disponibles dans l'UI Airflow

# 4. Vérifier les données dans BigQuery
bq query --nouse_legacy_sql \
  'SELECT COUNT(*) FROM `nyc_project.staging.stg_taxi_trips`'

# 5. Exécuter les transformations dbt
cd ../dbt
dbt run
dbt test

# 6. Générer la documentation dbt
cd dbt/
dbt docs generate
dbt docs serve  # http://localhost:8080
```



## Dashboards Power BI

### KPIs principaux
- **Volume de courses** : Évolution quotidienne/mensuelle
- **Impact météo** : Corrélation pluie/neige → courses taxi
- **Zones chaudes** : Heatmap des pickups par zone
- **Plaintes 311** : Distribution par type et borough
- **Vitesse trafic** : Moyennes par heure de la journée
### Captures d'écran
![Dashboard Overview](powerbi/screenshots/dashboard_overview.png)
![Météo & Mobilité](powerbi/screenshots/weather_impact.png)

## Tests & Qualité des données

### Tests dbt implémentés

```sql
-- Unicité des clés primaires
-- Non-nullité des colonnes critiques
-- Valeurs acceptées (ex: boroughs NYC)
-- Relations entre tables (foreign keys)
-- Fraîcheur des données (< 24h)
```



## Évolutions futures
- [ ] Ajout de prédictions ML (demande taxi par zone/heure)
- [ ] Alerting automatique sur anomalies (Great Expectations)
- [ ] Dashboard temps réel (Streamlit ou Dash)
- [ ] Expansion à d'autres villes (Chicago, LA)
- [ ] API publique pour exposer les données agrégées

### Documentation officielle
- [Airflow Documentation](https://airflow.apache.org/docs/)
- [dbt Documentation](https://docs.getdbt.com/)
- [BigQuery Documentation](https://cloud.google.com/bigquery/docs)
- [NYC Open Data](https://opendata.cityofnewyork.us/)

## Auteur
**Ahmadou NDIAYE**
- GitHub: [@Ahmadou0306](https://github.com/Ahmadou0306)
- LinkedIn: [Ahmadou Ndiaye](https://www.linkedin.com/in/ahmadou-ndiaye-792a09205/)
- Email: ahmadou.ndiaye.pro@gmail.com