## 🎯 Couche INTERMEDIATE : Modèles à créer

L'objectif de la couche **intermediate** est d'**enrichir, joindre et agréger** les données nettoyées du staging pour créer des tables avec de la **logique métier**.

---

## 📊 Modèles INTERMEDIATE pertinents et variés

### **1. int_trips_enriched** (Courses enrichies avec météo)

**Objectif :** Joindre toutes les courses (Yellow, FHV, HVFHV) avec les données météo du jour

**Cas d'usage :**
- Analyser l'impact de la pluie/neige sur le nombre de courses
- Comparer les tarifs moyens par conditions météo
- Identifier les heures de pointe par type de temps

**Tables sources :**
- `stg_yellow_taxi_trips`
- `stg_for_hire_vehicule_trips`
- `stg_hvfhv_trips`
- `stg_weather`

**Logique métier :**
- UNION des 3 sources taxi
- JOIN avec météo sur la date de pickup
- Ajout de flags : `is_rainy`, `is_snowy`, `is_rush_hour`
- Calcul de durée de trajet réelle
- Catégorisation de distance (courte/moyenne/longue)

**Volumétrie :** ~38M lignes/mois (3M + 15M + 20M)

---

### **2. int_311_by_zone_daily** (Plaintes agrégées par zone et jour)

**Objectif :** Agréger les plaintes 311 par zone géographique et date pour identifier les zones problématiques

**Cas d'usage :**
- Heatmap des zones avec le plus de plaintes parking
- Évolution temporelle des plaintes par borough
- Comparer efficacité des agences (temps de résolution moyen)

**Tables sources :**
- `stg_311_requests`

**Logique métier :**
- Agrégation par `borough`, `DATE(request_created_at)`, `complaint_type`
- Calculs : nombre de plaintes, temps de résolution moyen, % fermées
- Classement des zones par volume de plaintes

**Volumétrie :** ~500 lignes/jour (5 boroughs × 7 types × ~15 jours)

---

### **3. int_traffic_hourly_avg** (Vitesse moyenne du trafic par heure)

**Objectif :** Moyenner les mesures de vitesse (toutes les 5 min) par heure et segment pour analyser les patterns quotidiens

**Cas d'usage :**
- Identifier les heures de pointe par zone
- Détecter les embouteillages récurrents
- Comparer vitesse moyenne semaine vs weekend

**Tables sources :**
- `stg_traffic_speed`

**Logique métier :**
- Agrégation par `link_id`, `borough`, `DATE_TRUNC(measurement_timestamp, HOUR)`
- Calculs : vitesse moyenne, vitesse min/max, écart-type
- Flag : `is_congested` (speed < 15 mph)

**Volumétrie :** ~2k lignes/heure (segments × boroughs)

---

### **4. int_trips_with_traffic** (Courses avec conditions de trafic)

**Objectif :** Enrichir les courses avec l'état du trafic au moment du pickup pour analyser l'impact

**Cas d'usage :**
- Corrélation trafic dense → prix Uber plus élevés ?
- Impact du trafic sur la durée réelle des courses
- Zones où le trafic allonge le plus les courses

**Tables sources :**
- `int_trips_enriched`
- `int_traffic_hourly_avg`

**Logique métier :**
- JOIN entre courses et trafic sur zone + heure de pickup
- Calcul : écart entre durée estimée (distance / vitesse) et durée réelle
- Flag : `stuck_in_traffic` (durée réelle > 150% estimée)

**Volumétrie :** ~38M lignes/mois

---

### **5. int_weather_impact_daily** (Impact météo quotidien sur la mobilité)

**Objectif :** Agréger les métriques de mobilité par jour et conditions météo

**Cas d'usage :**
- Comparer nombre de courses jours de pluie vs jours secs
- Impact neige sur vitesse moyenne du trafic
- Corrélation température → plaintes (ex: chaleur → AC cassée)

**Tables sources :**
- `stg_weather`
- `int_trips_enriched` (agrégé)
- `int_traffic_hourly_avg` (agrégé)
- `int_311_by_zone_daily` (agrégé)

**Logique métier :**
- Agrégation par `weather_date`, `weather_condition`
- Calculs : total courses, vitesse trafic moyenne, total plaintes
- Comparaison : jours normaux vs jours extrêmes (pluie >50mm, neige >10mm)

**Volumétrie :** ~365 lignes/an (1 ligne/jour)

---

### **6. int_taxi_demand_hourly** (Demande de taxi par heure et zone)

**Objectif :** Agréger les pickups par zone et heure pour modéliser la demande

**Cas d'usage :**
- Prédire la demande future (ML)
- Optimiser la distribution des taxis
- Identifier zones sous-desservies

**Tables sources :**
- `int_trips_enriched`

**Logique métier :**
- Agrégation par `pickup_location_id`, `EXTRACT(HOUR FROM pickup_datetime)`, `EXTRACT(DAYOFWEEK FROM pickup_datetime)`
- Calculs : nombre de pickups, distance moyenne, tarif moyen
- Ajout : `is_weekend`, `is_holiday`, `time_slot` (morning/afternoon/evening/night)

**Volumétrie :** ~265 zones × 24 heures × 7 jours = ~45k lignes (réutilisable chaque semaine)

---

### **7. int_311_resolution_performance** (Performance de résolution par agence)

**Objectif :** Analyser l'efficacité des agences à traiter les plaintes

**Cas d'usage :**
- Benchmarking inter-agences
- Identifier les types de plaintes les plus lents à résoudre
- Alerter sur plaintes ouvertes depuis trop longtemps

**Tables sources :**
- `stg_311_requests`

**Logique métier :**
- Agrégation par `agency`, `complaint_type`, `DATE_TRUNC(request_created_at, MONTH)`
- Calculs : temps résolution médian/moyen, % fermées dans 24h/7j/30j
- Classement : agences les plus rapides/lentes

**Volumétrie :** ~50 lignes/mois (agences × types de plaintes)

---

## 📋 Synthèse des modèles INTERMEDIATE recommandés

| Modèle | Type | Volumétrie | Complexité | Priorité |
|--------|------|-----------|-----------|----------|
| **int_trips_enriched** | Jointure + enrichissement | ~38M/mois | ⭐⭐⭐ | 🔥 **Critique** |
| **int_311_by_zone_daily** | Agrégation | ~500/jour | ⭐⭐ | ✅ Haute |
| **int_traffic_hourly_avg** | Agrégation | ~2k/heure | ⭐⭐ | ✅ Haute |
| **int_trips_with_traffic** | Jointure complexe | ~38M/mois | ⭐⭐⭐⭐ | ⚠️ Moyenne |
| **int_weather_impact_daily** | Agrégation multi-sources | ~365/an | ⭐⭐⭐ | ✅ Haute |
| **int_taxi_demand_hourly** | Agrégation spatiotemporelle | ~45k | ⭐⭐ | ⚠️ Moyenne |
| **int_311_resolution_performance** | Agrégation KPI | ~50/mois | ⭐ | ⚠️ Basse |

---

## 🎯 Ordre de développement recommandé

### **Phase 1 : Fondations (Semaine 1)**
1. ✅ **int_trips_enriched** → Base pour tout le reste
2. ✅ **int_traffic_hourly_avg** → Simplifie les données horaires
3. ✅ **int_311_by_zone_daily** → Agrégation simple et utile

### **Phase 2 : Analyses avancées (Semaine 2)**
4. **int_weather_impact_daily** → Synthèse quotidienne
5. **int_taxi_demand_hourly** → Patterns de demande

### **Phase 3 : Optionnel (si temps)**
6. **int_trips_with_traffic** → Analyse croisée complexe
7. **int_311_resolution_performance** → KPIs agences

---

## 💡 Autres idées de modèles INTERMEDIATE (brainstorming)

| Idée | Description | Utilité |
|------|-------------|---------|
| **int_trips_profitability** | Calcul marge Uber/Lyft (fare - driver_pay) | Analyse business |
| **int_peak_hours_detection** | Détection automatique heures de pointe par zone | ML / Prédiction |
| **int_weather_anomalies** | Jours avec météo extrême vs normale | Alerting |
| **int_311_repeat_offenders** | Zones/adresses avec plaintes récurrentes | Priorisation travaux |
| **int_taxi_zone_popularity** | Ranking des zones par pickups/dropoffs | Dashboards |

---

## 🚀 Next Step

**Voulez-vous qu'on commence par créer `int_trips_enriched` ?**

C'est le modèle le plus critique car il :
1. ✅ Unifie les 3 sources taxi (UNION)
2. ✅ Ajoute la météo (JOIN)
3. ✅ Calcule des métriques utiles (durée, catégories)
4. ✅ Servira de base pour les marts finaux

On peut le faire ensemble étape par étape ! 🎯