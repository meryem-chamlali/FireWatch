
# 🔥 FireWatch – Plateforme Big Data de surveillance de l’activité thermique

##  Présentation du projet

**FireWatch** est un projet Big Data de bout en bout consacré à la surveillance de l’activité thermique liée aux incendies et à l’estimation des risques, à partir des données satellitaires fournies par **NASA FIRMS (Fire Information for Resource Management System)**.

Le projet vise à concevoir et développer un pipeline Big Data distribué capable d'ingérer, de stocker et de traiter des volumes importants de données satellitaires historiques, avec une extension vers le traitement en continu et la prédiction.

En combinant Apache Kafka, Apache Spark, HDFS et le Machine Learning, FireWatch permettra de réaliser des analyses spatio-temporelles, d'étudier l'évolution de l'activité thermique dans différentes zones géographiques et d'estimer la probabilité de futures détections de points chauds.

> **Important :** une détection satellitaire de point chaud ne signifie pas nécessairement qu'un incendie de forêt est présent. Elle peut également correspondre à des brûlages agricoles ou à d'autres sources de chaleur. FireWatch porte donc sur l'analyse de l'activité thermique détectée par satellite.

## 🎯 Objectifs du projet

Les principaux objectifs de FireWatch sont :

- Exploiter les données satellitaires provenant de NASA FIRMS.
- Mettre en place une infrastructure Big Data distribuée.
- Développer une couche d'ingestion de données avec Apache Kafka.
- Assurer le stockage distribué des données avec HDFS.
- Traiter de grands volumes de données de manière distribuée avec Apache Spark.
- Réaliser des analyses historiques et spatio-temporelles de l'activité thermique.
- Développer et évaluer des modèles de Machine Learning.
- Estimer la probabilité de futures détections de points chauds dans différentes zones géographiques.
- Exploiter les résultats à travers des tableaux de bord interactifs.

## 🏗️ Architecture globale

FireWatch repose sur une architecture Big Data distribuée organisée en plusieurs couches complémentaires.
![Architecture globale](docs/architecture%20fonctionnelle%20globale%20de%20FireWatch.png)

### 1. Couche d'ingestion – Apache Kafka

Un producteur développé en Python (`producer.py`) lit les fichiers CSV issus de NASA FIRMS, vérifie les données, construit des événements au format JSON et les publie dans un topic Kafka nommé :

`firms.detections`

Chaque événement est associé à un identifiant unique permettant de faciliter son identification et son suivi.

La couche d'ingestion assure :
- La lecture des fichiers CSV.
- La vérification des champs obligatoires.
- La validation des coordonnées géographiques.
- La conversion des valeurs numériques.
- La construction des événements JSON.
- L'envoi des messages vers Kafka.
- Le suivi des livraisons et des erreurs.

### 2. Couche de stockage – HDFS

HDFS (Hadoop Distributed File System) constitue la couche de stockage distribué du projet.

L'infrastructure comprend :
- Un NameNode pour gérer les métadonnées du système de fichiers.
- Deux DataNodes pour assurer le stockage distribué des blocs de données.

Cette couche sera utilisée pour conserver les données historiques et les données issues de l'ingestion Kafka.

### 3. Couche de traitement Batch – Apache Spark

Apache Spark sera utilisé pour traiter les données historiques à grande échelle.

Les traitements prévus comprennent :
- Le nettoyage et la préparation des données.
- La transformation et l'agrégation des détections.
- L'analyse temporelle de l'activité thermique.
- L'analyse spatiale par zone géographique.
- La préparation des variables destinées au Machine Learning.

### 4. Couche de traitement Streaming – Spark Structured Streaming

Spark Structured Streaming permettra de consommer les événements provenant du topic Kafka `firms.detections`.

Cette couche aura pour rôle de :
- Lire les nouveaux événements.
- Effectuer les transformations nécessaires.
- Exploiter les modèles de Machine Learning entraînés.
- Produire des résultats et des prédictions à partir des données entrantes.

### 5. Couche Machine Learning

La partie Machine Learning aura pour objectif d'estimer la probabilité de futures détections satellitaires de points chauds.

La tâche principale envisagée est un problème de **classification binaire**.

L'objectif est de déterminer si une cellule géographique d'environ 25 × 25 km connaîtra au moins une détection satellitaire de point chaud à haute confiance au cours des sept jours suivants.

Les classes sont définies comme suit :

- **Classe 0 :** aucune détection de point chaud à haute confiance.
- **Classe 1 :** au moins une détection de point chaud à haute confiance.

Les algorithmes envisagés sont :
- Régression logistique (Logistic Regression).
- Forêt aléatoire (Random Forest).
- Arbres de gradient boostés (Gradient-Boosted Trees – GBT).

Les modèles seront évalués à l'aide de plusieurs métriques : précision, rappel, F1-score et matrice de confusion.

Une analyse complémentaire pourra utiliser K-Means pour regrouper les zones géographiques selon leurs caractéristiques historiques d'activité thermique.

### 6. Couche de persistance – PostgreSQL

PostgreSQL sera utilisé pour stocker les résultats des traitements, les prédictions et les indicateurs analytiques afin de faciliter leur exploitation par les outils de visualisation.

### 7. Couche de visualisation – Grafana

Grafana permettra de concevoir des tableaux de bord interactifs pour suivre l'évolution de l'activité thermique et visualiser les résultats des analyses et des prédictions.

## 🛠️ Technologies utilisées

| Domaine | Technologies |
|---|---|
| Langage de programmation | Python |
| Source de données | NASA FIRMS – VIIRS |
| Ingestion des données | Apache Kafka |
| Stockage distribué | HDFS |
| Traitement Batch | Apache Spark |
| Traitement Streaming | Spark Structured Streaming |
| Machine Learning | Spark MLlib |
| Base de données | PostgreSQL |
| Visualisation | Grafana |
| Conteneurisation | Docker, Docker Compose |
| Gestion de versions | Git, GitHub |

## 🗂️ Données utilisées

Le projet exploite des données satellitaires provenant de **NASA FIRMS**, issues du capteur VIIRS (Visible Infrared Imaging Radiometer Suite).

Les données étudiées couvrent la période **2019–2025** et concernent deux pays :

- Espagne.
- Portugal.

Les fichiers CSV contiennent notamment les informations suivantes :

| Champ | Description |
|---|---|
| latitude | Latitude de la détection |
| longitude | Longitude de la détection |
| bright_ti4 | Température de brillance du canal thermique I4 |
| scan | Dimension du pixel selon l'axe de balayage |
| track | Dimension du pixel selon l'axe de déplacement |
| acq_date | Date d'acquisition |
| acq_time | Heure d'acquisition |
| satellite | Satellite ayant effectué la détection |
| instrument | Instrument de mesure |
| confidence | Niveau de confiance de la détection |
| bright_ti5 | Température de brillance du canal thermique I5 |
| frp | Puissance radiative du feu |
| daynight | Indication jour/nuit |
| type | Type de détection |

Les fichiers de données sont organisés localement par pays et par année. Ils ne sont pas versionnés sur GitHub afin de conserver un dépôt léger.

## 🐳 Infrastructure distribuée

L'environnement d'exécution est déployé à l'aide de Docker et Docker Compose.

Le cluster comprend les composants suivants :

| Composant | Nombre | Rôle |
|---|---:|---|
| Apache Kafka | 1 | Ingestion et gestion des messages |
| HDFS NameNode | 1 | Gestion du système de fichiers distribué |
| HDFS DataNodes | 2 | Stockage distribué des blocs de données |
| Spark Master | 1 | Coordination des traitements Spark |
| Spark Workers | 2 | Exécution des tâches distribuées |

Les configurations de l'infrastructure sont regroupées dans le dossier `infra/`.

## 🚀 Installation et exécution

### Prérequis

- Python 3.
- Docker Desktop.
- Docker Compose.
- Git.

### 1. Cloner le dépôt

```bash
git clone https://github.com/meryem-chamlali/FireWatch.git
cd FireWatch
```

### 2. Démarrer l'infrastructure

Depuis la racine du projet :

```bash
docker compose -f infra/docker-compose.yml up -d
```

Vérifier les conteneurs :

```bash
docker ps
```

### 3. Installer les dépendances Python

```bash
pip install -r ingestion/requirements.txt
```

### 4. Lancer le producteur Kafka

Exemple avec le fichier Espagne 2025 :

```bash
python ingestion/producer.py --csv data/Spain/viirs-jpss1_2025_Spain.csv --country spain --interval 0
```

Exemple avec le fichier Portugal 2025 :

```bash
python ingestion/producer.py --csv data/Portugal/viirs-jpss1_2025_Portugal.csv --country portugal --interval 0
```

Paramètres disponibles :

| Paramètre | Description |
|---|---|
| `--csv` | Chemin du fichier CSV à lire |
| `--country` | Pays concerné : `spain` ou `portugal` |
| `--bootstrap` | Adresse du serveur Kafka (par défaut `localhost:9092`) |
| `--interval` | Délai entre deux envois (par défaut 1 seconde) |
| `--max-rows` | Nombre maximal de lignes à traiter (0 pour toutes) |

### 5. Vérifier la réception des messages

Pour consulter les messages du topic Kafka :

```bash
docker exec firewatch-kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic firms.detections --from-beginning --max-messages 3
```

### 6. Arrêter l'infrastructure

```bash
docker compose -f infra/docker-compose.yml down
```

Cette commande arrête les conteneurs sans supprimer les volumes persistants.

## 📂 Structure du projet

```text
FireWatch/
│
├── ingestion/
│   ├── producer.py
│   └── requirements.txt
│
├── infra/
│   ├── docker-compose.yml
│   └── configs/
│
├── docs/
│   └── architecture fonctionnelle globale de FireWatch.png
│
├── data/
│   ├── Spain/
│   └── Portugal/
│
├── .gitignore
└── README.md
```

Les dossiers `batch/`, `streaming/`, `database/`, `models/` et `dashboard/` pourront être ajoutés au fur et à mesure de l'avancement du projet.

Le dossier `data/` est ignoré par Git : les fichiers CSV NASA FIRMS restent locaux et ne sont pas envoyés sur GitHub.

## 🚀 Avancement du projet

Le premier socle de l'infrastructure Big Data est mis en place : cluster Docker, configuration de Kafka, HDFS et Spark, ainsi que développement et validation du producteur Python pour l'ingestion des données NASA FIRMS.

Le projet se poursuit avec le stockage distribué, les traitements Batch et Streaming, l'analyse des données et le développement des modèles de Machine Learning.

## 👥 Équipe

Projet académique réalisé par une équipe de **3 étudiants** : Chaimae Blouaid/ Hassna ELbahmani/ Meryem Chamali .

## 📚 Source des données

**NASA FIRMS – Fire Information for Resource Management System**

https://firms.modaps.eosdis.nasa.gov/

Données satellitaires utilisées : VIIRS (Visible Infrared Imaging Radiometer Suite).

---

**FireWatch – Pipeline Big Data de bout en bout pour la surveillance de l’activité thermique liée aux incendies et l’estimation des risques, à partir des données satellitaires de la NASA FIRMS, avec Kafka, Spark, HDFS et Machine Learning.**
