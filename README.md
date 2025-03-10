# README - Big Data Project

## 🚀 Lancement du Projet

### 1️⃣ Cloner le projet

```sh
git clone https://github.com/TriguiB/Big-Data
cd Big-Data
```

### 2️⃣ Lancer les services Docker

```sh
docker-compose build
docker-compose up -d
```

### 3️⃣ Accéder aux interfaces

Une fois les services lancés, vous pouvez visualiser :

- **📊 MLflow UI** : Disponible sur [http://localhost:5000/](http://localhost:5000/)
- **🎨 Application Streamlit** : Disponible sur [http://localhost:8501/](http://localhost:8501/)

---

## 📄 Description

Notre projet consiste à :

- 📥 **Extraction** : Récupérer automatiquement les issues des repos publics GitHub via l'API GitHub (`collect_data.py`).
- 🔍 **Prétraitement** : Nettoyer et transformer les données selon des critères définis (`traitement_data.py`).
- 🤖 **Modélisation** : Entraîner un modèle de Machine Learning pour prédire le temps estimé de résolution d’une issue.
- 🎧 **Interface** : Fournir une interface utilisateur avec **Streamlit** pour interroger le modèle.

---

## 🏰 Architecture

Le projet repose sur plusieurs technologies :

- **📡 Extraction des données** : API GitHub + Kafka.
- **📄 Stockage** : MongoDB & Azure Blob Storage.
- **⚙️ Traitement** : Nettoyage et transformation avec PySpark.
- **📊 Modélisation** : Prédiction avec un modèle ML supervisé.
- **🎨 Interface utilisateur** : Développement avec Streamlit.
- **🐳 Containerisation** : Docker et `docker-compose.yml`.

---

## 🛠️ Utilisation

### 📥 Extraction des Issues GitHub *(optionnel)*

Lancer le script pour récupérer les issues :

```sh
python collect_data.py <min_date> <max_date>
```

Exemple :

```sh
python collect_data_auto.py 2020-01-01 2022-12-31
```

### 🔍 Traitement et stockage *(optionnel)*

Le pipeline traite les issues et les stocke dans MongoDB et Azure Blob Storage :

```sh
python azure_to_mongo.py
```

---

## 📊 Données

Les fichiers de données sont stockés sous forme de JSON :

- **Stockage cloud** : Azure Blob Storage
- **Base de données** : MongoDB

---

## 👥 Contributeurs

- Brahim TRIGUI
- Karim ABBES
- Raed KOUKI
- Dhia TRIGUI

