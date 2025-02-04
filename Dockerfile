# Utilisation de l'image officielle de Python
FROM python:3.9-slim

# Copier le fichier requirements.txt dans le conteneur
COPY requirements.txt /app/requirements.txt

# Installer les dépendances Python à partir de requirements.txt
RUN pip install -r /app/requirements.txt

# Définir le répertoire de travail
WORKDIR /app

# Copier les scripts d'ingestion
COPY ./scripts/Ingestion /app/scripts/Ingestion

# Exposer les ports nécessaires pour MongoDB et Spark (si nécessaire)
EXPOSE 27017 8080

# Commande pour lancer l'ingestion des données
CMD ["python", "scripts/Ingestion/collect_data.py"]
