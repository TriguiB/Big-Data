# Utilise l'image Bitnami Spark
FROM bitnami/spark:latest
# Définir le répertoire de travail
WORKDIR /app

# Copier les dépendances et les installer avec pip
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install mlflow


# Copier le code source dans le conteneur
COPY . /app/

# Lancer un terminal pour coder dans le conteneur
CMD ["bash"]