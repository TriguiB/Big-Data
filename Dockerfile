# Utilise l'image Bitnami Spark
FROM bitnami/spark:latest
# Download MongoDB Spark Connector JAR
RUN curl -o /opt/bitnami/spark/jars/mongo-spark-connector_2.12-10.4.1.jar \
    https://repo1.maven.org/maven2/org/mongodb/spark/mongo-spark-connector_2.12/10.4.1/mongo-spark-connector_2.12-10.4.1.jar
# Définir le répertoire de travail
WORKDIR /app

# Copier les dépendances et les installer avec pip
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

# Copier le code source dans le conteneur
COPY . /app/

# Lancer un terminal pour coder dans le conteneur
CMD ["bash"]
