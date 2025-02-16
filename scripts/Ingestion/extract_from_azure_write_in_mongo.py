from azure.storage.blob import BlobServiceClient
import json
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col, expr, datediff, to_date, when

# Azure Blob Storage configurations
AZURE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=issuesstorage;AccountKey=Q7It5++J5VE7284S/QP+ZqHE1cT6Mad16bvyC+Eqx1j1xpRh5QlWMFJAzdmUC/DguMF3CmEsK87R+AStyWxtjg==;EndpointSuffix=core.windows.net"
CONTAINER_NAME = "kafka-data"
BLOB_NAME = "messages_test.json"

# Fonction pour extraire les données depuis Azure Blob Storage
def get_blob_data():
    blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
    blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=BLOB_NAME)
    blob_data = blob_client.download_blob().readall()
    return json.loads(blob_data.decode('utf-8'))

# Récupérer le message
messages = get_blob_data()

# Initialiser Spark en mode local et avec MongoDB
spark = SparkSession.builder \
    .appName("MongoDB_Spark") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.4.1") \
    .config("spark.mongodb.read.connection.uri", "mongodb://mongodb:27017/github_issues") \
    .config("spark.mongodb.write.connection.uri", "mongodb://mongodb:27017/github_issues") \
    .config("spark.executor.memory", "1G") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# Vérifier la connexion au cluster Spark
print("🚀 Spark connecté au master:", spark.sparkContext.master)
print("🖥️ Nombre de workers disponibles:", spark.sparkContext.defaultParallelism)

try:
    # Vérification si messages est une liste ou un seul objet
    if not isinstance(messages, list):
        messages = [messages]

    # Transformation des messages en une liste de Row pour créer un DataFrame
    rows = []
    for message in messages:
        if 'closed_issues' in message:  # Vérifier si la clé 'closed_issues' existe
            closed_issues = message['closed_issues']
            for issue in closed_issues:
                # Prétraiter les closed_issues
                rows.append(Row(
                    issue_id=issue.get('id', None),
                    title=issue.get('title', ''),
                    body=issue.get('body', ''),
                    state=issue.get('state', ''),
                    created_at=issue.get('created_at', None),
                    closed_at=issue.get('closed_at', None),
                    language=message.get('language', ''),
                    stars=message.get('stars', 0)
                ))

    # Créer un DataFrame à partir des lignes
    df_issues = spark.createDataFrame(rows)

    # Effectuer des transformations supplémentaires si nécessaire (ex : calcul de durée)
    df_issues = df_issues.withColumn("created_at", to_date(col("created_at"))) \
        .withColumn("closed_at", to_date(col("closed_at"))) \
        .withColumn("duration", datediff(col("closed_at"), col("created_at"))) \
        .fillna("")

    # Afficher les données à insérer
    df_issues.show()

    # Écrire les données dans la collection MongoDB
    df_issues.write.format("mongodb") \
        .option("database", "github_issues") \
        .option("collection", "closed_issues") \
        .mode("append") \
        .save()

    print("✅ Données insérées avec succès dans MongoDB.")

    # Lire les données depuis MongoDB pour vérifier l'insertion
    df_read = spark.read.format("mongodb") \
        .option("database", "github_issues") \
        .option("collection", "closed_issues") \
        .load()

    # Afficher les données lues
    df_read.show()

finally:
    # Arrêter la session Spark
    spark.stop()
    print("✅ Spark session stopped.")
