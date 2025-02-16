from azure.storage.blob import BlobServiceClient
import json
from pyspark.sql import SparkSession
from pyspark.sql import Row

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
message = get_blob_data()
print(message)

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
    # Si le message est une liste de dictionnaires, passe-le directement au DataFrame
    if isinstance(message, list):
        df = spark.createDataFrame(message)
    else:
        # Sinon, transforme-le en une liste contenant ce dictionnaire
        df = spark.createDataFrame([message])

    # Afficher le message transformé
    df.show()

    # Insérer le message dans la collection MongoDB
    df.write.format("mongodb") \
        .option("database", "github_issues") \
        .option("collection", "issues") \
        .mode("append") \
        .save()

    print("✅ Données insérées avec succès dans MongoDB.")

    # Lire les données depuis MongoDB pour vérifier l'insertion
    df_read = spark.read.format("mongodb") \
        .option("database", "github_issues") \
        .option("collection", "issues") \
        .load()
    
    # Afficher les données lues
    df_read.show()

finally:
    # Arrêter la session Spark
    spark.stop()
    print("✅ Spark session stopped.")
