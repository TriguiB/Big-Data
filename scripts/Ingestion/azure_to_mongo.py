import os
import json
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, datediff, col
from azure.storage.blob import BlobServiceClient
from pymongo import MongoClient

# Configuration MongoDB
MONGO_URI = 'mongodb://localhost:27017/'  # Remplace par ton URI MongoDB
DB_NAME = 'github_data'
COLLECTION_NAME = 'closed_issues'

# Configuration Azure Blob Storage
AZURE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=issuesstorage;AccountKey=Q7It5++J5VE7284S/QP+ZqHE1cT6Mad16bvyC+Eqx1j1xpRh5QlWMFJAzdmUC/DguMF3CmEsK87R+AStyWxtjg==;EndpointSuffix=core.windows.net"
CONTAINER_NAME = 'kafka_data'

# Connexion MongoDB
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

# Connexion Azure Blob Storage
blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
container_client = blob_service_client.get_container_client(CONTAINER_NAME)

# Initialiser Spark en mode local et avec MongoDB
spark = SparkSession.builder \
    .appName("MongoDB_Spark") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.4.1") \
    .config("spark.mongodb.read.connection.uri", "mongodb://mongodb:27017/github_issues") \
    .config("spark.mongodb.write.connection.uri", "mongodb://mongodb:27017/github_issues") \
    .config("spark.executor.memory", "1G") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# Lire le fichier extracted_data.txt
def read_extracted_data_file(file_path):
    """Lire le fichier de données extraites et renvoyer les chemins existants."""
    if not os.path.exists(file_path):
        return []

    with open(file_path, 'r') as file:
        return [line.strip() for line in file.readlines()]

# Ajouter de nouveaux chemins dans le fichier extracted_data.txt
def append_to_extracted_data_file(file_path, paths):
    """Ajouter les chemins au fichier extracted_data.txt."""
    with open(file_path, 'a') as file:
        for path in paths:
            file.write(path + '\n')

def is_valid_blob_name(blob_name):
    """Check if the blob name contains invalid characters."""
    invalid_chars = r'[\\/:*?"<>|]'
    return not re.search(invalid_chars, blob_name)

def list_blob_hierarchy():
    """Lister les fichiers JSON dans le conteneur Azure Blob à partir de topics/git_data/year=2025/."""
    blobs = container_client.list_blobs(name_starts_with="topics/git_data/year=2025/")
    files = set()

    for blob in blobs:
        if not is_valid_blob_name(blob.name):
            print(f"⚠️ Skipping blob with invalid name: {blob.name}")
            continue

        try:
            # Exemple de chemin : topics/git_data/year=2025/month=02/day=14/hour=00/git_data+0+0000000000.json
            if blob.name.endswith('.json'):  # Ne traiter que les fichiers JSON
                files.add(blob.name)
        except Exception as e:
            print(f"⚠️ Skipping blob due to error: {blob.name}. Error: {e}")
            continue

    return files

# Lire le contenu d'un fichier JSON depuis Azure Blob Storage
def read_json_from_blob(path):
    """Lire le fichier JSON depuis Azure Blob Storage."""
    blob_client = container_client.get_blob_client(path)
    blob_data = blob_client.download_blob()
    return json.loads(blob_data.readall())

# Traiter et enregistrer les issues fermées dans MongoDB
def process_and_store_issues(data):
    """Traiter et stocker les issues fermées dans MongoDB."""
    rows = []
    for message in data:
        if 'closed_issues' in message:
            closed_issues = message['closed_issues']
            for issue in closed_issues:
                rows.append({
                    'issue_id': issue.get('id', None),
                    'title': issue.get('title', ''),
                    'body': issue.get('body', ''),
                    'state': issue.get('state', ''),
                    'created_at': issue.get('created_at', None),
                    'closed_at': issue.get('closed_at', None),
                    'language': message.get('language', ''),
                    'stars': message.get('stars', 0)
                })

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

# Traiter les fichiers JSON et ajouter les nouveaux chemins
def process_json_files(extracted_data_path):
    """Traiter les fichiers JSON non encore ajoutés à extracted_data.txt."""
    # Lire les chemins existants dans extracted_data.txt
    existing_paths = read_extracted_data_file(extracted_data_path)

    # Lister les fichiers JSON dans Azure Blob Storage
    available_files = list_blob_hierarchy()

    # Filtrer les nouveaux fichiers non encore présents dans extracted_data.txt
    new_files = [file_path for file_path in available_files if file_path not in existing_paths]

    if new_files:
        print(f"⚡ Nouveaux fichiers à traiter : {len(new_files)}")

        # Ajouter les nouveaux fichiers dans extracted_data.txt
        append_to_extracted_data_file(extracted_data_path, new_files)

        for file_path in new_files:
            print(f"📄 Traitement du fichier JSON : {file_path}")
            try:
                # Lire les données JSON depuis Azure Blob
                data = read_json_from_blob(file_path)

                # Traiter et enregistrer les issues fermées dans MongoDB
                process_and_store_issues(data)

                print(f"✅ Traitement terminé pour {file_path}")
            except Exception as e:
                print(f"❌ Erreur lors du traitement du fichier {file_path}: {e}")
    else:
        print("🔍 Aucun nouveau fichier à traiter.")

if __name__ == "__main__":
    extracted_data_file_path = 'extracted_data.txt'

    # Processus principal
    process_json_files(extracted_data_file_path)

    # Arrêter la session Spark après le traitement
    spark.stop()
    print("✅ Spark session stopped.")