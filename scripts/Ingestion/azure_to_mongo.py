import os
import json
import re
import urllib.parse
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, datediff, col
from azure.storage.blob import BlobServiceClient
from pymongo import MongoClient
from transformers import T5Tokenizer, T5ForConditionalGeneration
import torch
# Initialisation du modèle T5
tokenizer = T5Tokenizer.from_pretrained("t5-small")
model = T5ForConditionalGeneration.from_pretrained("t5-small")

# Configuration MongoDB
MONGO_URI = 'mongodb://localhost:27017/'  # Remplace par ton URI MongoDB
DB_NAME = 'github_data'
COLLECTION_NAME = 'closed_issues'

# Configuration Azure Blob Storage
AZURE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=issuesstorage;AccountKey=Q7It5++J5VE7284S/QP+ZqHE1cT6Mad16bvyC+Eqx1j1xpRh5QlWMFJAzdmUC/DguMF3CmEsK87R+AStyWxtjg==;EndpointSuffix=core.windows.net"
CONTAINER_NAME = 'kafka-data'

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
    .config("spark.executor.memory", "4G") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.rpc.message.maxSize", "2000") \
    .config("spark.mongodb.connection.timeoutMS", "60000") \
    .config("spark.mongodb.socket.timeoutMS", "60000") \
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

def list_blob_hierarchy():
    """Lister tous les blobs JSON dans le conteneur Azure Blob avec encodage des caractères spéciaux."""
    blobs = container_client.list_blobs(name_starts_with="topics/git_data/year=2025/")
    files = set()

    for blob in blobs:
        try:
            # Encoder le nom du blob pour gérer les caractères spéciaux
            encoded_blob_name = urllib.parse.quote(blob.name, safe=":/")  # Encodage tout en préservant "/"

            # Ajouter uniquement les blobs qui se terminent par .json
            if blob.name.endswith('.json'):
                print(f"🔍 Blob trouvé : {blob.name} | Encodé : {encoded_blob_name}")  # Afficher le nom original et encodé
                files.add(blob.name)  # Ajouter la version originale
        except Exception as e:
            print(f"⚠️ Erreur lors du traitement du blob : {blob.name}. Erreur : {e}")
            continue

    print(f"📋 Total des blobs trouvés : {len(files)}")  # Afficher le total des blobs trouvés
    return files

# Lire le contenu d'un fichier JSON depuis Azure Blob Storage
def read_json_from_blob(blob_name):
    """Lire le fichier JSON depuis Azure Blob Storage, même en cas de JSON concaténés."""
    try:
        blob_client = container_client.get_blob_client(blob_name)
        blob_data = blob_client.download_blob().readall().decode("utf-8")

        # Vérifier si le fichier contient plusieurs objets JSON concaténés
        if blob_data.strip().startswith("[") and blob_data.strip().endswith("]"):
            # Si c'est une liste JSON valide
            return json.loads(blob_data)
        else:
            # Traiter les cas où les objets JSON sont concaténés (par exemple, un JSON par ligne)
            data = []
            for line in blob_data.strip().splitlines():
                if line.strip():  # Ignorer les lignes vides
                    data.append(json.loads(line))
            return data
    except json.JSONDecodeError as e:
        print(f"❌ Erreur de parsing JSON dans le blob {blob_name}: {e}")
        raise
    except Exception as e:
        print(f"❌ Erreur lors de la lecture du blob {blob_name}: {e}")
        raise
def clean_text_t5(text):
    """Nettoyer le texte en utilisant T5."""
    if not text:
        return ""
    input_text = "summarize: " + text
    inputs = tokenizer(input_text, return_tensors="pt", max_length=512, truncation=True)
    summary_ids = model.generate(**inputs, max_length=150, min_length=30, length_penalty=2.0, num_beams=4, early_stopping=True)
    return tokenizer.decode(summary_ids[0], skip_special_tokens=True)

# Traiter et enregistrer les issues fermées dans MongoDB
# Fonction pour attribuer la classe 'duration_class' 
def assign_duration_class(duration):
    if duration == 0:
        return 1
    elif duration in [1, 2]:
        return 2
    elif duration in [3, 4, 5]:
        return 3
    elif duration in [6, 7, 8]:
        return 4
    else:
        return 5

# Traiter et enregistrer les issues fermées dans MongoDB
def process_and_store_issues(data):
    """Traiter et stocker les issues fermées dans MongoDB."""
    rows = []
    for message in data:
        if 'closed_issues' in message:
            closed_issues = message['closed_issues']
            for issue in closed_issues:
                cleaned_body = clean_text_t5(issue.get('body', ''))
                
                # Calculer la durée
                created_at = issue.get('created_at')
                closed_at = issue.get('closed_at')
                duration = None
                duration_class = None

                if created_at and closed_at:
                    created_at_date = to_date(created_at)
                    closed_at_date = to_date(closed_at)
                    duration = datediff(closed_at_date, created_at_date)
                    duration_class = assign_duration_class(duration)

                rows.append({
                    'issue_id': issue.get('id', None),
                    'title': issue.get('title', ''),
                    'body': cleaned_body,
                    'state': issue.get('state', ''),
                    'created_at': created_at,
                    'closed_at': closed_at,
                    'language': message.get('language', ''),
                    'stars': message.get('stars', 0),
                    'duration': duration,
                    'duration_class': duration_class  
                })
                
    # Créer un DataFrame à partir des lignes
    df_issues = spark.createDataFrame(rows)

    
    df_issues = df_issues.withColumn("created_at", to_date(col("created_at"))) \
        .withColumn("closed_at", to_date(col("closed_at"))) \
        .withColumn("duration", datediff(col("closed_at"), col("created_at"))) \
        .fillna("")  # Gérer les valeurs manquantes

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
                print("++++++++++++++++++++++++++++++++++++++++++++")
                # Lire les données JSON depuis Azure Blob
                data = read_json_from_blob(file_path)
                print("++++++++++++++++++++++++++++++++++++++++++++")

                # Traiter et enregistrer les issues fermées dans MongoDB
                process_and_store_issues(data)
                print("++++++++++++++++++++++++++++++++++++++++++++")
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
