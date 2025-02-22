import pymongo
import json
from azure.storage.blob import BlobServiceClient
from datetime import datetime

# Connexion à MongoDB
uri = "mongodb://mongodb:27017/"
client = pymongo.MongoClient(uri)
db = client["github_issues"]
collection = db["closed_issues"]

# Connexion à Azure Blob Storage
AZURE_STORAGE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=issuesstorage;AccountKey=Q7It5++J5VE7284S/QP+ZqHE1cT6Mad16bvyC+Eqx1j1xpRh5QlWMFJAzdmUC/DguMF3CmEsK87R+AStyWxtjg==;EndpointSuffix=core.windows.net"
AZURE_CONTAINER_NAME = 'kafka-data'

blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
container_client = blob_service_client.get_container_client(AZURE_CONTAINER_NAME)

# Fonction pour lire un fichier JSON depuis Azure Blob Storage
def read_json_from_blob(blob_name):
    """Lire le fichier JSON depuis Azure Blob Storage en gérant plusieurs formats."""
    try:
        blob_client = container_client.get_blob_client(blob_name)
        blob_data = blob_client.download_blob().readall().decode("utf-8")

        # Vérifier si c'est un tableau JSON ou une concaténation d'objets JSON
        if blob_data.strip().startswith("[") and blob_data.strip().endswith("]"):
            return json.loads(blob_data)  # Liste JSON valide
        else:
            # Gestion des fichiers contenant plusieurs objets JSON concaténés sans virgule
            data = []
            for line in blob_data.strip().splitlines():
                if line.strip():
                    try:
                        data.append(json.loads(line))
                    except json.JSONDecodeError as e:
                        print(f"⚠️ JSON partiellement corrompu dans {blob_name}, ligne ignorée: {e}")
            return data
    except json.JSONDecodeError as e:
        print(f"❌ Erreur JSON dans {blob_name}: {e}")
        return None
    except Exception as e:
        print(f"❌ Erreur de lecture du blob {blob_name}: {e}")
        return None

# Liste des fichiers à traiter
file_list = [
    "topics/git_data/year=2025/month=02/day=14/hour=00/git_data+2+0000000000.json",
    "topics/git_data/year=2025/month=02/day=15/hour=00/git_data+0+0000000211.json",
    "topics/git_data/year=2025/month=02/day=14/hour=00/git_data+1+0000000000.json",
    "topics/git_data/year=2025/month=02/day=14/hour=00/git_data+5+0000000000.json",
    "topics/git_data/year=2025/month=02/day=15/hour=00/git_data+5+0000000226.json",
    "topics/git_data/year=2025/month=02/day=15/hour=00/git_data+1+0000000264.json",
    "topics/git_data/year=2025/month=02/day=15/hour=00/git_data+4+0000000267.json",
    "topics/git_data/year=2025/month=02/day=14/hour=00/git_data+4+0000000002.json",
    "topics/git_data/year=2025/month=02/day=14/hour=00/git_data+3+0000000000.json",
    "topics/git_data/year=2025/month=02/day=14/hour=00/git_data+0+0000000000.json",
    "topics/git_data/year=2025/month=02/day=15/hour=00/git_data+2+0000000220.json"
]

# Fonction pour calculer la durée en jours entre created_at et closed_at
def calculate_duration(created_at, closed_at):
    """Calculer la différence en jours entre created_at et closed_at."""
    created_at = datetime.fromisoformat(created_at.replace("Z", "+00:00"))  # Convertir en datetime
    closed_at = datetime.fromisoformat(closed_at.replace("Z", "+00:00"))  # Convertir en datetime
    delta = closed_at - created_at
    return delta.days

# Parcourir et traiter chaque fichier
for file_path in file_list:
    try:
        print(f"📂 Traitement du fichier : {file_path}")
        repos_data = read_json_from_blob(file_path)

        # Vérifier si les données sont bien une liste d'objets (repos)
        if not isinstance(repos_data, list):
            print(f"⚠️ Données invalides pour {file_path}, ignorées.")
            continue

        # Parcours des repos pour extraire les closed_issues
        for repo in repos_data:
            closed_issues = repo.get("closed_issues", [])
            
            # Vérifier si la clé 'closed_issues' existe et contient des données
            if not isinstance(closed_issues, list):
                print(f"⚠️ 'closed_issues' non trouvé ou invalide dans {file_path}, ignoré.")
                continue

            # Mise à jour MongoDB pour chaque issue
            for issue in closed_issues:
                if not isinstance(issue, dict):  # Vérifier que chaque issue est un objet JSON valide
                    print(f"⚠️ Objet JSON invalide ignoré dans {file_path}: {issue}")
                    continue

                issue_id = issue.get("id")
                created_at = issue.get("created_at")
                closed_at = issue.get("closed_at")

                if issue_id and created_at and closed_at:
                    # Calculer la durée en jours
                    duration = calculate_duration(created_at, closed_at)

                    # Mise à jour dans MongoDB
                    result = collection.update_one(
                        {"issue_id": issue_id},
                        {"$set": {"duration": duration}}
                    )
                    if result.modified_count > 0:
                        print(f"✅ Issue {issue_id} mise à jour avec duration = {duration} jours")
                    else:
                        print(f"⚠️ Issue {issue_id} non trouvée ou déjà à jour.")

    except Exception as e:
        print(f"❌ Erreur lors du traitement de {file_path}: {e}")

print("🎯 Mise à jour MongoDB terminée !")
