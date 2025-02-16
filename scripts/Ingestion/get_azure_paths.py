from azure.storage.blob import BlobServiceClient

# Connexion à Azure Blob Storage
AZURE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=issuesstorage;AccountKey=Q7It5++J5VE7284S/QP+ZqHE1cT6Mad16bvyC+Eqx1j1xpRh5QlWMFJAzdmUC/DguMF3CmEsK87R+AStyWxtjg==;EndpointSuffix=core.windows.net"
CONTAINER_NAME = "kafka-data"

# Créer une instance du service Blob
blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)

# Obtenir une référence au conteneur
container_client = blob_service_client.get_container_client(CONTAINER_NAME)

# Fonction pour lister la hiérarchie des répertoires
def list_blob_hierarchy():
    # Lister tous les blobs dans le conteneur
    blobs = container_client.list_blobs()

    # Créer un ensemble pour contenir les répertoires uniques
    directories = set()

    # Parcourir les blobs et extraire les répertoires
    for blob in blobs:
        # Séparer le chemin du blob avec "/" et ajouter les répertoires à l'ensemble
        parts = blob.name.split('/')
        for i in range(1, len(parts)):
            directories.add('/'.join(parts[:i]))

    # Afficher la hiérarchie des répertoires
    if directories:
        print("Hiérarchie des répertoires dans le conteneur:")
        for directory in sorted(directories):
            print(directory)
    else:
        print("Aucune hiérarchie de répertoire trouvée.")

# Appeler la fonction
list_blob_hierarchy()
