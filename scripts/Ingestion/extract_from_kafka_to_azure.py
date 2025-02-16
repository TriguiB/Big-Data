from confluent_kafka import Consumer, KafkaException
from azure.storage.blob import BlobServiceClient
import json

# 🔹 Configuration Kafka
KAFKA_CONFIG = {
    'bootstrap.servers': 'pkc-lgwgm.eastus2.azure.confluent.cloud:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': 'VOLOCHIQOXYMW6SC',
    'sasl.password': 'mfcvWe2Ol/SFvgWA18/CPoNpoLeTvrho2hdWlAoHhTIjW4Dam0Y7y5ZnC8mXG8xp',
    'group.id': 'azure-consumer-group',  # Groupe de consommateurs
    'auto.offset.reset': 'earliest'  # Lire depuis le début
}

TOPIC = "git_data"

# 🔹 Configuration Azure Storage
AZURE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=issuesstorage;AccountKey=Q7It5++J5VE7284S/QP+ZqHE1cT6Mad16bvyC+Eqx1j1xpRh5QlWMFJAzdmUC/DguMF3CmEsK87R+AStyWxtjg==;EndpointSuffix=core.windows.net"
CONTAINER_NAME = "kafka-data"
BLOB_NAME = "messages_test.json"

try:
    # Initialiser Kafka Consumer
    consumer = Consumer(KAFKA_CONFIG)
    consumer.subscribe([TOPIC])

    # Initialiser la connexion Azure Storage
    blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
    blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=BLOB_NAME)

    # Lire un seul message
    message = consumer.poll(timeout=10)  # Attendre 10 secondes un message
    
    if message is None:
        print("⚠️ Aucun message reçu.")
    elif message.error():
        print(f"❌ Erreur Kafka: {message.error()}")
    else:
        data = message.value().decode('utf-8')  # Décoder le message
        print(f"✅ Message reçu: {data}")

        # Vérifier si le message est en JSON valide
        try:
            json_data = json.loads(data)  # Convertir en JSON
            
            # Sauvegarde dans Azure Blob Storage
            blob_client.upload_blob(json.dumps(json_data), overwrite=True)  # Écrase s'il existe déjà
            print("✅ Message sauvegardé dans Azure Blob Storage")
        
        except json.JSONDecodeError as e:
            print(f"❌ Erreur JSON: {e}")

finally:
    # Fermer proprement le consumer Kafka
    consumer.close()
