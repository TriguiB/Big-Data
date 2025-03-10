import requests
import json
import os
import sys
from confluent_kafka import Producer

# Configuration GitHub API
GITHUB_TOKEN = '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' 
HEADERS = {'Authorization': f'token {GITHUB_TOKEN}'}
BASE_URL = 'https://api.github.com/search/repositories'
PER_PAGE = 100  # Max autorisé par GitHub

KAFKA_CONFIG = {
    'bootstrap.servers': 'pkc-lgwgm.eastus2.azure.confluent.cloud:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': 'VOLOCHIQOXYMW6SC',
    'sasl.password': 'mfcvWe2Ol/SFvgWA18/CPoNpoLeTvrho2hdWlAoHhTIjW4Dam0Y7y5ZnC8mXG8xp'
}
KAFKA_TOPIC = 'git_data'  # Nom du topic Kafka

# Initialisation du producteur Kafka
producer = Producer(KAFKA_CONFIG)

# Récupérer les repositories GitHub
def fetch_github_data(min_date, max_date, page):
    """Récupère une page de repositories en fonction des dates de création"""
    query = f'language:angular created:{min_date}..{max_date}'
    
    params = {
        'q': query,
        'sort': 'stars',
        'order': 'desc',
        'per_page': PER_PAGE,
        'page': page
    }
    
    try:
        response = requests.get(BASE_URL, headers=HEADERS, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"❌ Erreur API: {str(e)}")
        return None

# Récupérer les "closed issues" d'un repo
def fetch_closed_issues(repo_full_name):
    """Récupère toutes les issues fermées d'un repository"""
    issues_url = f'https://api.github.com/repos/{repo_full_name}/issues'
    params = {'state': 'closed', 'per_page': 100}

    closed_issues = []
    page = 1

    while True:
        try:
            response = requests.get(issues_url, headers=HEADERS, params={**params, 'page': page})
            response.raise_for_status()
            issues = response.json()

            if not issues:
                break

            for issue in issues:
                if 'pull_request' not in issue:
                    closed_issues.append({
                        'id': issue['id'],
                        'title': issue['title'],
                        'body': issue.get('body', ''),
                        'state': issue['state'],
                        'created_at': issue['created_at'],
                        'closed_at': issue.get('closed_at', None),
                    })

            page += 1

        except requests.exceptions.RequestException as e:
            print(f"⚠️ Erreur lors de la récupération des issues pour {repo_full_name}: {str(e)}")
            break

    return closed_issues

# Envoyer des données à Kafka
def send_to_kafka(data):
    try:
        producer.produce(KAFKA_TOPIC, json.dumps(data))
        producer.flush()
        print(f"✅ Données envoyées à Kafka pour {data['full_name']}")
    except Exception as e:
        print(f"❌ Erreur lors de l'envoi à Kafka: {str(e)}")

# Fonction principale
def main():
    # Lire les arguments
    if len(sys.argv) != 3:
        print("❌ Utilisation : python collect_data_auto.py <min_date> <max_date>")
        print("   Exemple : python collect_data_auto.py 2020-01-01 2022-12-31")
        sys.exit(1)

    min_date, max_date = sys.argv[1], sys.argv[2]

    for page in range(1, 11):  # Limite de 1 000 résultats sur GitHub API
        print(f"🔍 Traitement de la page {page} ({min_date} → {max_date})...")
        data = fetch_github_data(min_date, max_date, page)

        if not data or 'items' not in data:
            print("⚠️ Aucune donnée reçue ou fin des résultats.")
            break

        for repo in data['items']:
            print(f"📦 Récupération des issues fermées pour {repo['full_name']}...")
            closed_issues = fetch_closed_issues(repo['full_name'])

            repo_data = {
                'id': repo['id'],
                'name': repo['name'],
                'full_name': repo['full_name'],
                'language': repo.get('language', 'Unknown'),
                'stars': repo['stargazers_count'],
                'created_at': repo['created_at'],
                'closed_issues': closed_issues
            }

            send_to_kafka(repo_data)

    print("✅ Collecte terminée")

if __name__ == "__main__":
    main()
