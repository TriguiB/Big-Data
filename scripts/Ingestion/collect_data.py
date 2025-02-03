import requests
import json
import base64
import markdown
import os
from bs4 import BeautifulSoup

# =======================
# CONFIGURATION
# =======================

# GitHub Authentication
# IMPORTANT: Do not hardcode your token in production.
TOKEN = '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'  # Replace with your GitHub token
headers = {'Authorization': f'token {TOKEN}'}

# GitHub API configuration
query = 'language:python'
base_url = 'https://api.github.com/search/repositories'
per_page = 50

# File paths (placed under data/raw/ as suggested by your project tree)
output_file = os.path.join('data', 'raw', 'github_repos_with_issues.json')
repo_ids_file = os.path.join('data', 'raw', 'github_repos_ids.json')


# =======================
# HELPER FUNCTIONS
# =======================

def ensure_dir(file_path):
    """
    Ensure that the directory for file_path exists.
    """
    directory = os.path.dirname(file_path)
    if directory and not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)


def markdown_to_text(markdown_content):
    """
    Convert Markdown content to plain text.
    """
    html_content = markdown.markdown(markdown_content)
    soup = BeautifulSoup(html_content, 'html.parser')
    return soup.get_text()


def load_existing_data():
    """
    Load existing repository data from the JSON file.
    """
    if os.path.exists(output_file):
        with open(output_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    return []


def load_existing_repo_ids():
    """
    Load existing repository IDs from the JSON file.
    """
    if os.path.exists(repo_ids_file):
        with open(repo_ids_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}


def save_data(data):
    """
    Save the repository data to a JSON file.
    """
    ensure_dir(output_file)
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4)


def save_repo_ids(repo_ids):
    """
    Save the repository IDs mapping to a JSON file.
    """
    ensure_dir(repo_ids_file)
    with open(repo_ids_file, 'w', encoding='utf-8') as f:
        json.dump(repo_ids, f, indent=4)


# =======================
# MAIN FUNCTION
# =======================

def main():
    # Load previously saved data (if any)
    existing_data = load_existing_data()
    # Create a set of repository full names that have already been processed
    existing_repos = {repo['full_name'] for repo in existing_data}
    # Load previously saved repository IDs (as keys)
    repo_ids = load_existing_repo_ids()

    all_repos = []
    page = 1

    # Retrieve repositories using the GitHub Search API
    while True:
        url = f'{base_url}?q={query}&per_page={per_page}&page={page}'
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            print(f"Erreur {response.status_code} lors de la récupération des repositories.")
            break

        data = response.json()
        if 'items' in data:
            all_repos.extend(data['items'])
        else:
            break

        if len(data['items']) < per_page:
            # No more pages to fetch
            break

        page += 1

    print(f"Nombre total de repositories récupérés : {len(all_repos)}")

    # Start with the already-existing data
    detailed_data = existing_data

    for repo in all_repos:
        repo_name = repo.get('full_name')
        repo_id = repo.get('id')
        
        # Check if the repository has already been processed
        if repo_name in existing_repos or str(repo_id) in repo_ids:
            print(f"Le repository {repo_name} (ID: {repo_id}) est déjà récupéré. Ignoré.")
            continue

        print(f"Traitement du repository : {repo_name} (ID: {repo_id})")

        # Retrieve the repository's README file
        readme_url = f'https://api.github.com/repos/{repo_name}/readme'
        readme_response = requests.get(readme_url, headers=headers)
        readme_data = readme_response.json()
        readme_content = None

        if 'content' in readme_data:
            try:
                # Decode from Base64 and convert Markdown to plain text
                readme_base64 = readme_data['content']
                readme_decoded = base64.b64decode(readme_base64).decode('utf-8')
                readme_content = markdown_to_text(readme_decoded)
            except Exception as e:
                print(f"Erreur lors du décodage du README pour {repo_name}: {e}")

        # Retrieve collaborators information
        collaborators_url = f'https://api.github.com/repos/{repo_name}/collaborators'
        collaborators_response = requests.get(collaborators_url, headers=headers)
        collaborators = collaborators_response.json()
        num_collaborators = len(collaborators) if isinstance(collaborators, list) else 0

        # Retrieve closed issues (exclude pull requests)
        issues_url = f'https://api.github.com/repos/{repo_name}/issues?state=closed&per_page={per_page}'
        issues_response = requests.get(issues_url, headers=headers)
        issues = []
        if issues_response.status_code == 200:
            try:
                issues = issues_response.json()
            except json.JSONDecodeError:
                print(f"Erreur de décodage JSON pour les issues du repository {repo_name}")
        else:
            print(f"Erreur {issues_response.status_code} lors de la récupération des issues du repository {repo_name}")

        # Build the repository data dictionary
        repo_data = {
            'name': repo.get('name'),
            'full_name': repo_name,
            'stars': repo.get('stargazers_count'),
            'forks': repo.get('forks_count'),
            'readme': readme_content,
            'collaborators': num_collaborators,
            'closed_issues': []
        }

        # Process each closed issue (excluding pull requests)
        for issue in issues:
            if 'pull_request' not in issue:
                issue_details = {
                    'id': issue.get('id'),
                    'title': issue.get('title'),
                    'state': issue.get('state'),
                    'created_at': issue.get('created_at'),
                    'closed_at': issue.get('closed_at'),
                    'body': issue.get('body', ''),
                    'labels': [label.get('name') for label in issue.get('labels', [])],
                    'assignees': [assignee.get('login') for assignee in issue.get('assignees', [])],
                }
                repo_data['closed_issues'].append(issue_details)

        # Append the processed repository data and record its ID
        detailed_data.append(repo_data)
        existing_repos.add(repo_name)
        repo_ids[str(repo_id)] = repo_name  # Using string for key consistency

    # Save the updated data to disk
    save_data(detailed_data)
    save_repo_ids(repo_ids)

    print(f"Données enregistrées dans {output_file}")
    print(f"IDs des repositories enregistrés dans {repo_ids_file}")


if __name__ == "__main__":
    main()
