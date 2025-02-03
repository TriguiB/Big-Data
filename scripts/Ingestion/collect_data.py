import os
import json
import base64
import requests
import pandas as pd
import markdown
import logging
from bs4 import BeautifulSoup
from datetime import datetime

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Configuration: GitHub Token & Headers (Replace with your own)
TOKEN = os.getenv("GITHUB_TOKEN")  # Store securely as an environment variable
if not TOKEN:
    raise ValueError("GitHub Token is missing! Set it as an environment variable.")
    
HEADERS = {'Authorization': f'token {TOKEN}'}
QUERY = "language:python"
BASE_URL = "https://api.github.com/search/repositories"
PER_PAGE = 50

# Paths (Ensure these folders exist)
RAW_DATA_PATH = "data/raw/github_repos_raw.json"
PROCESSED_DATA_PATH = "data/processed/github_repos_processed.csv"

# Function to convert Markdown to plain text
def markdown_to_text(md_content):
    html_content = markdown.markdown(md_content)
    soup = BeautifulSoup(html_content, "html.parser")
    return soup.get_text()

# Function to fetch repository data from GitHub API
def fetch_repositories():
    logging.info("Fetching repositories from GitHub...")
    all_repos = []
    page = 1

    while True:
        url = f"{BASE_URL}?q={QUERY}&per_page={PER_PAGE}&page={page}"
        response = requests.get(url, headers=HEADERS)

        if response.status_code != 200:
            logging.error(f"Error {response.status_code}: {response.text}")
            break

        data = response.json()
        if "items" in data:
            all_repos.extend(data["items"])
        else:
            break

        if len(data["items"]) < PER_PAGE:
            break  # Stop when there are no more results

        page += 1

    logging.info(f"Total repositories fetched: {len(all_repos)}")
    return all_repos

# Function to fetch repository details
def fetch_repository_details(repo):
    repo_name = repo["full_name"]
    repo_id = repo["id"]

    logging.info(f"Processing repository: {repo_name} (ID: {repo_id})")

    # Fetch README
    readme_url = f"https://api.github.com/repos/{repo_name}/readme"
    readme_response = requests.get(readme_url, headers=HEADERS)
    readme_content = None

    if readme_response.status_code == 200:
        readme_data = readme_response.json()
        if "content" in readme_data:
            readme_decoded = base64.b64decode(readme_data["content"]).decode("utf-8")
            readme_content = markdown_to_text(readme_decoded)

    # Fetch collaborators
    collaborators_url = f"https://api.github.com/repos/{repo_name}/collaborators"
    collaborators_response = requests.get(collaborators_url, headers=HEADERS)
    num_collaborators = len(collaborators_response.json()) if collaborators_response.status_code == 200 else 0

    # Fetch closed issues
    issues_url = f"https://api.github.com/repos/{repo_name}/issues?state=closed&per_page={PER_PAGE}"
    issues_response = requests.get(issues_url, headers=HEADERS)
    
    closed_issues = []
    if issues_response.status_code == 200:
        for issue in issues_response.json():
            if "pull_request" not in issue:  # Ignore pull requests
                closed_issues.append({
                    "id": issue["id"],
                    "title": issue["title"],
                    "state": issue["state"],
                    "created_at": issue["created_at"],
                    "closed_at": issue["closed_at"],
                    "body": issue.get("body", ""),
                    "labels": [label["name"] for label in issue.get("labels", [])],
                    "assignees": [assignee["login"] for assignee in issue.get("assignees", [])],
                })

    # Return repository details
    return {
        "repo_id": repo_id,
        "name": repo["name"],
        "full_name": repo["full_name"],
        "stars": repo["stargazers_count"],
        "forks": repo["forks_count"],
        "readme": readme_content,
        "collaborators": num_collaborators,
        "closed_issues": closed_issues,
    }

# Function to save raw data
def save_raw_data(data):
    logging.info(f"Saving raw data to {RAW_DATA_PATH}...")
    os.makedirs(os.path.dirname(RAW_DATA_PATH), exist_ok=True)
    with open(RAW_DATA_PATH, "w") as f:
        json.dump(data, f, indent=4)

# Function to process and save structured data
def process_and_save_data(raw_data):
    logging.info("Processing data and saving structured output...")

    # Transform raw data into a DataFrame
    processed_data = []
    for repo in raw_data:
        for issue in repo["closed_issues"]:
            processed_data.append({
                "repo_id": repo["repo_id"],
                "repo_name": repo["full_name"],
                "stars": repo["stars"],
                "forks": repo["forks"],
                "collaborators": repo["collaborators"],
                "issue_id": issue["id"],
                "issue_title": issue["title"],
                "issue_created_at": issue["created_at"],
                "issue_closed_at": issue["closed_at"],
                "issue_body": issue["body"],
                "issue_labels": ", ".join(issue["labels"]),
                "issue_assignees": ", ".join(issue["assignees"]),
            })

    # Convert to DataFrame
    df = pd.DataFrame(processed_data)

    # Save processed data
    os.makedirs(os.path.dirname(PROCESSED_DATA_PATH), exist_ok=True)
    df.to_csv(PROCESSED_DATA_PATH, index=False)
    logging.info(f"Structured data saved to {PROCESSED_DATA_PATH}")

# Main function to run ingestion pipeline
def main():
    logging.info("Starting data ingestion pipeline...")

    # Fetch repositories
    raw_repos = fetch_repositories()

    # Fetch details for each repository
    detailed_repos = [fetch_repository_details(repo) for repo in raw_repos]

    # Save raw data
    save_raw_data(detailed_repos)

    # Process and save structured data
    process_and_save_data(detailed_repos)

    logging.info("Data ingestion pipeline completed successfully.")

if __name__ == "__main__":
    main()
