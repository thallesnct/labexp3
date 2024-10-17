import os
import json
import random
import requests

def load_query(filename):
    with open(filename, 'r') as file:
        return file.read()
    
GITHUB_GRAPHQL_API_URL = "https://api.github.com/graphql"
GITHUB_TOKEN = "TOKEN_1_HERE"
GITHUB_TOKEN_ALT = "TOKEN_2_HERE"
use_alt_header = False

def get_headers():
    global use_alt_header
    if use_alt_header:
        use_alt_header = False
        return {
            "Authorization": f"Bearer {GITHUB_TOKEN_ALT}"
        }
    else:
        use_alt_header = True
        return {
            "Authorization": f"Bearer {GITHUB_TOKEN}"
        }
    
def run_query(query, variables):
    response = requests.post(GITHUB_GRAPHQL_API_URL, json={'query': query, 'variables': variables}, headers=get_headers())
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Query failed to run and returned code: {response.status_code}. Text: {response.text}")
    
# Terminal utils

# ANSI escape codes for colors
ANSI_COLORS = [
    '\033[31m',  # Red
    '\033[32m',  # Green
    '\033[33m',  # Yellow
    '\033[34m',  # Blue
    '\033[35m',  # Magenta
    '\033[36m',  # Cyan
    '\033[37m',  # White
]
ANSI_COLORS_MAP = {
    "Red": '\033[31m',  # Red
    "Green": '\033[32m',  # Green
    "Yellow": '\033[33m',  # Yellow
    "Blue": '\033[34m',  # Blue
    "Magenta": '\033[35m',  # Magenta
    "Cyan": '\033[36m',  # Cyan
    "White": '\033[37m',  # White
}
RESET = '\033[0m'  # Default

def print_with_color_prefix(prefix, message, color_choice = None):
    print(f"{random.choice(ANSI_COLORS) if color_choice is None else ANSI_COLORS_MAP[color_choice]}{prefix}{RESET}{message}")

def dump_json_to_file(obj, path):
    obj_str = json.dumps(obj, indent=4)
    with open(path, "w") as outfile:
        outfile.write(obj_str)

def get_json_for_repos_with_selected_prs(append_consolidated_metrics = False):
    json_data = []
    directory = "../data/dumps"
    
    for filename in os.listdir(directory):
        if filename.startswith('cursor-') and filename.endswith('-selected-prs.json'):
            file_path = os.path.join(directory, filename)
            data = None
            failed_getting_metrics_data = False
            
            with open(file_path, 'r') as file:
                data = json.load(file)
                
            if (append_consolidated_metrics is True):
                metrics_file_path = file_path.replace('selected-prs', 'consolidated-metrics')
                try:
                    with open(metrics_file_path, 'r') as metrics_file:
                        metrics_data = json.load(metrics_file)
                        for pr_data in data:
                            pr_data["state_map"] = 0 if pr_data["state"] == "CLOSED" else 1
                            pr_data["repo_name"] = metrics_data["name"]
                            pr_data["repo_id"] = metrics_data["id"]
                            pr_data["repo_owner"] = metrics_data["owner"]
                            pr_data["repo_stars"] = metrics_data["stars"]
                            pr_data["repo_url"] = metrics_data["url"]
                            pr_data["repo_total_pull_requests"] = metrics_data["total_pull_requests"]
                            pr_data["repo_selected_prs_amount"] = metrics_data["selected_prs_amount"]
                        json_data.append(data)
                except Exception as e:
                    print(f"Couldn't get metrics for {metrics_file_path}")
            else:
                json_data.append(data)
    
    return json_data