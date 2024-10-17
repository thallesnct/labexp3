from datetime import datetime, timezone
from pathlib import Path as path
import pandas as pd
import time
import json
from utils import load_query, run_query, print_with_color_prefix, dump_json_to_file, get_json_for_repos_with_selected_prs

repositories_data = []
config = {}
cursor = None
next_cursor = None
pr_page_cursor = None
pr_page_number = 0
current_repo_index = 0
progress = 0
date_format = "%Y-%m-%dT%H:%M:%SZ"

def save_config_file():
    config_path = f"../data/config.json"
    dump_json_to_file(obj=config, path=config_path)
    print(f"Updated config file.")
    
def should_remove_pr(pr):
    one_hour = 3600
    try:
        if pr["reviews"]["totalCount"] >= 1:
            created_at = datetime.strptime(pr['createdAt'], date_format)
            finished_at = datetime.strptime(pr['closedAt'], date_format)
            difference = (finished_at - created_at).total_seconds()
            
            if difference > one_hour:
                return False            
        
        return True
    except Exception as e:
        print("Error occurred while calculating should remove PR step for: ")
        print(json.dumps(pr, indent=4))
        return True


def get_time_since_last_pr_activity(pr):
    try:
        created_at = datetime.strptime(pr['createdAt'], date_format)
        finished_at = datetime.strptime(pr['closedAt'], date_format)
        time_since_last_pr_activity = finished_at - created_at
        
        return time_since_last_pr_activity.total_seconds()
    except Exception as e:
        print("Error occurred while calculating time since last activity for for: ")
        print(json.dumps(pr, indent=4))
        return -1

def get_pr_attrs(pr):
    return {
        "state": pr["state"],
        "description": len(pr["body"]),
        "additions": pr["additions"],
        "deletions": pr["deletions"],
        "changed_files": pr["changedFiles"],
        "closed_at": pr["closedAt"],
        "created_at": pr["createdAt"],
        "merged_at": pr["mergedAt"],
        "should_remove": should_remove_pr(pr),
        "time_since_last_activity": get_time_since_last_pr_activity(pr),
        "total_comments": pr["comments"]["totalCount"],
        "total_participants": pr["participants"]["totalCount"],
        "total_reviews": pr["reviews"]["totalCount"]
    }

def apply_pr_amount_threshold(repositories):
    filtered_repos = []
    for repo in repositories:
        pull_requests_count = repo["pullRequests"]["totalCount"]
        
        if pull_requests_count >= 100:            
            filtered_repos.append({
                "name": repo["name"],
                "id": repo["id"],
                "owner": repo["owner"]["login"],
                "stars": repo["stargazerCount"],
                "url": repo["url"],
                "total_pull_requests": pull_requests_count,
            })
    return filtered_repos

def execute_query_step(index, repo, internal_cursor=pr_page_cursor):
    prefix = f"Repo number: {index + 1} | "
    query = load_query('./queries/get_pull_requests.graphql')
    error = True
    retry_number = 0
    pr_list = None
    time.sleep(0 if retry_number % 2 == 0 else 4)
    
    while(error is True):
        try:
            variables = { "repoId": repo["id"] } if internal_cursor is None else { "repoId": repo["id"], "after": internal_cursor }
            print_with_color_prefix(prefix, f"RUNNING PR QUERY FOR REPO ID: {repo['id']} with cursor: { internal_cursor }", "Red")
            pr_list = run_query(query, variables)
            error = False
        except Exception as e:
            print_with_color_prefix(prefix, f"FAILED QUERYING PR DATA FOR REPO ID: {repo['id']}", "Red")
            error = True
            retry_number += 1
            seconds = 10 if retry_number > 10 else 4
            print_with_color_prefix(prefix, f"WAITING {seconds} SECONDS BEFORE RETRYING (retry n: {retry_number}) QUERY FOR REPO ID {repo['id']}...", "Red")
            time.sleep(seconds)
    
    return pr_list
            
def execute_pr_properties_step(pr_list, index, repo):
    prefix = f"Repo number: {index + 1} | "
    internal_pr_list = pr_list
    script_done_running = False
    tries = pr_page_number
    selected_prs = []
    selected_prs_file_path = f"../data/dumps/cursor-{str(cursor)}-repo-{index}-selected-prs.json"
    if (path(selected_prs_file_path).exists()):
        with open(selected_prs_file_path, 'r') as file:
            selected_prs = json.load(file)
        print_with_color_prefix(prefix, f"LOADED SELECTED PRS LIST FROM LOCAL FILE '{selected_prs_file_path}'", "Blue")
    
    def update_config_and_retrigger_query(update_page_cursor = True):
        global pr_page_number
        global pr_page_cursor
        nonlocal internal_pr_list
        if (update_page_cursor):
            pr_page_cursor = internal_pr_list["data"]["node"]["pullRequests"]["pageInfo"]["endCursor"]
            print_with_color_prefix(prefix, f"UPDATED CURSOR VARIABLE TO {pr_page_cursor}", "Blue")
            config["pr_page_cursor"] = pr_page_cursor
            pr_page_number = tries
            config["pr_page_number"] = pr_page_number
        save_config_file()
        
        internal_pr_list = execute_query_step(index, repo, internal_cursor=pr_page_cursor)
        
    def update_config_and_return_result():
        global pr_page_number
        global pr_page_cursor
        nonlocal script_done_running
        script_done_running = True
        pr_dataframe = pd.DataFrame(selected_prs)
        pr_dataframe["state"] = [pr["state"] for pr in selected_prs]
        pr_dataframe["total_reviews"] = [pr["total_reviews"] for pr in selected_prs]
        pr_dataframe["selected_prs_amount"] = int(len(selected_prs))
        print_with_color_prefix(prefix, f"GOT PR PROPERTIES FOR {repo['id']} SUCCESSFULLY!!", "Blue")
        pr_page_cursor = None
        pr_page_number = 0
        config["pr_page_cursor"] = None
        config["pr_page_number"] = 0
        config["current_repo_index"] = index + 1
        save_config_file()
        return pr_dataframe
    
    while(script_done_running is False):
        try:
            seconds_amount = 4 if index % 2 == 0 else 8
            print_with_color_prefix(prefix, f"ATTEMPT NUMBER {tries + 1} OF GETTING PR PROPERTIES FOR {repo['id']}", "Blue")
            mapped_prs = list(map(get_pr_attrs, internal_pr_list["data"]["node"]["pullRequests"]["nodes"]))
            dump_json_to_file(obj=mapped_prs, path=f"../data/dumps/cursor-{str(cursor)}-repo-{index}-prs-{tries * 100}-to-{(tries + 1) * 100}.json")
            filtered_prs = []
            for pr in mapped_prs:
                if (pr["should_remove"] is False):
                    filtered_prs.append(pr)
            dump_json_to_file(obj=filtered_prs, path=f"../data/dumps/cursor-{str(cursor)}-repo-{index}-prs-{tries * 100}-to-{(tries + 1) * 100}-filtered.json")
            has_next_page = internal_pr_list["data"]["node"]["pullRequests"]["pageInfo"]["hasNextPage"]
            
            if (not script_done_running and len(filtered_prs) == 0 and has_next_page):
                tries += 1
                print_with_color_prefix(prefix, f"LOOKING FROM INDEX {(tries - 1) * 100} UP TO INDEX {(tries) * 100} RETURNED 0 PULL REQUESTS WITH REVIEWS FOR {repo['id']}, SEARCHING FOR THE NEXT 100 PRS ON THE REPO...", "Blue")
                update_config_and_retrigger_query()
                
            elif has_next_page and len(filtered_prs) > 0 and len(selected_prs) < 100:
                print_with_color_prefix(prefix, f"SCORE! PROCESSING PULL REQUESTS FOUND...", "Blue")
                for pr in filtered_prs:
                    if len(selected_prs) < 100:
                        selected_prs.append(pr)
                dump_json_to_file(selected_prs, selected_prs_file_path)
                tries += 1
                if len(selected_prs) < 100:
                    print_with_color_prefix(prefix, f"SCORE! FOUND {len(filtered_prs)} PRS ADDING TO A TOTAL OF {len(selected_prs)} SELECTED, NOW LOOKING FOR ADDITIONAL PULL REQUESTS FROM INDEX {(tries - 1) * 100} UP TO INDEX {tries * 100} FOR REPO {repo['id']}, SEARCHING FOR THE NEXT 100 PRS ON THE REPO...", "Blue")
                    update_config_and_retrigger_query()
                else:
                    print_with_color_prefix(prefix, f"SCORE! FOUND {len(filtered_prs)} PRS ADDING TO A TOTAL OF {len(selected_prs)} SELECTED", "Blue")
                    return update_config_and_return_result()
            else:
                global pr_page_cursor
                for pr in filtered_prs:
                    selected_prs.append(pr)
                return update_config_and_return_result()

        except Exception as e:
            print(e)
            print_with_color_prefix(prefix, f"FAILURE MAPPING PR PROPERTIES FOR {repo['id']}", "Blue")
            if ("errors" in internal_pr_list):
                print_with_color_prefix(prefix, f"FOUND ERROR WHEN MAPPING PR PROPERTIES FOR {repo['id']}, RETRYING THE QUERY...", "Blue")
                time.sleep(seconds_amount)
                update_config_and_retrigger_query(update_page_cursor=False)

def add_pr_metrics(repositories, internal_cursor=pr_page_cursor):
    for index, repo in enumerate(repositories[current_repo_index:]):
        actual_index = index + current_repo_index
        prefix = f"Repo number: {actual_index + 1} | "
        if repo["total_pull_requests"] > 100:
            pr_list = execute_query_step(actual_index, repo, internal_cursor)
            pr_dataframe = execute_pr_properties_step(pr_list, actual_index, repo)

            try:
                print_with_color_prefix(prefix, f"CALCULATING PR METRICS FOR {repo['id']}", "Green")
                repo["selected_prs_amount"] = int(pr_dataframe.at[0, "selected_prs_amount"])
                repo["median_changed_files"] = pr_dataframe["changed_files"].median()
                repo["mean_changed_files"] = pr_dataframe["changed_files"].mean()
                repo["median_additions"] = pr_dataframe["additions"].median()
                repo["mean_additions"] = pr_dataframe["additions"].mean()
                repo["median_deletions"] = pr_dataframe["deletions"].median()
                repo["mean_deletions"] = pr_dataframe["deletions"].mean()
                repo["median_time_since_last_activity"] = pr_dataframe["time_since_last_activity"].median()
                repo["mean_time_since_last_activity"] = pr_dataframe["time_since_last_activity"].mean()
                repo["median_description_length"] = pr_dataframe["description"].median()
                repo["mean_description_length"] = pr_dataframe["description"].mean()
                repo["median_total_participants"] = pr_dataframe["total_participants"].median()
                repo["mean_total_participants"] = pr_dataframe["total_participants"].mean()
                repo["median_total_comments"] = pr_dataframe["total_comments"].median()
                repo["mean_total_comments"] = pr_dataframe["total_comments"].mean()
                print_with_color_prefix(prefix, f"CALCULATED PR METRICS FOR {repo['id']} SUCCESSFULLY!!", "Green")
                print(repo)
                dump_json_to_file(repo, f"../data/dumps/cursor-{str(cursor)}-repo-{actual_index}-consolidated-metrics.json")
            except Exception as e:
                print_with_color_prefix(prefix, e.with_traceback(), "Green")
                print_with_color_prefix(prefix, f"FAILURE CALCULATING PR METRICS FOR {repo['id']}", "Green")

if __name__ == "__main__":
    with open('../data/config.json', 'r') as file:
        config = json.load(file)
    print("==========================    config.json     ==========================")
    print(json.dumps(config, indent=4))
    print("========================== End of config.json ==========================")
    query = load_query('./queries/get_popular_repos.graphql')
    repo_amount = 200
    step_size = 20
    iteration = 0
    pr_page_cursor = config["pr_page_cursor"] if "pr_page_cursor" in config else pr_page_cursor
    cursor = config["cursor"] if "cursor" in config else cursor
    progress = config["progress"] if "progress" in config else progress
    pr_page_number = config["pr_page_number"] if "pr_page_number" in config else pr_page_number
    is_first_run = True
    
    # while progress < 1000 and len(repositories_data) < 200:
    while progress < repo_amount:
        config["cursor"] = cursor
        pr_page_cursor = None if is_first_run is False else pr_page_cursor
        config["pr_page_cursor"] = pr_page_cursor
        current_repo_index = config["current_repo_index"] if is_first_run and "current_repo_index" in config else 0
        config["current_repo_index"] = current_repo_index
        print(f"({(progress/repo_amount) * 100}% Progress) Started running query with cursor: {cursor}")
        variables = {"cursor": cursor}
        result = run_query(query, variables)
        repositories = result["data"]["search"]["nodes"]
        next_cursor = result["data"]["search"]["pageInfo"]["endCursor"] if result["data"]["search"]["pageInfo"]["hasNextPage"] else None
        
        add_pr_metrics(apply_pr_amount_threshold(repositories), internal_cursor=pr_page_cursor)
        progress = len(get_json_for_repos_with_selected_prs())
        config["progress"] = progress
        
        print(f"({(progress/repo_amount) * 100}% Progress) Running query with cursor: {cursor}")
        iteration += 1
        
        save_config_file()
        
        is_first_run = False
        if result["data"]["search"]["pageInfo"]["hasNextPage"] and len(repositories_data) < repo_amount:
            cursor = next_cursor
            time.sleep(10)
        else:
            break

    print("Finished extracting pull request data from selected repos.")
