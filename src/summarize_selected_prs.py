import pandas as pd
from utils import print_with_color_prefix, dump_json_to_file, get_json_for_repos_with_selected_prs

if __name__ == "__main__":
    prs = get_json_for_repos_with_selected_prs(append_consolidated_metrics=True)
    prs = sum(prs, [])
    
    dump_json_to_file(prs, '../data/concatenated-pr-data.json')
    
    prs_df = pd.DataFrame(prs)
    prs_df["size"] = prs_df["changed_files"] + prs_df["additions"] + prs_df["deletions"]
    prs_df["review_time"] = prs_df["time_since_last_activity"] / 3600
    prs_df["description_size"] = prs_df["description"]
    prs_df["interaction_amount"] = prs_df["total_participants"] + prs_df["total_comments"]
    
    prs_df.to_csv('../data/concatenated-pr-data.csv')
    
    merged_prs_df = prs_df[prs_df["state_map"] == 1]
    closed_prs_df = prs_df[prs_df["state_map"] == 0]

    correlation_values = {
        "a": {
            "rq1": prs_df["size"].corr(prs_df["state_map"]),
            "rq2": prs_df["review_time"].corr(prs_df["state_map"]),
            "rq3": prs_df["description_size"].corr(prs_df["state_map"]),
            "rq4": prs_df["interaction_amount"].corr(prs_df["state_map"]),
        },
        "b": {
            "rq1": prs_df["size"].corr(prs_df["total_reviews"]),
            "rq2": prs_df["review_time"].corr(prs_df["total_reviews"]),
            "rq3": prs_df["description_size"].corr(prs_df["total_reviews"]),
            "rq4": prs_df["interaction_amount"].corr(prs_df["total_reviews"]),
        }
    }
    
    dump_json_to_file(correlation_values, '../data/correlation-values.json')

    summarized_median_data = {
        "all_prs_median_size": prs_df["changed_files"].median() + prs_df["additions"].median() + prs_df["deletions"].median(),
        "merged_prs_median_size": merged_prs_df["changed_files"].median() + merged_prs_df["additions"].median() + merged_prs_df["deletions"].median(),
        "closed_prs_median_size": closed_prs_df["changed_files"].median() + closed_prs_df["additions"].median() + closed_prs_df["deletions"].median(),
        "median_review_time": prs_df["time_since_last_activity"].median() / 3600,
        "all_prs_median_review_time": prs_df["time_since_last_activity"].median() / 3600,
        "merged_prs_median_review_time": merged_prs_df["time_since_last_activity"].median() / 3600,
        "closed_prs_median_review_time": closed_prs_df["time_since_last_activity"].median() / 3600,
        "all_prs_median_description_size": prs_df["description"].median(),
        "merged_prs_median_description_size": merged_prs_df["description"].median(),
        "closed_prs_median_description_size": closed_prs_df["description"].median(),
        "all_prs_median_interaction_amount": prs_df["total_participants"].median() + prs_df["total_comments"].median(),
        "merged_prs_median_interaction_amount": merged_prs_df["total_participants"].median() + merged_prs_df["total_comments"].median(),
        "closed_prs_median_interaction_amount": closed_prs_df["total_participants"].median() + closed_prs_df["total_comments"].median(),
    }

    dump_json_to_file(summarized_median_data, '../data/summarized-median-data.json')