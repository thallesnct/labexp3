import json
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from scipy.stats import pearsonr

question_map = {
    "a": "state_map",
    "b": "total_reviews"
}

rq_map = {
    "rq1": "size",
    "rq2": "review_time",
    "rq3": "description_size",
    "rq4": "interaction_amount"
}

label_map = {
    "state_map": "o feedback final das revisões",
    "total_reviews": "o número de revisões realizadas",
    "size": "o tamanho dos PRs",
    "review_time": "o tempo de análise dos PRs",
    "description_size": "a descrição dos PRs",
    "interaction_amount": "as interações dos PRs"
}

if __name__ == "__main__":
    correlation_values_file_path = '../data/correlation-values.json'
    prs_df_file_path = '../data/concatenated-pr-data.csv'
    correlation_values = None
    prs_df = pd.read_csv(prs_df_file_path)
    
    with open(correlation_values_file_path, 'r') as file:
        correlation_values = json.load(file)
        
    for question, rq_list in correlation_values.items():
        question_prop = question_map[question]
        question_label = label_map[question_prop]
        
        for rq, corr in rq_list.items():
            rq_prop = rq_map[rq]
            rq_label = label_map[rq_prop]
            
            # Create a scatter plot with transparency
            plt.figure(figsize=(24, 10))
            plt.scatter(prs_df[rq_prop], prs_df[question_prop], alpha=0.1, color='blue')  # Set alpha to 0.1 for transparency

            # Add labels and title
            plt.xlabel(rq_prop.title())
            plt.ylabel(question_prop.title())
            plt.title(f'Qual a relação entre {rq_label} e {question_label}? (Corr: {corr:.2f})')

            # plt.figure(figsize=(8, 6))
            # plt.hexbin(prs_df[question_prop], prs_df[rq_prop], gridsize=50, cmap='Blues', mincnt=1)

            # Add color bar to show density
            # cb = plt.colorbar(label='Counts in Bin')
            
            # Create a scatter plot with a regression line
            # plt.figure(figsize=(8, 6))
            # sns.regplot(x=rq_prop, y=question_prop, data=prs_df, scatter_kws={'s': 50}, line_kws={"color":"red"})

            # Add labels and title
            # plt.xlabel(rq_label)
            # plt.ylabel(question_label)
            # plt.show()
            plt.savefig(f'../charts/chart-{question}-{rq}.png')