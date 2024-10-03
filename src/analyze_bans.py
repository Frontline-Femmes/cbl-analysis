import csv
import json
from collections import Counter
import matplotlib.pyplot as plt
from datetime import datetime
import os
import pandas as pd
import numpy as np
import seaborn as sns
from sklearn.cluster import KMeans
from sklearn.preprocessing import MultiLabelBinarizer
import traceback
from pytz import UTC
import logging
from jinja2 import Environment, FileSystemLoader
import matplotlib.dates as mdates

# Initialize logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Update paths
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
IMAGES_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'images')
TEMPLATES_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'templates')

def load_bans(file_path='cbl_bans.csv'):
    full_path = os.path.join(DATA_DIR, file_path)
    logging.info(f"Loading bans from {full_path}")
    bans = []
    with open(full_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            bans.append(row)
    return bans

def parse_reasons(reason_string):
    return [reason.strip() for reason in reason_string.split(',') if reason.strip()]

def run_analysis(analysis_func, title, explanation, bans, *args):
    try:
        logging.info(f"Starting analysis: {title}")
        plt.figure(figsize=(12, 8))
        data = analysis_func(bans, *args)
        img_name = f"{title.lower().replace(' ', '_')}.png"
        img_path = os.path.join(IMAGES_DIR, img_name)
        plt.savefig(img_path)
        plt.close()
        logging.info(f"Successfully generated {img_path}")
        return {
            'title': title,
            'explanation': explanation,
            'image': img_name,  # Just the filename, not the full path
            'data': data
        }
    except Exception as e:
        logging.error(f"Error in {title}: {str(e)}", exc_info=True)
        print(f"Data sample for debugging {title}:")
        print(pd.DataFrame(bans).head())
        return None

def analyze_correlation_between_ban_reasons(bans):
    bans_df = pd.DataFrame(bans)
    bans_df['parsed_reasons'] = bans_df['reason'].apply(parse_reasons)
    mlb = MultiLabelBinarizer()
    reasons_encoded = mlb.fit_transform(bans_df['parsed_reasons'])
    reason_columns = mlb.classes_
    reasons_df = pd.DataFrame(reasons_encoded, columns=reason_columns)
    corr_matrix = reasons_df.corr()
    sns.heatmap(corr_matrix, cmap='coolwarm', xticklabels=True, yticklabels=True)
    plt.title('Correlation Between Ban Reasons')
    plt.tight_layout()
    return corr_matrix.to_dict()

def analyze_temporal_trends_in_ban_reasons(bans):
    bans_df = pd.DataFrame(bans)
    bans_df['created'] = pd.to_datetime(bans_df['created']).dt.tz_localize(None)
    bans_df['year_month'] = bans_df['created'].dt.to_period('M')
    bans_df['parsed_reasons'] = bans_df['reason'].apply(parse_reasons)
    bans_exploded = bans_df.explode('parsed_reasons')
    trends = bans_exploded.groupby(['year_month', 'parsed_reasons']).size().reset_index(name='counts')
    trends_pivot = trends.pivot(index='year_month', columns='parsed_reasons', values='counts').fillna(0)
    top_reasons = bans_exploded['parsed_reasons'].value_counts().head(5).index
    trends_pivot[top_reasons].plot(kind='line')
    plt.title('Temporal Trends of Top Ban Reasons')
    plt.xlabel('Year-Month')
    plt.ylabel('Number of Bans')
    plt.legend(title='Ban Reasons')
    plt.tight_layout()
    return trends_pivot[top_reasons].to_dict()

def analyze_organizational_differences(bans):
    bans_df = pd.DataFrame(bans)
    bans_df['parsed_reasons'] = bans_df['reason'].apply(parse_reasons)
    bans_exploded = bans_df.explode('parsed_reasons')
    org_reason_counts = bans_exploded.groupby(['organisation_name', 'parsed_reasons']).size().reset_index(name='counts')
    org_reason_pivot = org_reason_counts.pivot(index='organisation_name', columns='parsed_reasons', values='counts').fillna(0)
    org_total_bans = org_reason_pivot.sum(axis=1)
    org_reason_normalized = org_reason_pivot.div(org_total_bans, axis=0)
    top_orgs = org_total_bans.sort_values(ascending=False).head(10).index
    org_sample = org_reason_normalized.loc[top_orgs]
    sns.heatmap(org_sample, cmap='viridis', xticklabels=True, yticklabels=True)
    plt.title('Organizational Differences in Ban Enforcement (Top 10 Organizations)')
    plt.tight_layout()
    return org_sample.to_dict()

def analyze_ban_durations_and_severity(bans):
    bans_df = pd.DataFrame(bans)
    bans_df['created'] = pd.to_datetime(bans_df['created'])
    bans_df['expires'] = pd.to_datetime(bans_df['expires'], errors='coerce')
    bans_df['parsed_reasons'] = bans_df['reason'].apply(parse_reasons)
    bans_df['ban_duration'] = (bans_df['expires'] - bans_df['created']).dt.total_seconds() / (3600 * 24)
    bans_df['ban_duration'] = bans_df['ban_duration'].fillna(-1)  # -1 indicates permanent ban
    bans_exploded = bans_df.explode('parsed_reasons')
    valid_durations = bans_exploded[bans_exploded['ban_duration'] >= 0]
    sns.boxplot(x='parsed_reasons', y='ban_duration', data=valid_durations)
    plt.title('Ban Durations by Reason')
    plt.xlabel('Ban Reason')
    plt.ylabel('Ban Duration (days)')
    plt.xticks(rotation=45)
    plt.tight_layout()
    return valid_durations.groupby('parsed_reasons')['ban_duration'].describe().to_dict()

def analyze_seasonal_and_weekly_patterns(bans):
    bans_df = pd.DataFrame(bans)
    bans_df['created'] = pd.to_datetime(bans_df['created'])
    bans_df['day_of_week'] = bans_df['created'].dt.day_name()
    bans_df['month'] = bans_df['created'].dt.month_name()
    day_counts = bans_df['day_of_week'].value_counts().reindex(['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday'])
    month_counts = bans_df['month'].value_counts().reindex([
        'January', 'February', 'March', 'April', 'May', 'June',
        'July', 'August', 'September', 'October', 'November', 'December'
    ])
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 12))
    day_counts.plot(kind='bar', ax=ax1)
    ax1.set_title('Bans by Day of Week')
    ax1.set_xlabel('Day')
    ax1.set_ylabel('Number of Bans')
    month_counts.plot(kind='bar', ax=ax2)
    ax2.set_title('Bans by Month')
    ax2.set_xlabel('Month')
    ax2.set_ylabel('Number of Bans')
    plt.tight_layout()
    return {'day_counts': day_counts.to_dict(), 'month_counts': month_counts.to_dict()}

def analyze_clustering_of_ban_reasons(bans):
    bans_df = pd.DataFrame(bans)
    bans_df['parsed_reasons'] = bans_df['reason'].apply(parse_reasons)
    mlb = MultiLabelBinarizer()
    reasons_encoded = mlb.fit_transform(bans_df['parsed_reasons'])
    kmeans = KMeans(n_clusters=5, random_state=42)
    kmeans.fit(reasons_encoded)
    bans_df['cluster'] = kmeans.labels_
    cluster_counts = bans_df['cluster'].value_counts().sort_index()
    cluster_counts.plot(kind='bar')
    plt.title('Ban Reason Clusters')
    plt.xlabel('Cluster')
    plt.ylabel('Number of Bans')
    plt.tight_layout()
    return cluster_counts.to_dict()

def analyze_emerging_behaviors(bans):
    bans_df = pd.DataFrame(bans)
    bans_df['created'] = pd.to_datetime(bans_df['created'])
    bans_df['year'] = bans_df['created'].dt.year
    bans_df['parsed_reasons'] = bans_df['reason'].apply(parse_reasons)
    bans_exploded = bans_df.explode('parsed_reasons')
    yearly_reason_counts = bans_exploded.groupby(['year', 'parsed_reasons']).size().reset_index(name='counts')
    reason_year_pivot = yearly_reason_counts.pivot(index='year', columns='parsed_reasons', values='counts').fillna(0)
    new_reasons_by_year = (reason_year_pivot > 0).astype(int).diff().fillna(0)
    new_reasons = new_reasons_by_year.columns[(new_reasons_by_year.sum() > 0)]
    new_reasons_trends = reason_year_pivot[new_reasons]
    new_reasons_trends.plot(kind='bar', stacked=True)
    plt.title('Emerging Ban Reasons Over Years')
    plt.xlabel('Year')
    plt.ylabel('Number of Bans')
    plt.legend(title='Ban Reasons', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()
    return new_reasons_trends.to_dict()

def analyze_ban_reason_combinations(bans):
    bans_df = pd.DataFrame(bans)
    reason_combinations = bans_df['reason'].value_counts().head(10)
    reason_combinations.plot(kind='bar')
    plt.title('Top 10 Ban Reason Combinations')
    plt.xlabel('Reason Combination')
    plt.ylabel('Frequency')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    return reason_combinations.to_dict()

def run_analyses(bans):
    analyses = [
        (analyze_correlation_between_ban_reasons, "Correlation Between Ban Reasons", "This heatmap shows the correlation between different ban reasons. Stronger correlations indicate that certain ban reasons often occur together."),
        (analyze_temporal_trends_in_ban_reasons, "Temporal Trends in Ban Reasons", "This line chart displays how the frequency of top ban reasons has changed over time, helping identify emerging or declining problematic behaviors."),
        (analyze_organizational_differences, "Organizational Differences in Ban Enforcement", "This heatmap illustrates how different organizations enforce bans, highlighting variations in moderation practices across communities."),
        (analyze_ban_durations_and_severity, "Ban Durations and Severity Correlation", "This boxplot shows the distribution of ban durations for different ban reasons, indicating how severity of punishment correlates with specific offenses."),
        (analyze_seasonal_and_weekly_patterns, "Seasonal and Weekly Patterns", "These bar charts display ban frequencies by day of the week and month, revealing temporal patterns in ban occurrences."),
        (analyze_clustering_of_ban_reasons, "Clustering of Ban Reasons", "This bar chart shows clusters of ban reasons, potentially revealing underlying patterns or categories of problematic behavior."),
        (analyze_emerging_behaviors, "Trend Analysis of Emerging Behaviors", "This stacked bar chart illustrates the emergence and growth of new ban reasons over time, highlighting evolving problematic behaviors."),
        (analyze_ban_reason_combinations, "Analysis of Ban Reason Combinations", "This bar chart shows the most common combinations of ban reasons, revealing frequently co-occurring problematic behaviors.")
    ]
    
    results = []
    json_data = {}
    for func, title, explanation in analyses:
        result = run_analysis(func, title, explanation, bans)
        if result:
            results.append(result)
            json_data[title] = result['data']
    
    return results, json_data

def generate_html_report(analyses_results):
    env = Environment(loader=FileSystemLoader(TEMPLATES_DIR))
    template = env.get_template('report_template.html')

    analysis_date = datetime.now().strftime("%Y-%m-%d")
    html_content = template.render(
        analysis_date=analysis_date,
        analyses_results=analyses_results
    )

    index_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'index.html')
    with open(index_path, 'w', encoding='utf-8') as f:
        f.write(html_content)
    logging.info(f"HTML report generated as {index_path}")

def default_serializer(obj):
    if isinstance(obj, (datetime, pd.Timestamp)):
        return obj.isoformat()
    elif isinstance(obj, pd.Period):
        return str(obj)
    raise TypeError(f"Type {type(obj)} not serializable")

def save_to_json(data, filename='analysis_results.json'):
    json_path = os.path.join(DATA_DIR, filename)
    converted_data = json.loads(json.dumps(data, default=default_serializer))
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(converted_data, f, indent=2, default=default_serializer)
    logging.info(f"Analysis results saved to {json_path}")

if __name__ == "__main__":
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(IMAGES_DIR, exist_ok=True)
    
    bans = load_bans()
    analyses_results, json_data = run_analyses(bans)
    generate_html_report(analyses_results)
    save_to_json(json_data)