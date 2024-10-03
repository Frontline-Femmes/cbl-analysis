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

# ... (other functions remain the same)

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

# ... (other analysis functions remain the same)

def generate_html_report(analyses_results):
    # Use Jinja2 for HTML templating
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

def save_to_json(data, filename='analysis_results.json'):
    json_path = os.path.join(DATA_DIR, filename)
    # ... (rest of the function remains the same)
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(converted_data, f, indent=2, default=default_serializer)
    logging.info(f"Analysis results saved to {json_path}")

if __name__ == "__main__":
    # Ensure directories exist
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(IMAGES_DIR, exist_ok=True)
    
    bans = load_bans()  # Load the bans data
    analyses_results, json_data = run_analyses(bans)
    generate_html_report(analyses_results)
    save_to_json(json_data)