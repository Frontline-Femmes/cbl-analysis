import requests
import csv
import json
from time import sleep
import signal
import sys
import os
import argparse

# Update paths
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
CHECKPOINT_FILE = os.path.join(DATA_DIR, 'bans_checkpoint.json')
CSV_FILE = os.path.join(DATA_DIR, 'cbl_bans.csv')

# GraphQL endpoint
GRAPHQL_ENDPOINT = 'https://communitybanlist.com/graphql'
BANS_PER_PAGE = 500

# GraphQL query to fetch all bans
query = """
query GetAllBans($after: String, $first: Int) {
  bans(first: $first, after: $after) {
    pageInfo {
      hasNextPage
      endCursor
    }
    edges {
      node {
        id
        created
        expires
        reason
        steamUser {
          id
          name
        }
        banList {
          name
          organisation {
            name
            discord
          }
        }
      }
    }
  }
}
"""

def fetch_bans(after):
    variables = {"after": after, "first": BANS_PER_PAGE}
    response = requests.post(GRAPHQL_ENDPOINT, json={'query': query, 'variables': variables})
    if response.status_code == 200:
        return response.json()['data']['bans']
    else:
        print(f"Error fetching data: {response.status_code}")
        return None

def save_checkpoint(after):
    temp_file = CHECKPOINT_FILE + '.tmp'
    with open(temp_file, 'w') as f:
        json.dump({"after": after}, f)
    os.replace(temp_file, CHECKPOINT_FILE)

def load_checkpoint():
    try:
        with open(CHECKPOINT_FILE, 'r') as f:
            data = json.load(f)
        return data.get("after")
    except FileNotFoundError:
        return None
    except json.JSONDecodeError:
        print("Error: bans_checkpoint.json is corrupted. Starting from the beginning.")
        return None

def signal_handler(sig, frame):
    print("\nInterrupt received. Saving progress and exiting...")
    save_checkpoint(current_after)
    sys.exit(0)

def fetch_all_bans():
    global current_after
    current_after = load_checkpoint()
    has_next_page = True
    
    signal.signal(signal.SIGINT, signal_handler)
    
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(CSV_FILE, 'a', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=[
            'id', 'created', 'expires', 'reason', 'steam_user_id', 'steam_user_name',
            'ban_list_name', 'organisation_name', 'organisation_discord'
        ])
        if not os.path.isfile(CSV_FILE):
            writer.writeheader()
        
        try:
            total_bans_fetched = 0
            bans_since_last_checkpoint = 0
            while has_next_page:
                result = fetch_bans(current_after)
                if result:
                    bans = result['edges']
                    if not bans:
                        print("No more bans to fetch. Exiting.")
                        break
                    
                    for ban_edge in bans:
                        ban = ban_edge['node']
                        row = {
                            'id': ban['id'],
                            'created': ban['created'],
                            'expires': ban['expires'],
                            'reason': ban['reason'],
                            'steam_user_id': ban['steamUser']['id'],
                            'steam_user_name': ban['steamUser']['name'],
                            'ban_list_name': ban['banList']['name'],
                            'organisation_name': ban['banList']['organisation']['name'],
                            'organisation_discord': ban['banList']['organisation']['discord']
                        }
                        writer.writerow(row)
                    
                    bans_fetched = len(bans)
                    total_bans_fetched += bans_fetched
                    bans_since_last_checkpoint += bans_fetched
                    
                    page_info = result['pageInfo']
                    has_next_page = page_info['hasNextPage']
                    current_after = page_info['endCursor']
                    
                    print(f"Fetched {bans_fetched} bans in this batch. Total: {total_bans_fetched}")
                    
                    if bans_since_last_checkpoint >= 10000:
                        save_checkpoint(current_after)
                        print(f"Checkpoint saved at {total_bans_fetched} bans.")
                        bans_since_last_checkpoint = 0
                else:
                    print("No result returned from fetch_bans. Exiting.")
                    break
                
                sleep(0.2)
            
            # Save final checkpoint
            save_checkpoint(current_after)
            print(f"Final checkpoint saved. Total bans fetched: {total_bans_fetched}")
        except Exception as e:
            print(f"An error occurred: {e}")
            print("Saving progress before exiting...")
            save_checkpoint(current_after)
            raise

def count_bans():
    try:
        with open(CSV_FILE, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            total_bans = sum(1 for _ in reader)

        print(f"Total bans retrieved so far: {total_bans}")
    except FileNotFoundError:
        print("No data found. The CSV file does not exist.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch and process CBL ban data")
    parser.add_argument('--count', action='store_true', help="Count the bans retrieved so far")
    args = parser.parse_args()

    if args.count:
        count_bans()
    else:
        try:
            print("Fetching all bans from CBL...")
            fetch_all_bans()
            print("Data saved to cbl_bans.csv")
        except KeyboardInterrupt:
            print("\nScript interrupted by user. Progress has been saved.")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            print("Progress has been saved.")
        finally:
            print("Exiting script.")