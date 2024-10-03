import os

# Update paths
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
CHECKPOINT_FILE = os.path.join(DATA_DIR, 'checkpoint.json')
CSV_FILE = os.path.join(DATA_DIR, 'cbl_data.csv')

# GraphQL endpoint
GRAPHQL_ENDPOINT = 'https://communitybanlist.com/graphql'
USERS_PER_PAGE = 500  # Increased from 100 to 500

# GraphQL query to fetch all steam users
query = """
query GetAllSteamUsers($after: String, $first: Int) {
  steamUsers(first: $first, after: $after) {
    pageInfo {
      hasNextPage
      endCursor
    }
    edges {
      node {
        id
        name
        avatarFull
        reputationPoints
        riskRating
        reputationRank
        activeBans: bans(orderBy: "created", orderDirection: DESC, expired: false) {
          edges {
            node {
              id
              created
              expires
              reason
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
        expiredBans: bans(orderBy: "created", orderDirection: DESC, expired: true) {
          edges {
            node {
              id
              created
              expires
              reason
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
    }
  }
}
"""

def fetch_steam_users(after):
    variables = {"after": after, "first": USERS_PER_PAGE}
    response = requests.post(GRAPHQL_ENDPOINT, json={'query': query, 'variables': variables})
    if response.status_code == 200:
        return response.json()['data']['steamUsers']
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
        return data.get("after"), []
    except FileNotFoundError:
        return None, []
    except json.JSONDecodeError:
        print("Error: checkpoint.json is corrupted. Attempting to repair...")
        # Try to find the last valid "after" value
        import re
        match = re.search(r'"after":\s*"([^"]+)"', content)
        if match:
            after = match.group(1)
            print(f"Recovered 'after' value: {after}")
            return after, []
        else:
            print("Unable to recover 'after' value. Starting from the beginning.")
            return None, []

def signal_handler(sig, frame):
    print("\nInterrupt received. Saving progress and exiting...")
    save_checkpoint(current_after)
    sys.exit(0)

def fetch_all_steam_users():
    global current_after
    current_after, _ = load_checkpoint()
    has_next_page = True
    
    signal.signal(signal.SIGINT, signal_handler)
    
    csv_file = 'cbl_data.csv'
    file_exists = os.path.isfile(csv_file)
    
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(CSV_FILE, 'a', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=[
            'id', 'name', 'avatarFull', 'reputationPoints', 'riskRating', 'reputationRank',
            'activeBans', 'expiredBans'
        ])
        if not file_exists:
            writer.writeheader()
        
        try:
            total_users_fetched = 0
            users_since_last_checkpoint = 0
            while has_next_page:
                result = fetch_steam_users(current_after)
                if result:
                    users = result['edges']
                    if not users:
                        print("No more users to fetch. Exiting.")
                        break
                    
                    for user_edge in users:
                        user = user_edge['node']
                        row = {
                            'id': user['id'],
                            'name': user['name'],
                            'avatarFull': user['avatarFull'],
                            'reputationPoints': user['reputationPoints'],
                            'riskRating': user['riskRating'],
                            'reputationRank': user['reputationRank'],
                            'activeBans': len(user['activeBans']['edges']),
                            'expiredBans': len(user['expiredBans']['edges'])
                        }
                        writer.writerow(row)
                    
                    users_fetched = len(users)
                    total_users_fetched += users_fetched
                    users_since_last_checkpoint += users_fetched
                    
                    page_info = result['pageInfo']
                    has_next_page = page_info['hasNextPage']
                    current_after = page_info['endCursor']
                    
                    print(f"Fetched {users_fetched} users in this batch. Total: {total_users_fetched}")
                    
                    if users_since_last_checkpoint >= 10000:
                        save_checkpoint(current_after)
                        print(f"Checkpoint saved at {total_users_fetched} users.")
                        users_since_last_checkpoint = 0
                else:
                    print("No result returned from fetch_steam_users. Exiting.")
                    break
                
                sleep(0.2)
            
            # Save final checkpoint
            save_checkpoint(current_after)
            print(f"Final checkpoint saved. Total users fetched: {total_users_fetched}")
        except Exception as e:
            print(f"An error occurred: {e}")
            print("Saving progress before exiting...")
            save_checkpoint(current_after)
            raise

def save_to_csv(data, filename):
    full_path = os.path.join(DATA_DIR, filename)
    with open(full_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=[
            'id', 'name', 'avatarFull', 'reputationPoints', 'riskRating', 'reputationRank',
            'activeBans', 'expiredBans'
        ])
        writer.writeheader()
        for user in data:
            row = {
                'id': user['id'],
                'name': user['name'],
                'avatarFull': user['avatarFull'],
                'reputationPoints': user['reputationPoints'],
                'riskRating': user['riskRating'],
                'reputationRank': user['reputationRank'],
                'activeBans': len(user['activeBans']['edges']),
                'expiredBans': len(user['expiredBans']['edges'])
            }
            writer.writerow(row)

def count_data():
    try:
        with open(CSV_FILE, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            total_users = 0
            total_active_bans = 0
            total_expired_bans = 0
            for row in reader:
                total_users += 1
                total_active_bans += int(row['activeBans'])
                total_expired_bans += int(row['expiredBans'])

        print(f"Total users retrieved so far: {total_users}")
        print(f"Total active bans: {total_active_bans}")
        print(f"Total expired bans: {total_expired_bans}")
        print(f"Total bans: {total_active_bans + total_expired_bans}")
    except FileNotFoundError:
        print("No data found. The CSV file does not exist.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch and process CBL data")
    parser.add_argument('--count', action='store_true', help="Count the data retrieved so far")
    args = parser.parse_args()

    if args.count:
        count_data()
    else:
        try:
            print("Fetching all Steam users from CBL...")
            fetch_all_steam_users()
            print("Data saved to cbl_data.csv")
        except KeyboardInterrupt:
            print("\nScript interrupted by user. Progress has been saved.")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            print("Progress has been saved.")
        finally:
            print("Exiting script.")