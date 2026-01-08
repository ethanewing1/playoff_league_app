import pandas as pd
import requests
from datetime import datetime
import os


def download_nflverse_data(season=2024, season_type='POST'):
    """
    Download player stats data from nflverse GitHub releases.
    
    Args:
        season: The NFL season year (default: 2024)
        season_type: 'REG' for regular season or 'POST' for postseason (default: 'POST')
    
    Returns:
        DataFrame with player stats
    """
    file_name = f"stats_player_post_{season}.csv"
    
    # Try to download from nflverse first
    base_url = "https://github.com/nflverse/nflverse-data/releases/download/stats_player"
    url = f"{base_url}/stats_player_{season_type.lower()}_{season}.csv"
    
    print(f"Attempting to download data from: {url}")
    
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()  # Raise an error for bad status codes
        
        # Save the file
        with open(file_name, 'wb') as f:
            f.write(response.content)
        
        # Read the CSV file
        df = pd.read_csv(file_name)
        print(f"Successfully downloaded {len(df)} rows of data")
        return df
    
    except (requests.exceptions.RequestException, Exception) as e:
        print(f"Download failed: {e}")
        print(f"Attempting to use local file: {file_name}")
        
        # Fall back to local file
        if os.path.exists(file_name):
            print(f"Using local file: {file_name}")
            df = pd.read_csv(file_name)
            print(f"Successfully loaded {len(df)} rows of data from local file")
            return df
        else:
            print(f"Error: Local file '{file_name}' not found.")
            raise FileNotFoundError(f"Could not download data and local file '{file_name}' does not exist.")


def filter_and_prepare_data(df):
    """
    Filter the dataframe to only include needed columns and position groups.
    
    Args:
        df: Raw player stats DataFrame
    
    Returns:
        Filtered DataFrame with needed columns
    """
    needed_cols = [
        'player_name', 'player_display_name', 'position', 'position_group', 'recent_team',
        'passing_yards', 'passing_tds', 'passing_interceptions', 'sacks_suffered',
        'sack_fumbles_lost', 'passing_2pt_conversions',
        'rushing_yards', 'rushing_tds', 'rushing_fumbles_lost', 'rushing_2pt_conversions',
        'receptions', 'receiving_yards', 'receiving_tds', 'receiving_fumbles_lost',
        'receiving_2pt_conversions', 'special_teams_tds',
        'def_tackles_solo', 'def_tackle_assists', 'def_tackles_for_loss', 'def_fumbles_forced',
        'def_sacks', 'def_interceptions', 'def_pass_defended', 'def_tds', 'def_fumbles',
        'def_safeties', 'fumble_recovery_opp', 'fumble_recovery_tds',
        'punt_return_yards', 'kickoff_return_yards',
        'fg_made', 'fg_att', 'fg_made_distance', 'pat_made', 'pat_att'
    ]
    
    # Filter columns (only keep those that exist in the dataframe)
    available_cols = [col for col in needed_cols if col in df.columns]
    df_filtered = df[available_cols].copy()
    
    # Filter position groups
    position_groups = ['QB', 'RB', 'WR', 'TE', 'DL', 'LB', 'DB', 'ST']
    df_filtered = df_filtered[df_filtered['position_group'].isin(position_groups)]
    
    # Fill NaN values with 0 for numeric columns
    numeric_columns = df_filtered.select_dtypes(include=['float64', 'int64']).columns
    df_filtered[numeric_columns] = df_filtered[numeric_columns].fillna(0)
    
    print(f"Filtered to {len(df_filtered)} players in position groups: {', '.join(position_groups)}")
    
    return df_filtered


def calculate_off_points(row):
    """Calculate fantasy points for offensive players (QB, RB, WR, TE)."""
    total = 0
    
    # Passing
    total += row['passing_yards'] / 25
    total += row['passing_tds'] * 4
    total -= row['passing_interceptions'] * 2
    total -= row['sacks_suffered'] * 0.5
    total -= row['sack_fumbles_lost'] * 2
    total += row['passing_2pt_conversions'] * 2
    
    # Rushing
    total += row['rushing_yards'] / 10
    total += row['rushing_tds'] * 6
    total -= row['rushing_fumbles_lost'] * 2
    total += row['rushing_2pt_conversions'] * 2
    
    # Receiving
    total += row['receptions'] * 0.5
    total += row['receiving_yards'] / 10
    total += row['receiving_tds'] * 6
    total -= row['receiving_fumbles_lost'] * 2
    total += row['receiving_2pt_conversions'] * 2
    
    # Special Teams
    total += row['special_teams_tds'] * 15
    total += row['punt_return_yards'] / 20
    total += row['kickoff_return_yards'] / 20
    
    return total


def calculate_def_points(row):
    """Calculate fantasy points for defensive players (DL, LB, DB)."""
    total = 0
    
    total += row['def_tackles_solo'] * 1
    total += row['def_tackle_assists'] * 0.5
    total += row['def_tackles_for_loss'] * 2
    total += row['def_fumbles_forced'] * 5
    total += row['def_sacks'] * 4
    total += row['def_interceptions'] * 7
    total += row['def_pass_defended'] * 3
    total += row['def_tds'] * 6
    total += row['def_safeties'] * 2
    total += row['fumble_recovery_opp'] * 3
    total += row['fumble_recovery_tds'] * 6
    
    return total


def calculate_fantasy_points(row):
    """Calculate fantasy points based on position group."""
    if row['position_group'] in ['QB', 'RB', 'WR', 'TE']:
        return calculate_off_points(row)
    elif row['position_group'] in ['DL', 'LB', 'DB']:
        return calculate_def_points(row)
    else:
        return 0


def add_fantasy_points(df):
    """
    Add fantasy points column to the dataframe.
    
    Args:
        df: DataFrame with player stats
    
    Returns:
        DataFrame with added fantasy_points column
    """
    print("Calculating fantasy points...")
    df['fantasy_points'] = df.apply(calculate_fantasy_points, axis=1)
    print("Fantasy points calculated!")
    
    return df


def connect_to_supabase():
    """
    Connect to Supabase database using PostgREST API.
    
    Returns:
        Tuple of (API URL, headers dict)
    """
    base_url = "https://cnfshlzatausdprfjjii.supabase.co"
    api_key = "sb_publishable_oyyGrWERz6v_9EZ024U6Gg_k9oT-UBd"
    
    # Supabase PostgREST endpoint
    rest_url = f"{base_url}/rest/v1"
    
    # Headers for API requests
    headers = {
        "apikey": api_key,
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal"
    }
    
    print("Connected to Supabase")
    
    return rest_url, headers


def save_to_supabase(df, rest_url, headers, season=2024):
    """
    Save the dataframe to Supabase database using REST API.
    
    Args:
        df: DataFrame with player stats and fantasy points
        rest_url: Supabase REST API URL
        headers: Request headers with API key
        season: Season year for table name
    """
    table_name = f"player_stats_{season}"
    
    # Add timestamp
    df['last_updated'] = datetime.now().isoformat()
    
    # Convert DataFrame to list of dictionaries
    records = df.to_dict('records')
    
    # Clear existing data (optional - remove if you want to keep historical data)
    try:
        delete_url = f"{rest_url}/{table_name}?id=gt.0"
        response = requests.delete(delete_url, headers=headers)
        if response.status_code in [200, 204]:
            print("Cleared existing records")
        else:
            print(f"Note: Could not clear existing data: {response.text}")
    except Exception as e:
        print(f"Note: Could not clear existing data (table might be empty): {e}")
    
    # Insert data in batches
    batch_size = 500  # Smaller batches for better reliability
    total_inserted = 0
    
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        try:
            insert_url = f"{rest_url}/{table_name}"
            response = requests.post(insert_url, json=batch, headers=headers)
            
            if response.status_code in [200, 201]:
                total_inserted += len(batch)
                print(f"Inserted batch {i//batch_size + 1}: {len(batch)} records")
            else:
                print(f"Error inserting batch: {response.status_code} - {response.text}")
                raise Exception(f"Failed to insert batch: {response.text}")
        except Exception as e:
            print(f"Error inserting batch: {e}")
            raise
    
def display_top_players(rest_url, headers, season=2024, limit=20):
    """
    Display top players by fantasy points from Supabase.
    
    Args:
        rest_url: Supabase REST API URL
        headers: Request headers with API key
        season: Season year for table name
        limit: Number of top players to display
    """
    table_name = f"player_stats_{season}"
    
    try:
        query_url = f"{rest_url}/{table_name}?select=player_display_name,position_group,recent_team,fantasy_points&order=fantasy_points.desc&limit={limit}"
        response = requests.get(query_url, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            df = pd.DataFrame(data)
            print(f"\nTop {limit} Players by Fantasy Points:")
            print("=" * 80)
            if not df.empty:
                print(df.to_string(index=False))
            else:
                print("No data found")
        else:
            print(f"Error fetching top players: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"Error fetching top players: {e}")


def main():
    """Main function to orchestrate the entire process."""
    try:
        # Step 1: Download data from nflverse
        print("\n" + "=" * 80)
        print("STEP 1: Downloading NFL Player Stats")
        print("=" * 80)
        season = 2024
        season_type = 'POST'
        df = download_nflverse_data(season=season, season_type=season_type)
        
        # Step 2: Filter and prepare data
        print("\n" + "=" * 80)
        print("STEP 2: Filtering and Preparing Data")
        print("=" * 80)
        df = filter_and_prepare_data(df)
        
        # Step 3: Calculate fantasy points
        print("\n" + "=" * 80)
        print("STEP 3: Calculating Fantasy Points")
        print("=" * 80)
        df = add_fantasy_points(df)
        
        # Step 4: Connect to Supabase
        print("\n" + "=" * 80)
        print("STEP 4: Connecting to Supabase")
        print("=" * 80)
        rest_url, headers = connect_to_supabase()
        
        # Step 5: Save to Supabase
        print("\n" + "=" * 80)
        print("STEP 5: Saving Data to Supabase")
        print("=" * 80)
        save_to_supabase(df, rest_url, headers, season=season)
        
        # Step 6: Display top players
        print("\n" + "=" * 80)
        print("STEP 6: Summary")
        print("=" * 80)
        display_top_players(rest_url, headers, season=season, limit=20)
        
        print("\n" + "=" * 80)
        print("Process completed successfully!")
        print("=" * 80)
        
    except Exception as e:
        print(f"\nError occurred: {e}")
        raise("\n" + "=" * 80)
        print("Process completed successfully!")
        print("=" * 80)
        
    except Exception as e:
        print(f"\nError occurred: {e}")
        raise


if __name__ == "__main__":
    main()
