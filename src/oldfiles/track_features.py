import pandas as pd
import http.client
import json
import os
import time
from pathlib import Path
from urllib.parse import quote
from datetime import datetime

# ================== CONFIGURATION ==================
# Get project root directory (calculate dynamically to work from any cwd)
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent

CHART_DIRECTORIES = [
    PROJECT_ROOT / "spotify_charts" / "spotify_daily_viral_charts"
]
# print(CHART_DIRECTORIES)  # Debug print removed
OUTPUT_FILE = PROJECT_ROOT / "data" / "all_songs_features.xlsx"
RAPIDAPI_KEY = "c3679947d7mshf2177dca5951699p114351jsnb55da7e51b8a"
RAPIDAPI_HOST = "track-analysis.p.rapidapi.com"

# Rate limiting configuration
DELAY_BETWEEN_REQUESTS = 1  # Delay after EVERY request (in seconds)
EXTRA_DELAY_AFTER_X_REQUESTS = 0.1  # Extra delay after every X requests
EXTRA_DELAY_TIME = 0.1  # How long to wait (in seconds) after X requests

# Checkpoint configuration
SAVE_INTERVAL = 1  # Save every 5 new songs

# Retry configuration
MAX_RETRIES = 10
RETRY_DELAY = 2  # Delay between retries


# ===================================================

def extract_track_id(track_uri):
    """
    Extract track ID from Spotify URI
    Example: spotify:track:6hw1Sy9wZ8UCxYGdpKrU6M -> 6hw1Sy9wZ8UCxYGdpKrU6M
    """
    if pd.isna(track_uri):
        return None
    return track_uri.split(":")[-1]


def fetch_track_features(track_name, artist_name, retries=MAX_RETRIES):
    """
    Fetch musical features from RapidAPI with retry logic
    """
    for attempt in range(retries):
        conn = None
        try:
            conn = http.client.HTTPSConnection(RAPIDAPI_HOST, timeout=15)

            headers = {
                'x-rapidapi-key': RAPIDAPI_KEY,
                'x-rapidapi-host': RAPIDAPI_HOST
            }

            # URL encode the parameters
            song_encoded = quote(track_name)
            artist_encoded = quote(artist_name)

            endpoint = f"/pktx/analysis?song={song_encoded}&artist={artist_encoded}"

            conn.request("GET", endpoint, headers=headers)
            res = conn.getresponse()
            data = res.read()

            # Check for successful response
            if res.status == 429:  # Rate limit hit
                print(f"    ⚠ Rate limit hit! Waiting {RETRY_DELAY * 2} seconds...")
                # print the error message from the response
                print(f"    Response: {data.decode('utf-8')}")
                time.sleep(RETRY_DELAY * 2)
                if attempt < retries - 1:
                    continue
                return None

            if res.status != 200:
                if attempt == retries - 1:
                    print(f"    ⚠ HTTP {res.status}")
                    return None
                time.sleep(RETRY_DELAY)
                continue

            # Parse JSON response
            features = json.loads(data.decode("utf-8"))
            return features

        except json.JSONDecodeError as e:
            if attempt == retries - 1:
                print(f"    ⚠ JSON decode error")
                return None
            time.sleep(RETRY_DELAY)

        except Exception as e:
            if attempt == retries - 1:
                print(f"    ⚠ Error: {str(e)[:50]}")
                return None
            time.sleep(RETRY_DELAY)

        finally:
            if conn:
                try:
                    conn.close()
                except:
                    pass

    return None


def load_existing_data():
    """Load existing data from Excel file if it exists"""
    if os.path.exists(OUTPUT_FILE):
        try:
            print(f"📂 Found existing file: {OUTPUT_FILE}")
            df = pd.read_excel(OUTPUT_FILE)
            print(f"✓ Loaded {len(df)} existing songs")
            return df
        except Exception as e:
            print(f"⚠ Error loading existing file: {str(e)}")
            print("Starting fresh...")
            return pd.DataFrame()
    else:
        print("📂 No existing file found. Starting fresh...")
        return pd.DataFrame()


def save_data(df, message="Saving checkpoint"):
    """Save data with backup"""
    try:
        # Create backup directory if it doesn't exist
        backup_dir = Path(OUTPUT_FILE).parent / "backups"
        backup_dir.mkdir(parents=True, exist_ok=True)

        # Create backup of existing file
        if os.path.exists(OUTPUT_FILE):
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_file = backup_dir / f"backup_{timestamp}.xlsx"

            import shutil
            shutil.copy2(OUTPUT_FILE, backup_file)

        # Save the new data
        df.to_excel(OUTPUT_FILE, index=False)
        print(f"  💾 {message}: {len(df)} songs saved")

    except Exception as e:
        print(f"  ✗ Error saving file: {str(e)}")


def main():
    print("=" * 70)
    print("🎵 Spotify Musical Features Fetcher (Sequential)")
    print("=" * 70)
    print("📁 Chart directories:")
    for directory in CHART_DIRECTORIES:
        print(f"   - {directory}")
    print(f"💾 Output file: {OUTPUT_FILE}")
    print(f"⏱️  Delay between requests: {DELAY_BETWEEN_REQUESTS}s")
    print(f"⏸️  Extra delay: {EXTRA_DELAY_TIME}s every {EXTRA_DELAY_AFTER_X_REQUESTS} requests")
    print(f"💾 Checkpoint: Every {SAVE_INTERVAL} songs")
    print("=" * 70 + "\n")

    # Load existing data
    all_songs_df = load_existing_data()

    # Get set of already processed track IDs
    if not all_songs_df.empty and 'track_id' in all_songs_df.columns:
        processed_tracks = set(all_songs_df['track_id'].dropna())
        print(f"✓ Already processed: {len(processed_tracks)} tracks\n")
    else:
        processed_tracks = set()

    # Get all chart files
    chart_files = []
    for directory in CHART_DIRECTORIES:
        dir_path = Path(directory)
        if dir_path.exists():
            # Daily charts
            chart_files.extend(dir_path.glob("viral-de-daily-*.csv"))
        else:
            print(f"⚠ Warning: Directory not found: {directory}")
    
    chart_files = sorted(list(set(chart_files)))

    if not chart_files:
        print(f"✗ No chart files found in specified directories")
        return

    print(f"📊 Found {len(chart_files)} chart files\n")

    # Process tracks sequentially
    new_songs_count = 0
    failed_count = 0
    total_requests = 0
    start_time = time.time()

    print("🚀 Starting sequential processing...\n")

    for file_idx, file_path in enumerate(chart_files, 1):
        try:
            df = pd.read_csv(file_path)
            print(f"📄 [{file_idx}/{len(chart_files)}] Processing: {file_path.name}")

            for idx, row in df.iterrows():
                track_id = extract_track_id(row['track_uri'])

                # Skip if already processed or invalid
                if track_id in processed_tracks or track_id is None:
                    continue

                # Mark as processed
                processed_tracks.add(track_id)

                # Get track info
                track_name = row['track_name']
                artists = row['artists']
                main_artist = artists.split(',')[0].strip() if ',' in str(artists) else artists

                # Truncate long track names for display
                display_name = track_name[:45] + "..." if len(track_name) > 45 else track_name
                display_artist = main_artist[:25] + "..." if len(main_artist) > 25 else main_artist

                print(f"  [{new_songs_count + failed_count + 1:4d}] 🎵 '{display_name}' by '{display_artist}'", end="")

                # Fetch features from API
                features = fetch_track_features(track_name, main_artist)
                total_requests += 1

                if features:
                    # Create song data
                    song_data = {
                        'track_id': track_id,
                        'track_name': track_name,
                        'artists': artists,
                        'track_uri': row['track_uri'],
                        'release_date': row['release_date'],
                        # Musical features
                        'tempo': features.get('tempo'),
                        'duration': features.get('duration'),
                        'energy': features.get('energy'),
                        'danceability': features.get('danceability'),
                        'happiness': features.get('happiness'),
                        'acousticness': features.get('acousticness'),
                        'instrumentalness': features.get('instrumentalness'),
                        'liveness': features.get('liveness'),
                        'speechiness': features.get('speechiness'),
                        'loudness': features.get('loudness'),
                        'popularity': features.get('popularity'),
                        'key': features.get('key'),
                        'mode': features.get('mode'),
                        'camelot': features.get('camelot')
                    }

                    # Add to dataframe
                    all_songs_df = pd.concat([all_songs_df, pd.DataFrame([song_data])], ignore_index=True)
                    new_songs_count += 1
                    print(" ✓")

                    # Save checkpoint every SAVE_INTERVAL songs
                    if new_songs_count % SAVE_INTERVAL == 0:
                        save_data(all_songs_df, f"Checkpoint: {new_songs_count} new songs")
                else:
                    failed_count += 1
                    print(" ✗")

                # Regular delay after every request
                time.sleep(DELAY_BETWEEN_REQUESTS)

                # Extra delay after every X requests
                if total_requests % EXTRA_DELAY_AFTER_X_REQUESTS == 0:
                    print(f"    ⏸️  Taking a {EXTRA_DELAY_TIME}s break after {total_requests} requests...")
                    time.sleep(EXTRA_DELAY_TIME)

            print(f"  ✓ Completed {file_path.name}\n")

        except Exception as e:
            print(f"  ✗ Error processing {file_path.name}: {str(e)}\n")
            continue

    elapsed_time = time.time() - start_time

    # Final save
    print(f"\n{'=' * 70}")
    print("💾 Processing complete! Saving final results...")
    print(f"{'=' * 70}")
    save_data(all_songs_df, "Final save completed")

    # Print summary
    print(f"\n{'=' * 70}")
    print("📊 SUMMARY")
    print(f"{'=' * 70}")
    print(f"⏱️  Total time: {elapsed_time / 60:.1f} minutes ({elapsed_time:.0f} seconds)")
    if total_requests > 0:
        print(f"📈 Average speed: {total_requests / elapsed_time:.2f} requests/second")
    print(f"🎯 Total requests made: {total_requests}")
    print(f"📦 Total songs in database: {len(all_songs_df)}")
    print(f"✓ New songs added: {new_songs_count}")
    print(f"✗ Failed requests: {failed_count}")

    if not all_songs_df.empty and new_songs_count > 0:
        print(f"\n🎵 Musical Features Statistics:")
        numeric_cols = ['tempo', 'danceability', 'energy', 'happiness', 'loudness']
        for col in numeric_cols:
            if col in all_songs_df.columns:
                mean_val = all_songs_df[col].mean()
                if pd.notna(mean_val):
                    if col == 'loudness':
                        print(f"  {col.capitalize():15} {mean_val:.2f} dB")
                    elif col == 'tempo':
                        print(f"  {col.capitalize():15} {mean_val:.2f} BPM")
                    else:
                        print(f"  {col.capitalize():15} {mean_val:.2f}")

    print(f"\n✅ All done! Results saved to: {OUTPUT_FILE}\n")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Script interrupted by user!")
        print("💾 Progress has been saved in checkpoints.")
        print("   Restart the script to continue from where you left off.\n")
    except Exception as e:
        print(f"\n\n❌ Fatal error: {str(e)}")
        import traceback

        traceback.print_exc()