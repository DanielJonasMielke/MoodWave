import requests
import pandas as pd
from datetime import datetime, timedelta
import time
import os
from dotenv import load_dotenv

load_dotenv()

class SpotifyChartsScraper:
    def __init__(self, bearer_token=None):
        self.bearer_token = bearer_token
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })

        if bearer_token:
            self.session.headers.update({
                'authorization': f'Bearer {bearer_token}'
            })

    def get_daily_chart(self, chart_type="viral-de-daily", date=None):

        if date is None:
            date = datetime.now().strftime('%Y-%m-%d')
        elif isinstance(date, datetime):
            date = date.strftime('%Y-%m-%d')

        # Build API URL
        url = f'https://charts-spotify-com-service.spotify.com/auth/v0/charts/{chart_type}/{date}'

        try:
            response = self.session.get(url)
            response.raise_for_status()
            data = response.json()

            # Parse entries
            tracks = []
            for entry in data.get('entries', []):
                meta = entry.get('trackMetadata', {})
                chart_data = entry.get('chartEntryData', {})

                tracks.append({
                    'rank': chart_data.get('currentRank'),
                    'previous_rank': chart_data.get('previousRank'),
                    'peak_rank': chart_data.get('peakRank'),
                    'peak_date': chart_data.get('peakDate'),
                    'appear_on_chart': chart_data.get('appearOnChartDayCount'),
                    'track_name': meta.get('trackName'),
                    'track_uri': meta.get('trackUri'),
                    'artists': ', '.join([a.get('name', '') for a in meta.get('artists', [])]),
                    'artist_uris': ', '.join([a.get('spotifyUri', '') for a in meta.get('artists', [])]),
                    'release_date': meta.get('releaseDate'),
                    'chart_date': date,
                    'chart_type': chart_type
                })

            return pd.DataFrame(tracks)

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 401:
                print("❌ Authentication failed. Your bearer token may be expired.")
            elif e.response.status_code == 404:
                print(f"❌ Chart not found: {chart_type} for date {date}")
            else:
                print(f"❌ HTTP Error: {e}")
            return None
        except Exception as e:
            print(f"❌ Error: {e}")
            return None

    def scrape_date_range(self, chart_type, start_date, end_date, delay=2, max_retries=3,
                          save_individual=True, output_dir="spotify_charts"):

        dates = self.get_date_range(start_date, end_date)
        all_data = []
        failed_dates = []

        # Create output directory if saving individual files
        if save_individual:
            os.makedirs(output_dir, exist_ok=True)

        total_dates = len(dates)

        for idx, date in enumerate(dates, 1):
            print(f"📥 [{idx}/{total_dates}] Downloading {chart_type} for {date}...")

            # Retry logic with exponential backoff
            retry_count = 0
            success = False

            while retry_count < max_retries and not success:
                df = self.get_daily_chart(chart_type, date)

                if df is not None and not df.empty:
                    all_data.append(df)
                    print(f"   ✓ Got {len(df)} tracks")

                    # Save individual file
                    if save_individual:
                        filename = f"{output_dir}/{chart_type}-{date}.csv"
                        df.to_csv(filename, index=False)
                        print(f"   💾 Saved to {filename}")

                    success = True
                else:
                    retry_count += 1
                    if retry_count < max_retries:
                        wait_time = delay * (2 ** retry_count)  # Exponential backoff
                        print(f"   ⚠️  Retry {retry_count}/{max_retries} after {wait_time}s...")
                        time.sleep(wait_time)
                    else:
                        print(f"   ✗ Failed after {max_retries} attempts")
                        failed_dates.append(date)

            # Rate limiting delay between successful requests
            if success and idx < total_dates:
                time.sleep(delay)

        # Summary
        print("\n" + "=" * 60)
        print(f"✓ Successfully scraped: {len(all_data)} days")
        if save_individual:
            print(f"📁 Individual files saved to: {output_dir}/")
        if failed_dates:
            print(f"✗ Failed dates ({len(failed_dates)}): {', '.join(failed_dates)}")
        print("=" * 60)

        if all_data:
            return pd.concat(all_data, ignore_index=True)
        return None

    def get_date_range(self, start_date, end_date):
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, '%Y-%m-%d')
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, '%Y-%m-%d')

        dates = []
        current = start_date
        while current <= end_date:
            dates.append(current.strftime('%Y-%m-%d'))
            current += timedelta(days=1)
        return dates


if __name__ == "__main__":
    # ===== USER CONFIGURATION =====
    # Set your dates here (format: YYYY-MM-DD)
    START_DATE = os.getenv('START_DATE', "2025-02-11")  # Change this to your start date
    END_DATE = os.getenv('END_DATE', "2025-12-31")  # Change this to your end date

    # Optional: Change chart type if needed
    CHART_TYPE = os.getenv('CHART_TYPE', 'viral-de-daily')  # e.g., "regional-de-daily", "viral-global-daily"
    # ==============================

    # Get bearer token from environment variable
    # Set it with: export SPOTIFY_BEARER_TOKEN="your_token_here"
    BEARER_TOKEN = os.getenv('SPOTIFY_BEARER_TOKEN')

    if not BEARER_TOKEN:
        print("Error: SPOTIFY_BEARER_TOKEN not found in environment variables")
        exit(1)

    print(f"📅 Date range: {START_DATE} to {END_DATE}")
    print(f"📊 Chart type: {CHART_TYPE}")
    print()

    scraper = SpotifyChartsScraper(bearer_token=BEARER_TOKEN)

    df_range = scraper.scrape_date_range(
        chart_type=CHART_TYPE,
        start_date=START_DATE,
        end_date=END_DATE,
        delay=8,
        max_retries=3,
        save_individual=True,
        output_dir="spotify_charts/spotify_daily_viral_charts"
    )