# src/producers/sandbox_track_producer.py
"""
Sandbox track producer that simulates real-time data flow in accelerated time.
- Scrapes Spotify charts in real-time
- Fetches missing song features from "fake API" (ORIGINAL database)
- Simulates API delays, errors, and retry logic
- Enforces time windows for each simulation day
- Streams features to Kafka one-by-one (NO BATCHING)
"""

from supabase import create_client, Client
from datetime import datetime, timedelta, date
import time
from kafka import KafkaProducer
import json
import os
import yaml
import requests
import random
from dotenv import load_dotenv

load_dotenv()


def load_config() -> dict:
    """Load configuration from config.yaml"""
    config_path = os.environ.get("CONFIG_PATH", "config.yaml")
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


class SandboxTrackProducer:
    """
    Sandbox track producer with accelerated time simulation.
    Fetches charts, queries ORIGINAL database for features, simulates API behavior.
    """
    
    def __init__(self):
        self.config = load_config()
        
        # Sandbox configuration
        self.sandbox_config = self.config.get('sandbox', {})
        if not self.sandbox_config.get('enabled', False):
            raise ValueError("Sandbox mode is not enabled in config.yaml")
        
        # Simulation parameters
        self.start_date = datetime.strptime(self.sandbox_config['start_date'], "%Y-%m-%d").date()
        self.interval_seconds = self.sandbox_config['simulation_interval_seconds']
        self.start_timestamp = time.time()
        
        # Fake API simulation parameters
        self.api_delay_range = self.sandbox_config['api_delay_range_seconds']
        self.api_error_probability = self.sandbox_config['api_error_probability']
        
        # Two Supabase clients
        # ORIGINAL database - for reading features (fake API)
        original_url = os.getenv("SUPABASE_URL")
        original_key = os.getenv("SUPABASE_KEY")
        self.original_supabase: Client = create_client(original_url, original_key)
        
        # SANDBOX database - for checking which songs we already have
        sandbox_url = os.getenv("SANDBOX_SUPABASE_URL")
        sandbox_key = os.getenv("SANDBOX_SUPABASE_KEY")
        self.sandbox_supabase: Client = create_client(sandbox_url, sandbox_key)
        
        # Table names
        self.track_features_table = self.config['supabase']['track_features_table']
        self.chart_type = self.config['spotify']['chart_type']
        
        # Kafka producer
        self.producer = KafkaProducer(
            bootstrap_servers=self.config['kafka']['bootstrap_servers'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.charts_topic = self.config['kafka']['charts_topic']
        self.features_topic = self.config['kafka']['features_topic']
        
        # Spotify API for chart scraping
        self.bearer_token = os.getenv("SPOTIFY_BEARER_TOKEN")
        if not self.bearer_token:
            raise ValueError("SPOTIFY_BEARER_TOKEN not found in environment variables")
        
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        })
        self.session.headers.update({"authorization": f"Bearer {self.bearer_token}"})
        
        # Logging setup
        os.makedirs('logs', exist_ok=True)
        
        print("=" * 60)
        print("SANDBOX TRACK PRODUCER INITIALIZED")
        print("=" * 60)
        print(f"Start date: {self.start_date}")
        print(f"Simulation interval: {self.interval_seconds}s per day")
        print(f"Chart type: {self.chart_type}")
        print(f"API delay range: {self.api_delay_range}s")
        print(f"API error probability: {self.api_error_probability}")
        print("=" * 60)
    
    # ==================== TIME SYNCHRONIZATION ====================
    
    def calculate_current_simulation_day(self) -> tuple[date, int]:
        """
        Calculate which simulation day we're currently in based on elapsed time.
        Independent calculation - no shared state required.
        Returns (current_date, days_elapsed)
        """
        elapsed_seconds = time.time() - self.start_timestamp
        days_elapsed = int(elapsed_seconds // self.interval_seconds)
        current_date = self.start_date + timedelta(days=days_elapsed)
        return current_date, days_elapsed
    
    # ==================== DATABASE QUERIES ====================
    
    def get_existing_song_ids(self) -> set[str]:
        """Query SANDBOX database for songs we've already processed"""
        try:
            response = (
                self.sandbox_supabase.table(self.track_features_table)
                .select("track_id")
                .execute()
            )
            return set(row["track_id"] for row in response.data)
        except Exception as e:
            print(f"Error fetching existing song IDs from SANDBOX: {e}")
            return set()
        
    def get_existing_features(self, track_ids: list[str]) -> dict[str, dict]:
        """Fetch existing features from SANDBOX database for given track IDs"""
        try:
            response = (
                self.sandbox_supabase.table(self.track_features_table)
                .select("*")
                .in_("track_id", track_ids)
                .execute()
            )
            # Return as dict with track_id as key for easy lookup
            return {row["track_id"]: row for row in response.data}
        except Exception as e:
            print(f"Error fetching existing features from SANDBOX: {e}")
            return {}
    
    # ==================== SPOTIFY CHARTS (REUSED FROM ORIGINAL) ====================
    
    def fetch_daily_charts(self, date_str: str) -> list[dict] | None:
        """Fetch chart data from ORIGINAL database for a specific date"""
        try:
            response = (
                self.original_supabase.table("spotify_regional_charts")
                .select("*")
                .eq("chart_date", date_str)
                .execute()
            )
            
            if not response.data:
                print(f"    {date_str}: No chart data found in database")
                return None
            
            tracks = []
            for row in response.data:
                track_uri = row.get("track_uri", "")
                track_id = track_uri.split(":")[-1] if track_uri else None
                
                tracks.append({
                    "rank": row.get("rank"),
                    "previous_rank": row.get("previous_rank"),
                    "peak_rank": row.get("peak_rank"),
                    "peak_date": row.get("peak_date"),
                    "appear_on_chart": row.get("appear_on_chart"),
                    "track_name": row.get("track_name"),
                    "track_id": track_id,
                    "track_uri": track_uri,
                    "artists": row.get("artists"),
                    "artist_uris": row.get("artist_uris"),
                    "release_date": row.get("release_date"),
                    "chart_date": date_str,
                    "chart_type": row.get("chart_type"),
                })
            
            print(f"    {date_str}: fetched {len(tracks)} tracks from database")
            return tracks
        
        except Exception as e:
            print(f"    {date_str}: Database error - {e}")
            return None
    
    # ==================== FAKE API (ORIGINAL DATABASE) ====================
    
    def fetch_features_from_fake_api(self, track_id: str, attempt: int) -> tuple[dict | None, bool, str]:
        """
        Simulate API call by querying ORIGINAL database.
        Returns (features_dict, success, reason)
        """
        # First, check if track exists in ORIGINAL database
        try:
            response = self.original_supabase.table(self.track_features_table)\
                .select('*')\
                .eq('track_id', track_id)\
                .execute()
            if not response.data:
                # Track doesn't exist - skip immediately (no retry)
                return None, False, "not_found"
            
            track_features = response.data[0]
            
        except Exception as e:
            return None, False, f"query_error: {e}"
        
        # Simulate API delay
        delay = random.uniform(self.api_delay_range[0], self.api_delay_range[1])
        time.sleep(delay)
        
        # Simulate error based on attempt number
        if attempt == 1:
            success_rate = 1 - self.api_error_probability
        elif attempt == 2:
            success_rate = 0.4  # 60% fail
        elif attempt == 3:
            success_rate = 0.2  # 80% fail
        else:
            return None, False, "max_retries"
        
        if random.random() > success_rate:
            # Simulated failure
            return None, False, "simulated_error"
        
        # Success - return the features
        return track_features, True, "success"
    
    def fetch_features_with_retry(self, track: dict, date_str: str) -> dict | None:
        """
        Fetch features with retry logic.
        Logs failures after 3 attempts.
        """
        track_id = track.get('track_id')

        for attempt in range(1, 4):
            features, success, reason = self.fetch_features_from_fake_api(track_id, attempt)
            
            if success:
                return features
            
            if reason == "not_found":
                # Track doesn't exist in ORIGINAL database - skip immediately
                return None
            
            # Wait before retry (if not last attempt)
            if attempt < 3:
                time.sleep(3)
        
        # Failed after 3 attempts - log it
        self.log_failed_feature(track, date_str, "max_retries")
        return None
    
    # ==================== KAFKA PUBLISHING ====================
    
    def publish_charts(self, charts_data: list[dict], date_str: str):
        """Publish chart data to Kafka"""
        message = {
            "chart_date": date_str,
            "chart_type": self.chart_type,
            "tracks": charts_data,
            "timestamp": datetime.now().isoformat(),
            "sandbox": True
        }
        self.producer.send(self.charts_topic, value=message)
        self.producer.flush()
        print(f"    -> Published {len(charts_data)} tracks to Kafka")
    
    def publish_single_feature(self, feature: dict, chart_date: str):
        """Publish ONE feature to Kafka immediately (NO BATCHING)"""
        message = {
            "track_id": feature.get('track_id'),
            "track_name": feature.get('track_name'),
            "chart_date": chart_date,
            "artists": feature.get('artists'),
            "track_uri": feature.get('track_uri'),
            "release_date": feature.get('release_date'),
            "tempo": feature.get('tempo'),
            "duration": feature.get('duration'),
            "energy": feature.get('energy'),
            "danceability": feature.get('danceability'),
            "happiness": feature.get('happiness'),
            "acousticness": feature.get('acousticness'),
            "instrumentalness": feature.get('instrumentalness'),
            "liveness": feature.get('liveness'),
            "speechiness": feature.get('speechiness'),
            "loudness_db": feature.get('loudness_db'),
            "popularity": feature.get('popularity'),
            "key": feature.get('key'),
            "mode": feature.get('mode'),
            "camelot": feature.get('camelot'),
            "timestamp": datetime.now().isoformat(),
            "sandbox": True
        }
        self.producer.send(self.features_topic, value=message)
        self.producer.flush()
    
    # ==================== LOGGING ====================
    
    def log_failed_feature(self, track: dict, date_str: str, reason: str):
        """Log failed feature to logs/failed_features_YYYYMMDD.log"""
        log_file = f"logs/failed_features_{date_str.replace('-', '')}.log"
        
        with open(log_file, 'a') as f:
            f.write(f"{date_str},{track.get('track_id')},{track.get('track_name')},{track.get('artists')},3,{reason}\n")
    
    def log_timeout_feature(self, track: dict, date_str: str):
        """Log timeout feature to logs/timeout_features_YYYYMMDD.log"""
        log_file = f"logs/timeout_features_{date_str.replace('-', '')}.log"
        
        with open(log_file, 'a') as f:
            f.write(f"{date_str},{track.get('track_id')},{track.get('track_name')},{track.get('artists')},time_exceeded\n")
    
    # ==================== MAIN PROCESSING ====================
    
    def process_simulation_day(self, date_obj: date, day_end_timestamp: float):
        """
        Process one simulation day with time window enforcement.
        Returns early if time window expires.
        """
        date_str = date_obj.strftime("%Y-%m-%d")
        
        # 1. Fetch charts for this day
        chart_data = self.fetch_daily_charts(date_str)
        if not chart_data:
            print(f"    No chart data for {date_str}")
            return
        
        # 2. Publish charts immediately to Kafka
        self.publish_charts(chart_data, date_str)
        
        # 3. Get existing song IDs from SANDBOX database
        existing_song_ids = self.get_existing_song_ids()

        # 4. Separate tracks into existing vs new
        tracks_with_existing_features = [
            track for track in chart_data
            if track.get("track_id") and track["track_id"] in existing_song_ids
        ]

        tracks_needing_features = [
            track for track in chart_data
            if track.get("track_id") and track["track_id"] not in existing_song_ids
        ]

        print(f"    Songs with existing features: {len(tracks_with_existing_features)}")
        print(f"    Songs needing features: {len(tracks_needing_features)}")

        # 5. Republish existing features from SANDBOX database
        if tracks_with_existing_features:
            existing_track_ids = [t["track_id"] for t in tracks_with_existing_features]
            existing_features = self.get_existing_features(existing_track_ids)
            
            for track in tracks_with_existing_features:
                track_id = track["track_id"]
                if track_id in existing_features:
                    self.publish_single_feature(existing_features[track_id], date_str)
                    print(f"    [REPUBLISHED] {track['track_name'][:40]}")

        # 6. Fetch and stream NEW features ONE BY ONE
        for i, track in enumerate(tracks_needing_features):
            # Check if time window has closed
            if time.time() >= day_end_timestamp:
                # Time's up! Log remaining tracks and move to next day
                print(f"    TIME WINDOW CLOSED - logging {len(tracks_needing_features) - i} remaining tracks")
                for remaining_track in tracks_needing_features[i:]:
                    self.log_timeout_feature(remaining_track, date_str)
                break
            
            display_name = track["track_name"][:40]
            print(f"    [{i+1}/{len(tracks_needing_features)}] {display_name}...", end=" ")
            
            # Fetch with retry logic and error simulation
            features = self.fetch_features_with_retry(track, date_str)
            
            if features:
                # Stream immediately to Kafka (NO BATCHING)
                self.publish_single_feature(features, date_str)
                existing_song_ids.add(track["track_id"])
                print("[OK]")
            else:
                print("[FAILED]")
    
    def run(self):
        """Main loop - process one day per simulation interval"""
        print("\nStarting sandbox simulation...\n")
        
        last_processed_day = -1
        
        while True:
            try:
                # Calculate current simulation day (independent, deterministic)
                current_date, days_elapsed = self.calculate_current_simulation_day()
                
                # Only process if we've moved to a new day
                if days_elapsed != last_processed_day:
                    print(f"\n{'='*60}")
                    print(f"DAY {days_elapsed}: {current_date}")
                    print(f"{'='*60}")
                    
                    # Calculate when this day's time window closes
                    day_end_timestamp = self.start_timestamp + ((days_elapsed + 1) * self.interval_seconds)
                    
                    # Process this simulation day
                    self.process_simulation_day(current_date, day_end_timestamp)
                    
                    last_processed_day = days_elapsed
                
                # Check every second to catch day transitions quickly
                time.sleep(1)
                
            except KeyboardInterrupt:
                print("\n\nShutting down...")
                self.producer.flush()
                break
            except Exception as e:
                print(f"Error in main loop: {e}")
                import traceback
                traceback.print_exc()
                time.sleep(5)


if __name__ == "__main__":
    producer = SandboxTrackProducer()
    producer.run()