# src/producers/track_producer.py
from supabase import create_client, Client
from datetime import datetime, timedelta
import time
from kafka import KafkaProducer
import json
from dotenv import load_dotenv
import os
import requests
import http.client
from urllib.parse import quote
import yaml
from concurrent.futures import ThreadPoolExecutor, as_completed

load_dotenv()


class TrackProducer:
    def __init__(self):
        url = os.getenv("SUPABASE_URL")
        key = os.getenv("SUPABASE_KEY")

        with open("config.yaml", "r") as f:
            config = yaml.safe_load(f)

        # Config values
        self.chart_type = config["spotify"]["chart_type"]
        # Determine charts table based on chart type
        if "viral" in self.chart_type:
            self.charts_table = config["supabase"]["viral_charts_table"]
        else:
            self.charts_table = config["supabase"]["regional_charts_table"]
        self.track_features_table = config["supabase"]["track_features_table"]
        self.start_date = config["producer"]["start_date"]
        self.time_interval = config["producer"]["fetch_interval_seconds"]

        # Spotify API
        self.bearer_token = os.getenv("SPOTIFY_BEARER_TOKEN")
        if not self.bearer_token:
            raise ValueError("SPOTIFY_BEARER_TOKEN not found in environment variables")

        # RapidAPI
        self.rapidapi_key = os.getenv("RAPIDAPI_KEY")
        if not self.rapidapi_key:
            raise ValueError("RAPIDAPI_KEY not found in environment variables")
        self.rapidapi_host = config["rapidapi"]["host"]

        # Supabase client
        self.supabase: Client = create_client(url, key)
        if self.supabase is None:
            raise ValueError("Failed to create Supabase client")
        print("Supabase client initialized")

        # Initialize Kafka producer
        self.producer = KafkaProducer(
            bootstrap_servers=config["kafka"]["bootstrap_servers"],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        self.charts_topic = config["kafka"]["charts_topic"]
        self.features_topic = config["kafka"]["features_topic"]

        # HTTP session for Spotify
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        })
        self.session.headers.update({"authorization": f"Bearer {self.bearer_token}"})

    # ==================== DATE UTILITIES ====================

    def generate_all_dates(self, start_date_str: str, end_date_str: str) -> list[str]:
        """Generate all dates between start and end (inclusive)"""
        start = datetime.strptime(start_date_str, "%Y-%m-%d")
        end = datetime.strptime(end_date_str, "%Y-%m-%d")

        dates = []
        current = start
        while current <= end:
            dates.append(current.strftime("%Y-%m-%d"))
            current += timedelta(days=1)
        return dates

    def get_existing_chart_dates(self) -> set[str]:
        """Fetch all dates we already have charts for in the database"""
        try:
            response = (
                self.supabase.table(self.charts_table)
                .select("chart_date")
                .execute()
            )
            return set(row["chart_date"] for row in response.data)
        except Exception as e:
            print(f"Error fetching existing chart dates: {e}")
            return set()

    def find_missing_chart_dates(self) -> list[str]:
        """Find dates where we don't have chart data yet"""
        today = datetime.now().strftime("%Y-%m-%d")
        all_dates = set(self.generate_all_dates(self.start_date, today))
        existing_dates = self.get_existing_chart_dates()

        missing = all_dates - existing_dates
        print(f"Missing chart dates: {len(missing)}")

        return sorted(missing)

    # ==================== SPOTIFY CHARTS ====================

    def fetch_daily_charts(self, date_str: str) -> list[dict] | None:
        """Fetch Spotify chart data for a specific date"""
        url = f"https://charts-spotify-com-service.spotify.com/auth/v0/charts/{self.chart_type}/{date_str}"

        try:
            response = self.session.get(url)
            response.raise_for_status()
            data = response.json()

            tracks = []
            for entry in data.get("entries", []):
                meta = entry.get("trackMetadata", {})
                chart_data = entry.get("chartEntryData", {})

                track_uri = meta.get("trackUri", "")
                track_id = track_uri.split(":")[-1] if track_uri else None

                tracks.append({
                    "rank": chart_data.get("currentRank"),
                    "previous_rank": chart_data.get("previousRank"),
                    "peak_rank": chart_data.get("peakRank"),
                    "peak_date": chart_data.get("peakDate"),
                    "appear_on_chart": chart_data.get("appearOnChartDayCount"),
                    "track_name": meta.get("trackName"),
                    "track_id": track_id,
                    "track_uri": track_uri,
                    "artists": ", ".join(
                        [a.get("name", "") for a in meta.get("artists", [])]
                    ),
                    "artist_uris": ", ".join(
                        [a.get("spotifyUri", "") for a in meta.get("artists", [])]
                    ),
                    "release_date": meta.get("releaseDate"),
                    "chart_date": date_str,
                    "chart_type": self.chart_type,
                })

            print(f"    {date_str}: fetched {len(tracks)} tracks")
            return tracks

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 401:
                print(f"    {date_str}: Auth failed - bearer token may be expired")
            elif e.response.status_code == 404:
                print(f"    {date_str}: Chart not found")
            else:
                print(f"    {date_str}: HTTP Error - {e}")
            return None
        except Exception as e:
            print(f"    {date_str}: Error - {e}")
            return None

    # ==================== SONG FEATURES ====================

    def get_existing_song_ids(self) -> set[str]:
        """Fetch all song IDs we already have features for"""
        try:
            response = (
                self.supabase.table(self.track_features_table)
                .select("track_id")
                .execute()
            )
            return set(row["track_id"] for row in response.data)
        except Exception as e:
            print(f"Error fetching existing song IDs: {e}")
            return set()

    def fetch_track_features(
        self, track_name: str, artist_name: str, max_retries: int = 3
    ) -> dict | None:
        """Fetch musical features from RapidAPI"""
        for attempt in range(max_retries):
            conn = None
            try:
                conn = http.client.HTTPSConnection(self.rapidapi_host, timeout=15)

                headers = {
                    "x-rapidapi-key": self.rapidapi_key,
                    "x-rapidapi-host": self.rapidapi_host,
                }

                song_encoded = quote(track_name)
                artist_encoded = quote(artist_name)
                endpoint = f"/pktx/analysis?song={song_encoded}&artist={artist_encoded}"

                conn.request("GET", endpoint, headers=headers)
                res = conn.getresponse()
                data = res.read()

                if res.status == 429:  # Rate limit
                    wait_time = 2 * (attempt + 1)
                    print(f"      Rate limit hit, waiting {wait_time}s...")
                    time.sleep(wait_time)
                    continue

                if res.status != 200:
                    if attempt == max_retries - 1:
                        print(f"      HTTP {res.status} for '{track_name[:30]}...'")
                        return None
                    time.sleep(1)
                    continue

                features = json.loads(data.decode("utf-8"))
                return features

            except json.JSONDecodeError:
                if attempt == max_retries - 1:
                    return None
                time.sleep(1)

            except Exception as e:
                if attempt == max_retries - 1:
                    print(f"      Error fetching features: {str(e)[:50]}")
                    return None
                time.sleep(1)

            finally:
                if conn:
                    try:
                        conn.close()
                    except:
                        pass

        return None

    def get_features_for_track(self, track: dict) -> dict | None:
        """Get features for a single track, returning enriched track data"""
        track_name = track.get("track_name", "")
        artists = track.get("artists", "")
        main_artist = artists.split(",")[0].strip() if "," in artists else artists

        features = self.fetch_track_features(track_name, main_artist)

        if not features:
            return None

        return {
            "track_id": track.get("track_id"),
            "track_name": track_name,
            "artists": artists,
            "track_uri": track.get("track_uri"),
            "release_date": track.get("release_date"),
            "tempo": features.get("tempo"),
            "duration": features.get("duration"),
            "energy": features.get("energy"),
            "danceability": features.get("danceability"),
            "happiness": features.get("happiness"),
            "acousticness": features.get("acousticness"),
            "instrumentalness": features.get("instrumentalness"),
            "liveness": features.get("liveness"),
            "speechiness": features.get("speechiness"),
            "loudness_db": features.get("loudness"),  # DB schema uses loudness_db
            "popularity": features.get("popularity"),
            "key": features.get("key"),
            "mode": features.get("mode"),
            "camelot": features.get("camelot"),
        }

    # ==================== KAFKA PUBLISHING ====================

    def publish_charts(self, charts_data: list[dict], date_str: str):
        """Publish chart data to Kafka"""
        message = {
            "chart_date": date_str,
            "chart_type": self.chart_type,
            "tracks": charts_data,
            "timestamp": datetime.now().isoformat(),
        }
        self.producer.send(self.charts_topic, value=message)
        print(f"-> Published {len(charts_data)} tracks to {self.charts_topic}")

    def publish_features(self, features_data: list[dict]):
        """Publish song features to Kafka"""
        if not features_data:
            return

        message = {
            "features": features_data,
            "count": len(features_data),
            "timestamp": datetime.now().isoformat(),
        }
        self.producer.send(self.features_topic, value=message)
        print(f"-> Published {len(features_data)} features to {self.features_topic}")

    # ==================== MAIN PROCESSING ====================

    def process_date(self, date_str: str, existing_song_ids: set[str]) -> tuple[list[dict], list[dict]]:
        """
        Process a single date:
        - Fetch chart data
        - Find songs needing features
        - Fetch missing features
        Returns (chart_data, new_features)
        """
        print(f"\nProcessing date: {date_str}")

        # Fetch chart for this date
        chart_data = self.fetch_daily_charts(date_str)
        if not chart_data:
            return [], []

        # Find tracks that need features
        tracks_needing_features = [
            track for track in chart_data
            if track.get("track_id") and track["track_id"] not in existing_song_ids
        ]

        print(f"    Songs needing features: {len(tracks_needing_features)}")

        # Fetch features for new songs
        new_features = []
        for i, track in enumerate(tracks_needing_features):
            display_name = track["track_name"][:40]
            print(f"    [{i+1}/{len(tracks_needing_features)}] Fetching: {display_name}...", end="")

            features = self.get_features_for_track(track)
            if features:
                new_features.append(features)
                existing_song_ids.add(track["track_id"])  # Mark as processed
                print(" [OK]")
            else:
                print(" [FAIL]")

            # Rate limiting
            time.sleep(0.5)

        return chart_data, new_features

    def run(self):
        """Main loop to fetch charts, features, and publish to Kafka"""
        print("=" * 60)
        print("Starting TrackProducer...")
        print(f"   Chart type: {self.chart_type}")
        print(f"   Start date: {self.start_date}")
        print(f"   Interval: {self.time_interval}s")
        print("=" * 60)

        while True:
            try:
                # Get all existing song IDs (for deduplication)
                existing_song_ids = self.get_existing_song_ids()
                print(f"\nExisting songs in database: {len(existing_song_ids)}")

                # Find missing chart dates
                missing_dates = self.find_missing_chart_dates()

                if not missing_dates:
                    print("All charts up to date!")
                else:
                    print(f"Processing {len(missing_dates)} missing dates...")

                    for date_str in missing_dates:
                        chart_data, new_features = self.process_date(
                            date_str, existing_song_ids
                        )

                        # Publish to Kafka in parallel
                        if chart_data:
                            with ThreadPoolExecutor(max_workers=2) as executor:
                                futures = []
                                futures.append(
                                    executor.submit(
                                        self.publish_charts, chart_data, date_str
                                    )
                                )
                                if new_features:
                                    futures.append(
                                        executor.submit(
                                            self.publish_features, new_features
                                        )
                                    )

                                # Wait for both to complete
                                for future in as_completed(futures):
                                    future.result()

                        # Small delay between dates to be nice to APIs
                        time.sleep(2)

                # Flush Kafka producer
                self.producer.flush()
                print(f"\nSleeping for {self.time_interval}s...")

            except Exception as e:
                print(f"Error in main loop: {e}")

            time.sleep(self.time_interval)


if __name__ == "__main__":
    producer = TrackProducer()
    producer.run()

