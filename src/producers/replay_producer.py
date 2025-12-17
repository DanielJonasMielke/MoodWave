# src/producers/replay_producer.py
"""
Replay producer that streams historical data from Supabase to Kafka.
Simulates daily data flow for testing the full pipeline.
"""

from supabase import create_client, Client
from kafka import KafkaProducer
import json
import yaml
import os
import time
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()


def load_config() -> dict:
    """Load configuration from config.yaml"""
    config_path = os.environ.get("CONFIG_PATH", "config.yaml")
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


class ReplayProducer:
    """
    Replays historical data from Supabase to Kafka.
    Simulates a real-time data stream for testing the analysis pipeline.
    """

    def __init__(self):
        url = os.getenv("SUPABASE_URL")
        key = os.getenv("SUPABASE_KEY")

        self.config = load_config()

        # Supabase client
        self.supabase: Client = create_client(url, key)
        if self.supabase is None:
            raise ValueError("Failed to create Supabase client")
        print("[REPLAY] Supabase client initialized")

        # Kafka producer
        self.producer = KafkaProducer(
            bootstrap_servers=self.config["kafka"]["bootstrap_servers"],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        print("[REPLAY] Kafka producer initialized")

        # Topics
        self.news_topic = self.config["kafka"]["news_topic"]
        self.charts_topic = self.config["kafka"]["charts_topic"]
        self.features_topic = self.config["kafka"]["features_topic"]

        # Tables
        self.news_table = self.config["supabase"]["news_tones_table"]
        self.charts_table = self.config["supabase"]["regional_charts_table"]
        self.features_table = self.config["supabase"]["track_features_table"]

    def fetch_news_data(self, limit: int = 1000) -> list:
        """Fetch news tone data from Supabase"""
        print(f"[REPLAY] Fetching news data from '{self.news_table}'...")
        try:
            response = (
                self.supabase.table(self.news_table)
                .select("*")
                .order("date")
                .limit(limit)
                .execute()
            )
            print(f"[REPLAY] Found {len(response.data)} news records")
            return response.data
        except Exception as e:
            print(f"[REPLAY] Error fetching news data: {e}")
            return []

    def fetch_charts_data(self, limit: int = 5000) -> list:
        """Fetch chart data from Supabase"""
        print(f"[REPLAY] Fetching charts data from '{self.charts_table}'...")
        try:
            response = (
                self.supabase.table(self.charts_table)
                .select("*")
                .order("chart_date")
                .limit(limit)
                .execute()
            )
            print(f"[REPLAY] Found {len(response.data)} chart records")
            return response.data
        except Exception as e:
            print(f"[REPLAY] Error fetching charts data: {e}")
            return []

    def fetch_features_data(self, limit: int = 5000) -> list:
        """Fetch track features from Supabase"""
        print(f"[REPLAY] Fetching features data from '{self.features_table}'...")
        try:
            response = (
                self.supabase.table(self.features_table)
                .select("*")
                .limit(limit)
                .execute()
            )
            print(f"[REPLAY] Found {len(response.data)} feature records")
            return response.data
        except Exception as e:
            print(f"[REPLAY] Error fetching features data: {e}")
            return []

    def stream_news(self, data: list, delay: float = 0.1):
        """Stream news data to Kafka"""
        print(f"\n[STREAM] Streaming {len(data)} news records to '{self.news_topic}'...")
        
        for i, record in enumerate(data):
            message = {
                "date": record.get("date"),
                "average_tone": record.get("average_tone"),
                "timestamp": datetime.now().isoformat(),
                "replay": True,
            }
            self.producer.send(self.news_topic, value=message)
            
            if (i + 1) % 50 == 0:
                print(f"   -> Sent {i + 1}/{len(data)} news records")
            
            time.sleep(delay)
        
        self.producer.flush()
        print(f"[STREAM] Completed streaming {len(data)} news records")

    def stream_charts(self, data: list, delay: float = 0.05):
        """Stream chart data to Kafka, grouped by date"""
        print(f"\n[STREAM] Streaming {len(data)} chart records to '{self.charts_topic}'...")
        
        # Group by date
        by_date = {}
        for record in data:
            date = record.get("chart_date")
            if date not in by_date:
                by_date[date] = []
            by_date[date].append(record)
        
        for i, (date, tracks) in enumerate(sorted(by_date.items())):
            message = {
                "chart_date": date,
                "chart_type": tracks[0].get("chart_type", "regional"),
                "tracks": [
                    {
                        "rank": t.get("rank"),
                        "previous_rank": t.get("previous_rank"),
                        "peak_rank": t.get("peak_rank"),
                        "track_name": t.get("track_name"),
                        "track_id": t.get("track_id"),
                        "artists": t.get("artists"),
                        "release_date": t.get("release_date"),
                    }
                    for t in tracks
                ],
                "timestamp": datetime.now().isoformat(),
                "replay": True,
            }
            self.producer.send(self.charts_topic, value=message)
            
            if (i + 1) % 10 == 0:
                print(f"   -> Sent {i + 1}/{len(by_date)} chart dates")
            
            time.sleep(delay)
        
        self.producer.flush()
        print(f"[STREAM] Completed streaming {len(by_date)} chart dates")

    def stream_features(self, data: list, delay: float = 0.01, batch_size: int = 50):
        """Stream feature data to Kafka in batches"""
        print(f"\n[STREAM] Streaming {len(data)} feature records to '{self.features_topic}'...")
        
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            message = {
                "features": [
                    {
                        "track_id": f.get("track_id"),
                        "track_name": f.get("track_name"),
                        "artists": f.get("artists"),
                        "tempo": f.get("tempo"),
                        "energy": f.get("energy"),
                        "danceability": f.get("danceability"),
                        "happiness": f.get("happiness"),
                        "acousticness": f.get("acousticness"),
                        "instrumentalness": f.get("instrumentalness"),
                        "liveness": f.get("liveness"),
                        "speechiness": f.get("speechiness"),
                        "loudness_db": f.get("loudness_db"),
                        "popularity": f.get("popularity"),
                        "key": f.get("key"),
                        "mode": f.get("mode"),
                    }
                    for f in batch
                ],
                "count": len(batch),
                "timestamp": datetime.now().isoformat(),
                "replay": True,
            }
            self.producer.send(self.features_topic, value=message)
            
            batch_num = (i // batch_size) + 1
            total_batches = (len(data) + batch_size - 1) // batch_size
            if batch_num % 10 == 0 or batch_num == total_batches:
                print(f"   -> Sent batch {batch_num}/{total_batches}")
            
            time.sleep(delay)
        
        self.producer.flush()
        print(f"[STREAM] Completed streaming {len(data)} feature records")

    def run(self, include_news: bool = True, include_charts: bool = True, 
            include_features: bool = True, delay: float = 0.1):
        """Run the replay pipeline"""
        print("=" * 70)
        print("MOODWAVE REPLAY PRODUCER")
        print("=" * 70)
        print(f"Kafka: {self.config['kafka']['bootstrap_servers']}")
        print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 70)

        if include_news:
            news_data = self.fetch_news_data()
            if news_data:
                self.stream_news(news_data, delay=delay)

        if include_charts:
            charts_data = self.fetch_charts_data()
            if charts_data:
                self.stream_charts(charts_data, delay=delay)

        if include_features:
            features_data = self.fetch_features_data()
            if features_data:
                self.stream_features(features_data, delay=delay * 0.5)

        print("\n" + "=" * 70)
        print("REPLAY COMPLETE")
        print("=" * 70)


def main():
    """Entry point"""
    import argparse

    parser = argparse.ArgumentParser(description="Replay historical data from Supabase to Kafka")
    parser.add_argument("--news", action="store_true", help="Include news data")
    parser.add_argument("--charts", action="store_true", help="Include charts data")
    parser.add_argument("--features", action="store_true", help="Include features data")
    parser.add_argument("--all", action="store_true", help="Include all data types")
    parser.add_argument("--delay", type=float, default=0.1, help="Delay between messages (seconds)")
    
    args = parser.parse_args()

    # Default to all if nothing specified
    if not (args.news or args.charts or args.features or args.all):
        args.all = True

    producer = ReplayProducer()
    producer.run(
        include_news=args.all or args.news,
        include_charts=args.all or args.charts,
        include_features=args.all or args.features,
        delay=args.delay,
    )


if __name__ == "__main__":
    main()

