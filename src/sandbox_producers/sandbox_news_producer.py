# src/producers/sandbox_news_producer.py
"""
Sandbox news producer that simulates real-time data flow in accelerated time.
Fetches GDELT news tones for sequential days based on simulation time.
Each simulated day processes in `simulation_interval_seconds` real-time seconds.
"""

from datetime import datetime, timedelta, date
import time
from kafka import KafkaProducer
import json
import os
import yaml
from gdeltdoc import GdeltDoc, Filters
from dotenv import load_dotenv
from prometheus_client import start_http_server, Gauge, Counter, Info

load_dotenv()


def load_config() -> dict:
    """Load configuration from config.yaml"""
    config_path = os.environ.get("CONFIG_PATH", "config.yaml")
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


class SandboxNewsProducer:
    """
    Sandbox news producer that simulates real-time data flow in accelerated time.
    Fetches GDELT news tones for sequential days based on simulation time.
    """
    
    def __init__(self):
        self.config = load_config()
        
        # Sandbox configuration
        self.sandbox_config = self.config.get('sandbox', {})
        if not self.sandbox_config.get('enabled', False):
            raise ValueError("Sandbox mode is not enabled in config.yaml")
        
        self.start_date = datetime.strptime(self.sandbox_config['start_date'], "%Y-%m-%d").date()
        self.interval_seconds = self.sandbox_config['simulation_interval_seconds']
        self.start_timestamp = time.time()
        
        # GDELT configuration
        self.country = self.config['gdelt']['country']
        self.language = self.config['gdelt']['language']
        
        # Kafka producer
        self.producer = KafkaProducer(
            bootstrap_servers=self.config['kafka']['bootstrap_servers'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.topic = self.config['kafka']['news_topic']
        
        # === PROMETHEUS METRICS ===
        # Simulation progress
        self.simulation_day_gauge = Gauge(
            'sandbox_news_simulation_day', 
            'Current simulation day number'
        )
        self.simulation_date_info = Info(
            'sandbox_news_simulation_date',
            'Current simulation date'
        )
        
        # API call counters
        self.api_calls_total = Counter(
            'sandbox_news_api_calls_total',
            'Total GDELT API calls made'
        )
        self.api_calls_success = Counter(
            'sandbox_news_api_calls_success',
            'Successful GDELT API calls'
        )
        self.api_calls_failed = Counter(
            'sandbox_news_api_calls_failed',
            'Failed GDELT API calls'
        )
        
        # Processing metrics
        self.last_tone_value = Gauge(
            'sandbox_news_last_tone_value',
            'Last fetched tone value'
        )
        self.messages_published = Counter(
            'sandbox_news_messages_published_total',
            'Total messages published to Kafka'
        )
        # === END OF METRICS ===
        
        print("=" * 60)
        print("SANDBOX NEWS PRODUCER INITIALIZED")
        print("=" * 60)
        print(f"Start date: {self.start_date}")
        print(f"Simulation interval: {self.interval_seconds}s per day")
        print(f"Kafka topic: {self.topic}")
        print(f"GDELT: {self.country} / {self.language}")
        print("=" * 60)
    
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
    
    def fetch_tone_for_date(self, date_obj: date) -> float:
        """Fetch average tone for a single date from GDELT"""
        date_str = date_obj.strftime("%Y-%m-%d")
        
        gd = GdeltDoc()
        day_end = date_obj + timedelta(days=1)
        
        f = Filters(
            keyword="",
            start_date=date_str,
            end_date=day_end.strftime("%Y-%m-%d"),
            country=self.country,
            language=self.language
        )
        
        # Increment API call counter
        self.api_calls_total.inc()
        
        try:
            timeline = gd.timeline_search("timelinetone", f)
            
            if not timeline.empty:
                avg_tone = timeline['Average Tone'].mean()
                print(f"    {date_str}: tone = {avg_tone:.4f}")
                
                # Track success and value
                self.api_calls_success.inc()
                self.last_tone_value.set(avg_tone)
                
                return avg_tone
            else:
                print(f"    {date_str}: No data")
                self.api_calls_failed.inc()
                return None
                
        except Exception as e:
            print(f"    {date_str}: Error - {e}")
            self.api_calls_failed.inc()
            return None
    
    def publish_tone(self, date_obj: date, avg_tone: float):
        """Publish news tone to Kafka"""
        date_str = date_obj.strftime("%Y-%m-%d")
        message = {
            'date': date_str,
            'average_tone': avg_tone,
            'timestamp': datetime.now().isoformat(),
            'sandbox': True
        }
        
        self.producer.send(self.topic, value=message)
        self.producer.flush()
        
        # Track published message
        self.messages_published.inc()
        
        print(f"-> Published to Kafka: {date_str}")
    
    def run(self):
        """Main loop - process one day per simulation interval"""
        print("\nStarting sandbox simulation...\n")
        
        # Start Prometheus metrics server on port 8000
        print("Starting Prometheus metrics server on port 8000...")
        start_http_server(8000)
        print("Metrics available at http://localhost:8000/metrics\n")
        
        last_processed_day = -1
        
        while True:
            try:
                # Calculate current simulation day (independent, deterministic)
                current_date, days_elapsed = self.calculate_current_simulation_day()
                
                # Update simulation progress metrics
                self.simulation_day_gauge.set(days_elapsed)
                self.simulation_date_info.info({
                    'date': str(current_date),
                    'days_elapsed': str(days_elapsed)
                })
                
                # Only process if we've moved to a new day
                if days_elapsed != last_processed_day:
                    print(f"\n{'='*60}")
                    print(f"DAY {days_elapsed}: {current_date}")
                    print(f"{'='*60}")
                    
                    # Fetch tone for this day from GDELT API
                    avg_tone = self.fetch_tone_for_date(current_date)
                    
                    # Publish to Kafka if we got data
                    if avg_tone is not None:
                        self.publish_tone(current_date, avg_tone)
                    else:
                        print(f"-> Skipping {current_date} (no data)")
                    
                    last_processed_day = days_elapsed
                
                # Check every second to catch day transitions quickly
                time.sleep(1)
                
            except KeyboardInterrupt:
                print("\n\nShutting down...")
                self.producer.flush()
                break
            except Exception as e:
                print(f"Error in main loop: {e}")
                time.sleep(5)


if __name__ == "__main__":
    producer = SandboxNewsProducer()
    producer.run()