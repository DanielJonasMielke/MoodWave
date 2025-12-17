# src/producers/news_producer.py
from supabase import create_client, Client
from datetime import datetime, timedelta
import time
from kafka import KafkaProducer

import json
from dotenv import load_dotenv
import os
from gdeltdoc import GdeltDoc, Filters
from datetime import datetime, timedelta
import yaml

load_dotenv()

class NewsProducer:
    def __init__(self):
        url = os.getenv("SUPABASE_URL")
        key = os.getenv("SUPABASE_KEY")
        
        with open("config.yaml", "r") as f:
            config = yaml.safe_load(f)
        self.news_tones_table = config['supabase']['news_tones_table']
        self.start_date = config['producer']['start_date']
        self.time_interval = config['producer']['fetch_interval_seconds']
        
        self.supabase: Client = create_client(url, key)
        if self.supabase is None:
            raise ValueError("Failed to create Supabase client")
        else:
            print("Supabase client initialized")

        # Initialize Kafka producer
        self.producer = KafkaProducer(
            bootstrap_servers=config['kafka']['bootstrap_servers'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.topic = config['kafka']['news_topic']
        
    def get_existing_dates(self):
        """Fetch all dates we already have in the database"""
        try:
            response = self.supabase.table(self.news_tones_table).select('date').execute()
            # Returns list of dates like ['2020-01-01', '2020-01-02', ...]
            return set(row['date'] for row in response.data)
        except Exception as e:
            print(f"Error fetching existing dates: {e}")
            return set()
        
    def find_missing_dates(self, start_date_str):
        today = datetime.now().strftime("%Y-%m-%d")
        all_dates = set(generate_all_dates(start_date_str, today))
        existing_dates = self.get_existing_dates()
        
        missing = all_dates - existing_dates
        print(f"Missing dates: {len(missing)}")
        
        return sorted(missing)
    
    def fetch_tone_for_date(self, date_str):
        """Fetch average tone for a single date from GDELT"""
        gd = GdeltDoc()
        
        current_date = datetime.strptime(date_str, "%Y-%m-%d")
        day_end = current_date + timedelta(days=1)
        
        f = Filters(
            keyword="",
            start_date=current_date.strftime("%Y-%m-%d"),
            end_date=day_end.strftime("%Y-%m-%d"),
            country="GM",
            language="GERMAN"
        )
        
        try:
            timeline = gd.timeline_search("timelinetone", f)
            
            if not timeline.empty:
                avg_tone = timeline['Average Tone'].mean()
                print(f"    {date_str}: tone = {avg_tone:.4f}")
                return avg_tone
            else:
                print(f"    {date_str}: No data")
                return None
                
        except Exception as e:
            print(f"    {date_str}: Error - {e}")
            return None
        
    def publish_tone(self, date_str, avg_tone):
        """Publish a single date's tone to Kafka"""
        message = {
            'date': date_str,
            'average_tone': avg_tone,
            'timestamp': datetime.now().isoformat()
        }
        
        self.producer.send(self.topic, value=message)
        print(f"-> Published to Kafka: {date_str}")

    def run(self):
        """Main loop to fetch and publish news tones"""
        print("Starting NewsProducer...")
        
        while True:
            missing_dates = self.find_missing_dates(self.start_date)

            missing_dates_tones = []
            for date in missing_dates:
                date_tone = self.fetch_tone_for_date(date_str=date)
                missing_dates_tones.append({
                    "date": date,
                    "average_tone": date_tone
                })
            
            for item in missing_dates_tones:
                if item["average_tone"] is not None:
                    self.publish_tone(item["date"], item["average_tone"])

            time.sleep(self.time_interval)
    

def generate_all_dates(start_date_str, end_date_str):
    """Create a list of every single date between start and end"""
    # Convert strings to datetime objects
    start = datetime.strptime(start_date_str, "%Y-%m-%d")
    end = datetime.strptime(end_date_str, "%Y-%m-%d")
    
    all_dates = []
    
    current = start
    
    while current <= end:
        all_dates.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)
    
    return all_dates
        
if __name__ == "__main__":
    producer = NewsProducer()
    producer.run()

