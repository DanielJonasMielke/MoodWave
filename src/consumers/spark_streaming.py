"""
Spark Kafka Consumer for MoodWave Music Sentiment Analysis
Consumes three Kafka topics with time-window synchronization:
- news_tone (1 message per date)
- daily_charts (1 message per date with 200 tracks)
- musical_features (<=200 individual messages per date, no date field)
"""

import os
import yaml
import json
import time
from datetime import datetime, timedelta
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from supabase import create_client, Client
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, ArrayType, BooleanType
    


def load_config() -> dict:
    config_path = os.environ.get("CONFIG_PATH", "config.yaml")
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    
    print("\n" + "=" * 70)
    print("MOODWAVE SPARK KAFKA CONSUMER")
    print("=" * 70)
    print(f"Configuration loaded from: {config_path}")
    print(f"Kafka bootstrap servers: {config['kafka']['bootstrap_servers']}")
    print(f"Topics:")
    print(f"  - News: {config['kafka']['news_topic']}")
    print(f"  - Charts: {config['kafka']['charts_topic']}")
    print(f"  - Features: {config['kafka']['features_topic']}")
    print(f"Simulation interval: {config['sandbox']['simulation_interval_seconds']}s per day")
    print("=" * 70 + "\n")
    return config

def init_spark_session() -> SparkSession:
    print("Initializing Spark Session...")
    spark = SparkSession.builder \
        .appName("MoodWave-Kafka-Consumer") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0") \
        .config("spark.sql.streaming.schemaInference", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")  # Reduce log verbosity
    print(f"-> Spark Session created: {spark.version}")
    print(20 * "-")
    print(3 * "\n")
    return spark

def create_supabase_client() -> Client:
    print("Connecting to Supabase database...")
    supabase_url = os.getenv("SANDBOX_SUPABASE_URL")
    supabase_key = os.getenv("SANDBOX_SUPABASE_KEY")
    
    if not supabase_url or not supabase_key:
        raise ValueError("SANDBOX_SUPABASE_URL and SANDBOX_SUPABASE_KEY must be set in .env file")
    
    supabase: Client = create_client(supabase_url, supabase_key)
    print(f"-> Connected to Supabase: {supabase_url[:30]}...")
    print()

    return supabase

def create_stream_readers(config: dict, spark: SparkSession):
    print("Setting up Kafka stream readers...")
    
    kafka_bootstrap_servers = config['kafka']['bootstrap_servers']
    news_topic = config['kafka']['news_topic']
    charts_topic = config['kafka']['charts_topic']
    features_topic = config['kafka']['features_topic']
    
    # Read from news_tone topic
    news_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", news_topic) \
        .option("startingOffsets", "earliest") \
        .load()
    
    print(f"-> News stream connected to topic: {news_topic}")
    
    # Read from daily_charts topic
    charts_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", charts_topic) \
        .option("startingOffsets", "earliest") \
        .load()
    
    print(f"-> Charts stream connected to topic: {charts_topic}")
    
    # Read from musical_features topic
    features_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", features_topic) \
        .option("startingOffsets", "earliest") \
        .load()
    
    print(f"-> Features stream connected to topic: {features_topic}")
    print()
    return news_stream, charts_stream, features_stream

def parse_all_topics_json(news_stream, charts_stream, features_stream):
    print("Parsing JSON from Kafka messages...")
    
    # Schema for news_tone messages
    news_schema = StructType([
        StructField("date", StringType(), True),
        StructField("average_tone", DoubleType(), True),
        StructField("timestamp", StringType(), True),
        StructField("sandbox", BooleanType(), True)
    ])
    
    # Schema for daily_charts messages - contains array of tracks
    track_schema = StructType([
        StructField("rank", IntegerType(), True),
        StructField("previous_rank", IntegerType(), True),
        StructField("peak_rank", IntegerType(), True),
        StructField("peak_date", StringType(), True),
        StructField("appear_on_chart", StringType(), True),
        StructField("track_name", StringType(), True),
        StructField("track_id", StringType(), True),
        StructField("track_uri", StringType(), True),
        StructField("artists", StringType(), True),
        StructField("artist_uris", StringType(), True),
        StructField("release_date", StringType(), True),
        StructField("chart_date", StringType(), True),
        StructField("chart_type", StringType(), True)
    ])
    
    charts_schema = StructType([
        StructField("chart_date", StringType(), True),
        StructField("chart_type", StringType(), True),
        StructField("tracks", ArrayType(track_schema), True),
        StructField("timestamp", StringType(), True),
        StructField("sandbox", BooleanType(), True)
    ])
    
    # Schema for musical_features messages
    features_schema = StructType([
        StructField("track_id", StringType(), True),
        StructField("track_name", StringType(), True),
        StructField("chart_date", StringType(), True),
        StructField("artists", StringType(), True),
        StructField("track_uri", StringType(), True),
        StructField("release_date", StringType(), True),
        StructField("tempo", DoubleType(), True),
        StructField("duration", DoubleType(), True),
        StructField("energy", DoubleType(), True),
        StructField("danceability", DoubleType(), True),
        StructField("happiness", DoubleType(), True),
        StructField("acousticness", DoubleType(), True),
        StructField("instrumentalness", DoubleType(), True),
        StructField("liveness", DoubleType(), True),
        StructField("speechiness", DoubleType(), True),
        StructField("loudness_db", DoubleType(), True),
        StructField("popularity", IntegerType(), True),
        StructField("key", StringType(), True),
        StructField("mode", StringType(), True),
        StructField("camelot", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("sandbox", BooleanType(), True)
    ])
    
    # Parse news stream
    news_parsed = news_stream.select(
        from_json(col("value").cast("string"), news_schema).alias("data")
    ).select("data.*")
    
    print(f"✓ News stream parsed")
    
    # Parse charts stream
    charts_parsed = charts_stream.select(
        from_json(col("value").cast("string"), charts_schema).alias("data")
    ).select("data.*")
    
    print(f"✓ Charts stream parsed")
    
    # Parse features stream
    features_parsed = features_stream.select(
        from_json(col("value").cast("string"), features_schema).alias("data")
    ).select("data.*")
    
    print(f"✓ Features stream parsed")
    print()
    return news_parsed, charts_parsed, features_parsed


def main():
    """Main entry point - this is where execution starts"""
    
    # Step 1: Load environment variables from .env file
    load_dotenv()
    print("Environment variables loaded")
    
    # Step 2: Load configuration from config.yaml
    config = load_config()

    # Step 3: Initialize Spark Session
    spark = init_spark_session()
    
    # Step 4: Connect to Supabase (SANDBOX database)
    supabase = create_supabase_client()

    # Step 5: Set up Kafka streaming reads for each topic
    news_stream, charts_stream, features_stream = create_stream_readers(config, spark)
    print(news_stream)

    # Step 6: Parse JSON from Kafka messages
    news_parsed, charts_parsed, features_parsed = parse_all_topics_json(
        news_stream, charts_stream, features_stream
    )

if __name__ == "__main__":
    main()