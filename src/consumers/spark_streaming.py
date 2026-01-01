"""
Spark Kafka Consumer for MoodWave Music Sentiment Analysis

Dual-Stream Architecture:
- Stream 1: Raw data storage (Append mode) - writes all topics to Supabase
- Stream 2: Daily feature aggregations (Update mode with watermark) - calculates daily averages

Consumes three Kafka topics:
- news_tones (1 message per date)
- daily_charts (1 message per date with 200 tracks)
- musical_features (<=200 individual messages per date)
"""

import os
import yaml
from dotenv import load_dotenv
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, from_json, avg, count, lit, to_timestamp, concat, window, explode
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    IntegerType, ArrayType, BooleanType
)
from supabase import create_client, Client


# =============================================================================
# CONFIGURATION
# =============================================================================

def load_config() -> dict:
    """Load configuration from config.yaml"""
    config_path = os.environ.get("CONFIG_PATH", "config.yaml")
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
        # Override Kafka bootstrap servers from environment if set (for Docker)
        if os.getenv('KAFKA_BOOTSTRAP_SERVERS'):
            config['kafka']['bootstrap_servers'] = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
    
    print("\n" + "=" * 70)
    print("MOODWAVE SPARK KAFKA CONSUMER")
    print("=" * 70)
    print(f"Configuration loaded from: {config_path}")
    print(f"Kafka bootstrap servers: {config['kafka']['bootstrap_servers']}")
    print(f"Topics:")
    print(f"  - News: {config['kafka']['news_topic']}")
    print(f"  - Charts: {config['kafka']['charts_topic']}")
    print(f"  - Features: {config['kafka']['features_topic']}")
    
    # Consumer config with defaults
    consumer_config = config.get('consumer', {})
    trigger_interval = consumer_config.get('trigger_interval_seconds', 300)
    checkpoint_location = consumer_config.get('checkpoint_location', '/tmp/spark_checkpoints')
    
    # Override with sandbox settings if sandbox mode is enabled
    sandbox_config = config.get('sandbox', {})
    if sandbox_config.get('enabled', False):
        trigger_interval = sandbox_config.get('consumer_trigger_interval_seconds', 10)
        print(f"  [SANDBOX MODE ACTIVE]")
    
    print(f"Trigger interval: {trigger_interval}s")
    print(f"Checkpoint location: {checkpoint_location}")
    print("=" * 70 + "\n")
    
    # Store computed values back in config for easy access
    config['_trigger_interval'] = trigger_interval
    config['_checkpoint_location'] = checkpoint_location
    
    return config


def init_spark_session() -> SparkSession:
    """Initialize Spark Session with Kafka support"""
    print("Initializing Spark Session...")
    spark = SparkSession.builder \
        .appName("MoodWave-Kafka-Consumer") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.sql.streaming.schemaInference", "true") \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    print(f"-> Spark Session created: {spark.version}")
    print("-" * 40 + "\n")
    return spark


def create_supabase_client() -> Client:
    """Create Supabase client for database operations"""
    print("Connecting to Supabase database...")
    supabase_url = os.getenv("SANDBOX_SUPABASE_URL")
    supabase_key = os.getenv("SANDBOX_SUPABASE_KEY")
    
    if not supabase_url or not supabase_key:
        raise ValueError("SANDBOX_SUPABASE_URL and SANDBOX_SUPABASE_KEY must be set in .env file")
    
    supabase: Client = create_client(supabase_url, supabase_key)
    print(f"-> Connected to Supabase: {supabase_url[:50]}...")
    print()
    return supabase


# =============================================================================
# SCHEMAS
# =============================================================================

def get_news_schema() -> StructType:
    """Schema for news_tones messages"""
    return StructType([
        StructField("date", StringType(), True),
        StructField("average_tone", DoubleType(), True),
        StructField("timestamp", StringType(), True),
        StructField("sandbox", BooleanType(), True)
    ])


def get_track_schema() -> StructType:
    """Schema for individual track in daily_charts"""
    return StructType([
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


def get_charts_schema() -> StructType:
    """Schema for daily_charts messages (contains array of tracks)"""
    return StructType([
        StructField("chart_date", StringType(), True),
        StructField("chart_type", StringType(), True),
        StructField("tracks", ArrayType(get_track_schema()), True),
        StructField("timestamp", StringType(), True),
        StructField("sandbox", BooleanType(), True)
    ])


def get_features_schema() -> StructType:
    """Schema for musical_features messages"""
    return StructType([
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


# =============================================================================
# KAFKA STREAM READERS
# =============================================================================

def create_stream_readers(config: dict, spark: SparkSession):
    """Create Kafka stream readers for all three topics"""
    print("Setting up Kafka stream readers...")
    
    kafka_servers = config['kafka']['bootstrap_servers']
    news_topic = config['kafka']['news_topic']
    charts_topic = config['kafka']['charts_topic']
    features_topic = config['kafka']['features_topic']
    
    # News stream
    news_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_servers) \
        .option("subscribe", news_topic) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()
    print(f"-> News stream connected: {news_topic}")
    
    # Charts stream
    charts_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_servers) \
        .option("subscribe", charts_topic) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()
    print(f"-> Charts stream connected: {charts_topic}")
    
    # Features stream
    features_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_servers) \
        .option("subscribe", features_topic) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()
    print(f"-> Features stream connected: {features_topic}")
    print()
    
    return news_stream, charts_stream, features_stream


def parse_kafka_streams(news_stream, charts_stream, features_stream):
    """Parse JSON from Kafka message values"""
    print("Parsing JSON from Kafka messages...")
    
    # Parse news
    news_parsed = news_stream.select(
        from_json(col("value").cast("string"), get_news_schema()).alias("data")
    ).select("data.*")
    print("-> News stream parsed")
    
    # Parse charts
    charts_parsed = charts_stream.select(
        from_json(col("value").cast("string"), get_charts_schema()).alias("data")
    ).select("data.*")
    print("-> Charts stream parsed")
    
    # Parse features
    features_parsed = features_stream.select(
        from_json(col("value").cast("string"), get_features_schema()).alias("data")
    ).select("data.*")
    print("-> Features stream parsed")
    print()
    
    return news_parsed, charts_parsed, features_parsed


# =============================================================================
# SUPABASE WRITE FUNCTIONS
# =============================================================================

def upsert_news_batch(batch_df: DataFrame, batch_id: int, supabase: Client):
    """
    Write news_tones batch to gdelt_daily_tone table.
    UPSERT on date column.
    """
    if batch_df.isEmpty():
        return
    
    rows = batch_df.collect()
    print(f"[NEWS] Batch {batch_id}: Writing {len(rows)} records to gdelt_daily_tone")
    
    for row in rows:
        record = {
            "date": row["date"],
            "average_tone": row["average_tone"]
        }
        try:
            supabase.table("gdelt_daily_tone").upsert(
                record, 
                on_conflict="date"
            ).execute()
        except Exception as e:
            print(f"[NEWS] Error upserting record: {e}")


def upsert_charts_batch(batch_df: DataFrame, batch_id: int, supabase: Client):
    """
    Write daily_charts batch to spotify_regional_charts table.
    Explodes tracks array and UPSERTs on (track_uri, chart_date).
    """
    if batch_df.isEmpty():
        return
    
    # Explode the tracks array to get individual track rows
    exploded_df = batch_df.select(explode(col("tracks")).alias("track"))
    tracks_df = exploded_df.select("track.*")
    
    if tracks_df.isEmpty():
        return
    
    rows = tracks_df.collect()
    print(f"[CHARTS] Batch {batch_id}: Writing {len(rows)} tracks to spotify_regional_charts")
    
    for row in rows:
        record = {
            "rank": row["rank"],
            "previous_rank": row["previous_rank"],
            "peak_rank": row["peak_rank"],
            "peak_date": row["peak_date"],
            "appear_on_chart": row["appear_on_chart"],
            "track_name": row["track_name"],
            "track_uri": row["track_uri"],
            "artists": row["artists"],
            "artist_uris": row["artist_uris"],
            "release_date": row["release_date"],
            "chart_date": row["chart_date"],
            "chart_type": row["chart_type"]
        }
        try:
            supabase.table("spotify_regional_charts").upsert(
                record,
                on_conflict="track_uri,chart_date"
            ).execute()
        except Exception as e:
            print(f"[CHARTS] Error upserting record: {e}")


def upsert_features_batch(batch_df: DataFrame, batch_id: int, supabase: Client):
    """
    Write musical_features batch to track_features table.
    UPSERT on track_id column.
    """
    if batch_df.isEmpty():
        return
    
    rows = batch_df.collect()
    print(f"[FEATURES] Batch {batch_id}: Writing {len(rows)} records to track_features")
    
    for row in rows:
        record = {
            "track_id": row["track_id"],
            "track_name": row["track_name"],
            "artists": row["artists"],
            "track_uri": row["track_uri"],
            "release_date": row["release_date"],
            "tempo": row["tempo"],
            "duration": row["duration"],
            "energy": row["energy"],
            "danceability": row["danceability"],
            "happiness": row["happiness"],
            "acousticness": row["acousticness"],
            "instrumentalness": row["instrumentalness"],
            "liveness": row["liveness"],
            "speechiness": row["speechiness"],
            "loudness_db": row["loudness_db"],
            "popularity": row["popularity"],
            "key": row["key"],
            "mode": row["mode"],
            "camelot": row["camelot"]
        }
        try:
            supabase.table("track_features").upsert(
                record,
                on_conflict="track_id"
            ).execute()
        except Exception as e:
            print(f"[FEATURES] Error upserting record: {e}")


def upsert_daily_avg_batch(batch_df: DataFrame, batch_id: int, supabase: Client):
    """
    Write aggregated daily averages to track_features_daily_avg table.
    UPSERT on date column.
    """
    if batch_df.isEmpty():
        return
    
    rows = batch_df.collect()
    print(f"[DAILY_AVG] Batch {batch_id}: Writing {len(rows)} aggregated records")
    
    for row in rows:
        # Extract date from the window or date column
        date_value = row["chart_date"]
        
        record = {
            "date": date_value,
            "avg_tempo": row["avg_tempo"],
            "avg_duration": row["avg_duration"],
            "avg_energy": row["avg_energy"],
            "avg_danceability": row["avg_danceability"],
            "avg_happiness": row["avg_happiness"],
            "avg_acousticness": row["avg_acousticness"],
            "avg_instrumentalness": row["avg_instrumentalness"],
            "avg_liveness": row["avg_liveness"],
            "avg_speechiness": row["avg_speechiness"],
            "avg_loudness_db": row["avg_loudness_db"],
            "avg_popularity": row["avg_popularity"],
            "track_count": row["track_count"],
            "reliability_score": row["reliability_score"]
        }
        
        try:
            supabase.table("track_features_daily_avg").upsert(
                record,
                on_conflict="date"
            ).execute()
            print(f"[DAILY_AVG] Upserted: date={date_value}, count={row['track_count']}, reliability={row['reliability_score']:.2f}")
        except Exception as e:
            print(f"[DAILY_AVG] Error upserting record: {e}")


# =============================================================================
# STREAM 1: RAW DATA STORAGE (APPEND MODE)
# =============================================================================

def start_raw_data_streams(
    news_parsed: DataFrame,
    charts_parsed: DataFrame,
    features_parsed: DataFrame,
    config: dict,
    supabase: Client
):
    """
    Start Stream 1: Raw data storage streams for all three topics.
    Uses Append mode with foreachBatch to write to Supabase.
    """
    print("=" * 70)
    print("STARTING STREAM 1: RAW DATA STORAGE")
    print("=" * 70)
    
    trigger_interval = config['_trigger_interval']
    checkpoint_base = config['_checkpoint_location']
    
    # News stream writer
    news_query = news_parsed.writeStream \
        .outputMode("append") \
        .trigger(processingTime=f"{trigger_interval} seconds") \
        .foreachBatch(lambda df, id: upsert_news_batch(df, id, supabase)) \
        .option("checkpointLocation", f"{checkpoint_base}/stream1_news") \
        .queryName("raw_news_storage") \
        .start()
    print(f"-> News raw storage stream started")
    
    # Charts stream writer
    charts_query = charts_parsed.writeStream \
        .outputMode("append") \
        .trigger(processingTime=f"{trigger_interval} seconds") \
        .foreachBatch(lambda df, id: upsert_charts_batch(df, id, supabase)) \
        .option("checkpointLocation", f"{checkpoint_base}/stream1_charts") \
        .queryName("raw_charts_storage") \
        .start()
    print(f"-> Charts raw storage stream started")
    
    # Features stream writer
    features_query = features_parsed.writeStream \
        .outputMode("append") \
        .trigger(processingTime=f"{trigger_interval} seconds") \
        .foreachBatch(lambda df, id: upsert_features_batch(df, id, supabase)) \
        .option("checkpointLocation", f"{checkpoint_base}/stream1_features") \
        .queryName("raw_features_storage") \
        .start()
    print(f"-> Features raw storage stream started")
    print()
    
    return [news_query, charts_query, features_query]


# =============================================================================
# STREAM 2: DAILY FEATURE AGGREGATIONS (UPDATE MODE WITH WATERMARK)
# =============================================================================

def create_watermarked_features_stream(features_parsed: DataFrame) -> DataFrame:
    """
    Create watermarked stream for daily aggregations.
    
    Critical: Convert chart_date string to timestamp at 23:59:59 (end of day).
    This ensures the watermark closes after 1 day, not 2.
    
    Example:
    - chart_date: "2020-01-01"
    - event_time: 2020-01-01 23:59:59
    - watermark = max(event_time) - 1 day
    """
    
    # Convert chart_date string to timestamp at end of day (23:59:59)
    # Format: "2020-01-01" -> "2020-01-01 23:59:59"
    features_with_event_time = features_parsed.withColumn(
        "event_time",
        to_timestamp(concat(col("chart_date"), lit(" 23:59:59")), "yyyy-MM-dd HH:mm:ss")
    )
    
    # Apply watermark: 1 day delay for late arrivals
    watermarked_df = features_with_event_time.withWatermark("event_time", "1 day")
    
    return watermarked_df


def create_daily_aggregation(watermarked_df: DataFrame) -> DataFrame:
    """
    Aggregate musical features by chart_date.
    
    Calculates:
    - Average of all numeric feature columns
    - track_count: number of distinct tracks
    - reliability_score: track_count / 200.0
    """
    
    aggregated_df = watermarked_df.groupBy("chart_date").agg(
        avg("tempo").alias("avg_tempo"),
        avg("duration").alias("avg_duration"),
        avg("energy").alias("avg_energy"),
        avg("danceability").alias("avg_danceability"),
        avg("happiness").alias("avg_happiness"),
        avg("acousticness").alias("avg_acousticness"),
        avg("instrumentalness").alias("avg_instrumentalness"),
        avg("liveness").alias("avg_liveness"),
        avg("speechiness").alias("avg_speechiness"),
        avg("loudness_db").alias("avg_loudness_db"),
        avg("popularity").alias("avg_popularity"),
        count("track_id").alias("track_count")
    ).withColumn(
        "reliability_score",
        col("track_count") / lit(200.0)
    )
    
    return aggregated_df


def start_daily_aggregation_stream(
    features_parsed: DataFrame,
    config: dict,
    supabase: Client
):
    """
    Start Stream 2: Daily feature aggregations with watermarking.
    Uses Update mode to emit updated aggregations as new data arrives.
    """
    print("=" * 70)
    print("STARTING STREAM 2: DAILY FEATURE AGGREGATIONS")
    print("=" * 70)
    
    trigger_interval = config['_trigger_interval']
    checkpoint_base = config['_checkpoint_location']
    
    # Create watermarked stream
    watermarked_df = create_watermarked_features_stream(features_parsed)
    print("-> Watermark applied: 1 day on event_time (chart_date at 23:59:59)")
    
    # Create daily aggregation
    daily_avg_df = create_daily_aggregation(watermarked_df)
    print("-> Daily aggregation pipeline created")
    
    # Start the stream with Update mode
    daily_avg_query = daily_avg_df.writeStream \
        .outputMode("update") \
        .trigger(processingTime=f"{trigger_interval} seconds") \
        .foreachBatch(lambda df, id: upsert_daily_avg_batch(df, id, supabase)) \
        .option("checkpointLocation", f"{checkpoint_base}/stream2_daily_avg") \
        .queryName("daily_features_aggregation") \
        .start()
    
    print(f"-> Daily aggregation stream started (Update mode)")
    print()
    
    return daily_avg_query


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================

def main():
    """Main entry point for the MoodWave Spark Consumer"""
    
    # Load environment variables
    load_dotenv()
    print("Environment variables loaded")
    
    # Load configuration
    config = load_config()
    
    # Initialize Spark Session
    spark = init_spark_session()
    
    # Connect to Supabase
    supabase = create_supabase_client()
    
    # Create Kafka stream readers
    news_stream, charts_stream, features_stream = create_stream_readers(config, spark)
    
    # Parse JSON from Kafka messages
    news_parsed, charts_parsed, features_parsed = parse_kafka_streams(
        news_stream, charts_stream, features_stream
    )
    
    # =========================================================================
    # STREAM 1: Raw Data Storage (Append mode, no watermark)
    # =========================================================================
    raw_queries = start_raw_data_streams(
        news_parsed,
        charts_parsed,
        features_parsed,
        config,
        supabase
    )
    
    # =========================================================================
    # STREAM 2: Daily Feature Aggregations (Update mode with watermark)
    # =========================================================================
    # Note: We need a separate read from features topic for Stream 2
    # because we can't reuse the same stream with different output modes
    features_stream_2 = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", config['kafka']['bootstrap_servers']) \
        .option("subscribe", config['kafka']['features_topic']) \
        .option("startingOffsets", "earliest") \
        .load()
    
    features_parsed_2 = features_stream_2.select(
        from_json(col("value").cast("string"), get_features_schema()).alias("data")
    ).select("data.*")
    
    daily_avg_query = start_daily_aggregation_stream(
        features_parsed_2,
        config,
        supabase
    )
    
    # =========================================================================
    # AWAIT TERMINATION
    # =========================================================================
    print("=" * 70)
    print("ALL STREAMS STARTED - AWAITING DATA")
    print("=" * 70)
    print("Active streaming queries:")
    for query in spark.streams.active:
        print(f"  - {query.name}: {query.status}")
    print()
    print("Press Ctrl+C to stop the consumer")
    print("=" * 70 + "\n")
    
    # Wait for all streams (blocks until termination)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()