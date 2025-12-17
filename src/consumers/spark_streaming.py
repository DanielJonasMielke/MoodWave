from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import yaml


class SparkStreamConsumer:
    def __init__(self):
        # Load config
        with open("config.yaml", "r") as f:
            config = yaml.safe_load(f)

        # Initialize Spark Session
        self.spark = SparkSession.builder \
            .appName("TrackMetricsConsumer") \
            .config("spark.sql.streaming.schemaInference", "true") \
            .getOrCreate()

        self.spark.sparkContext.setLogLevel("WARN")

        # Kafka config
        self.kafka_bootstrap = config["kafka"]["bootstrap_servers"]
        self.charts_topic = config["kafka"]["charts_topic"]
        self.features_topic = config["kafka"]["features_topic"]

        print("Spark Session initialized")

    def calculate_metrics(self, df, epoch_id):
        """
        This function is called when a window closes.
        For now, just pass - we'll implement metrics calculation later.
        """
        # fetch list of existing songs in the database of current charts list
        # combine this with song features list of the current window
        # calculate average features for current charts list

        # also add news tone data and build correlations
        # additional metrics might be: 
        # average energy of top n songs etc... -> should this be done in this consumer or in seperate spark job?
        # songstreaming 
        pass

    def run(self):
        """
        Main streaming logic with watermarks
        """
        print("Starting Spark Streaming Consumer...")
        print(f"Reading from topics: {self.charts_topic}, {self.features_topic}")

        # Define schema for charts data
        charts_schema = StructType([
            StructField("chart_date", StringType(), True),
            StructField("chart_type", StringType(), True),
            StructField("tracks", ArrayType(StructType([
                StructField("rank", IntegerType(), True),
                StructField("previous_rank", IntegerType(), True),
                StructField("peak_rank", IntegerType(), True),
                StructField("peak_date", StringType(), True),
                StructField("appear_on_chart", IntegerType(), True),
                StructField("track_name", StringType(), True),
                StructField("track_id", StringType(), True),
                StructField("track_uri", StringType(), True),
                StructField("artists", StringType(), True),
                StructField("artist_uris", StringType(), True),
                StructField("release_date", StringType(), True),
                StructField("chart_date", StringType(), True),
                StructField("chart_type", StringType(), True)
            ])), True),
            StructField("timestamp", StringType(), True)
        ])

        # Define schema for features data
        features_schema = StructType([
            StructField("features", ArrayType(StructType([
                StructField("track_id", StringType(), True),
                StructField("track_name", StringType(), True),
                StructField("artists", StringType(), True),
                StructField("track_uri", StringType(), True),
                StructField("release_date", StringType(), True),
                StructField("tempo", DoubleType(), True),
                StructField("duration", IntegerType(), True),
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
                StructField("camelot", StringType(), True)
            ])), True),
            StructField("count", IntegerType(), True),
            StructField("timestamp", StringType(), True)
        ])

        # Read charts stream from Kafka
        charts_stream = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_bootstrap) \
            .option("subscribe", self.charts_topic) \
            .option("startingOffsets", "earliest") \
            .load()

        # Parse JSON and add event time
        charts_parsed = charts_stream \
            .select(from_json(col("value").cast("string"), charts_schema).alias("data")) \
            .select(
                col("data.chart_date").alias("chart_date"),
                col("data.chart_type").alias("chart_type"),
                col("data.tracks").alias("tracks"),
                to_timestamp(col("data.timestamp")).alias("event_time")
            ) \
            .withWatermark("event_time", "10 minutes")

        # Read features stream from Kafka
        features_stream = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_bootstrap) \
            .option("subscribe", self.features_topic) \
            .option("startingOffsets", "earliest") \
            .load()

        # Parse JSON and add event time
        features_parsed = features_stream \
            .select(from_json(col("value").cast("string"), features_schema).alias("data")) \
            .select(
                col("data.features").alias("features"),
                col("data.count").alias("feature_count"),
                to_timestamp(col("data.timestamp")).alias("event_time")
            ) \
            .withWatermark("event_time", "10 minutes")

        # Group by window and chart_date
        # When window closes, the data will be processed
        windowed_charts = charts_parsed \
            .groupBy(
                window(col("event_time"), "1 day"),
                col("chart_date")
            ) \
            .agg(
                first("tracks").alias("tracks"),
                first("chart_type").alias("chart_type")
            )

        windowed_features = features_parsed \
            .groupBy(
                window(col("event_time"), "1 day")
            ) \
            .agg(
                collect_list("features").alias("all_features"),
                sum("feature_count").alias("total_features")
            )

        # Start the streaming query with foreachBatch
        # This calls calculate_metrics when each window closes
        query = windowed_charts \
            .writeStream \
            .outputMode("update") \
            .foreachBatch(self.calculate_metrics) \
            .start()

        print("Streaming query started. Waiting for data...")
        print("Watermark: 10 minutes after each event")
        
        # Keep the stream running
        query.awaitTermination()


if __name__ == "__main__":
    consumer = SparkStreamConsumer()
    consumer.run()