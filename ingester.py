import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, lower, split, explode, length
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

def main():
    # 1. Create SparkSession
    spark = (
        SparkSession.builder
        .appName("NewsPulse")
        .master("local[*]")
        .getOrCreate()
    )
    # Set log level to ERROR to reduce console noise
    spark.sparkContext.setLogLevel("ERROR")
    
    # 2. Define schema for incoming JSON data
    schema = StructType([
        StructField("source", StringType(), True),
        StructField("title", StringType(), True),
        StructField("url", StringType(), True),
        StructField("ts", TimestampType(), True)
    ])
    
    # Ensure directory exists
    incoming_dir = os.path.join(os.path.dirname(__file__), "data", "incoming")
    if not os.path.exists(incoming_dir):
        os.makedirs(incoming_dir)
        
    # 3. Read stream
    df = spark.readStream.schema(schema).json(incoming_dir)
    
    # ==================================================
    # AGGREGATION 1 - by_source
    # ==================================================
    agg_by_source = df.groupBy("source").count()
    
    query_by_source = (
        agg_by_source.writeStream
        .outputMode("complete")
        .format("memory")
        .queryName("by_source")
        .start()
    )
    
    # ==================================================
    # AGGREGATION 2 - by_window
    # ==================================================
    df_watermarked = df.withWatermark("ts", "2 hours")
    agg_by_window = df_watermarked.groupBy(window("ts", "1 hour")).count()
    
    query_by_window = (
        agg_by_window.writeStream
        .outputMode("complete")
        .format("memory")
        .queryName("by_window")
        .start()
    )
    
    # ==================================================
    # AGGREGATION 3 - top_words
    # ==================================================
    stop_words = ["the","a","an","and","or","to","of","in","on","for","with","is","are","at","from"]
    
    # Process the title: lowercase, extract words, and explode into rows
    # We use a simple regex to split on non-alphabetic characters
    words_df = df.select(
        explode(split(lower(col("title")), "[^a-z]+")).alias("word")
    )
    
    # Filter out short words and stop words
    filtered_words = words_df.filter(
        (length(col("word")) >= 3) & 
        (~col("word").isin(stop_words))
    )
    
    # Count word occurrences
    agg_top_words = filtered_words.groupBy("word").count()
    
    query_top_words = (
        agg_top_words.writeStream
        .outputMode("complete")
        .format("memory")
        .queryName("top_words")
        .start()
    )
    
    print("🚀 Streaming Job Started!")
    print("Waiting for data in data/incoming...")
    
    # Wait for termination
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
