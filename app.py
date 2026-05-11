import os
import streamlit as st
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, lower, split, explode, length
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from streamlit_autorefresh import st_autorefresh
from openai import OpenAI

# ----- HADOOP FIX FOR WINDOWS -----
hadoop_dir = os.path.join(os.path.dirname(__file__), "hadoop")
os.environ["HADOOP_HOME"] = hadoop_dir
os.environ["PATH"] += os.pathsep + os.path.join(hadoop_dir, "bin")
# ----------------------------------

# 1. AUTO REFRESH
# Refresh the dashboard every 10 seconds (10000 ms)
st_autorefresh(interval=10000, key="news_dashboard_refresh")

# 2. CONNECT TO SPARK AND START STREAMING
@st.cache_resource
def init_spark_and_streams():
    spark = (
        SparkSession.builder
        .appName("NewsPulse")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    
    # Check if streams are already active
    if len(spark.streams.active) == 0:
        schema = StructType([
            StructField("source", StringType(), True),
            StructField("title", StringType(), True),
            StructField("url", StringType(), True),
            StructField("ts", TimestampType(), True)
        ])
        
        incoming_dir = os.path.join(os.path.dirname(__file__), "data", "incoming")
        if not os.path.exists(incoming_dir):
            os.makedirs(incoming_dir)
            
        df = spark.readStream.schema(schema).option("timestampFormat", "yyyy-MM-dd HH:mm:ss").json(incoming_dir)
        
        # AGGREGATION 1 - by_source
        agg_by_source = df.groupBy("source").count()
        agg_by_source.writeStream.outputMode("complete").format("memory").queryName("by_source").start()
        
        # AGGREGATION 2 - by_window
        df_watermarked = df.withWatermark("ts", "2 hours")
        agg_by_window = df_watermarked.groupBy(window("ts", "1 hour")).count()
        agg_by_window.writeStream.outputMode("complete").format("memory").queryName("by_window").start()
        
        # AGGREGATION 3 - top_words
        stop_words = ["the","a","an","and","or","to","of","in","on","for","with","is","are","at","from"]
        words_df = df.select(explode(split(lower(col("title")), "[^a-z]+")).alias("word"))
        filtered_words = words_df.filter((length(col("word")) >= 3) & (~col("word").isin(stop_words)))
        agg_top_words = filtered_words.groupBy("word").count()
        agg_top_words.writeStream.outputMode("complete").format("memory").queryName("top_words").start()
        
    return spark

spark = init_spark_and_streams()

# 3. GROK API INTEGRATION
def generate_summary(keywords):
    try:
        api_key = os.getenv("GROQ_API_KEY", "your_api_key_here")
        if not api_key or api_key == "your_api_key_here":
            raise ValueError("API key not set. Please add it to the code.")
            
        client = OpenAI(
            api_key=api_key,
            base_url="https://api.groq.com/openai/v1"
        )
        
        prompt = f"Summarize these trending news keywords in under 80 words and mention at least 3 major storylines. Keywords: {', '.join(keywords)}"
        
        response = client.chat.completions.create(
            model="llama3-8b-8192",
            messages=[
                {"role": "system", "content": "You are a concise AI news summarizer."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=150
        )
        return response.choices[0].message.content
        
    except Exception as e:
        # Fallback if API fails (never crash)
        fallback = ", ".join(keywords[:5]) if keywords else "None"
        return f"Trending topics: {fallback}..."

# 4. STREAMLIT LAYOUT
st.set_page_config(page_title="News Pulse", layout="wide")
st.title("📰 News Pulse - Live Dashboard")

# Helper function to query memory tables safely
def get_table_data(table_name):
    try:
        # Try to query the memory sink
        return spark.sql(f"SELECT * FROM {table_name}").toPandas()
    except Exception:
        # Return empty dataframe if table doesn't exist yet
        return pd.DataFrame()

# Layout Structure: 2 columns for charts, 2 columns for tables & summary
row1_col1, row1_col2 = st.columns(2)

# ==================================================
# Source Distribution (Bar Chart)
# ==================================================
with row1_col1:
    st.subheader("Source Distribution")
    df_source = get_table_data("by_source")
    
    if not df_source.empty:
        # Streamlit bar_chart expects index to be the category
        df_chart = df_source.set_index("source")
        st.bar_chart(df_chart["count"])
    else:
        st.info("Waiting for data in 'by_source'...")

# ==================================================
# News Volume Over Time (Line Chart)
# ==================================================
with row1_col2:
    st.subheader("News Volume Over Time")
    df_window = get_table_data("by_window")
    
    if not df_window.empty:
        # Extract the start time from the window struct
        df_window["time"] = df_window["window"].apply(lambda w: w["start"])
        # Sort chronologically and set time as index
        df_chart = df_window.sort_values("time").set_index("time")
        st.line_chart(df_chart["count"])
    else:
        st.info("Waiting for data in 'by_window'...")

st.divider()

row2_col1, row2_col2 = st.columns(2)

# ==================================================
# Top Keywords (Table)
# ==================================================
top_keywords_list = []
with row2_col1:
    st.subheader("Top Keywords")
    df_words = get_table_data("top_words")
    
    if not df_words.empty:
        # Order by count descending
        df_words = df_words.sort_values("count", ascending=False).head(15)
        # Reset index to make the table look cleaner
        st.table(df_words.reset_index(drop=True))
        # Save keywords for the Grok summary
        top_keywords_list = df_words["word"].tolist()
    else:
        st.info("Waiting for data in 'top_words'...")

# ==================================================
# AI Summary (Grok API)
# ==================================================
with row2_col2:
    st.subheader("🤖 AI Summary")
    if top_keywords_list:
        summary = generate_summary(top_keywords_list)
        st.write(summary)
    else:
        st.info("Waiting for keywords to generate summary...")
