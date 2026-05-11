# NewsPulseInClassActivity
This is the inclass activity for "SE446 BigData Challenge News Pulse" - 11th May 2026/Monday

News Pulse

News Pulse is a real-time Big Data news monitoring system developed using Python, PySpark Structured Streaming, and Streamlit. The application continuously collects live RSS news feeds, processes them through Spark streaming pipelines, and visualizes the results on an interactive dashboard. The dashboard displays live analytics including news source distribution, headline activity over time, and the most frequent trending keywords. It also supports AI-generated summaries of current trending topics using an LLM API.

Pipeline Scalability Analysis

If the incoming data volume increased by 1000×, the first bottleneck would likely be the in-memory Spark aggregations, particularly the top_words and by_source queries. Since these aggregations use outputMode("complete"), Spark continuously stores and rewrites the entire aggregation state in memory, which could eventually overwhelm the driver and lead to memory exhaustion. To improve scalability, I would use event-time watermarks to remove outdated state data, replace complete mode with update or append mode where possible, and move the output from local memory sinks to scalable distributed storage solutions such as Kafka or Delta Lake.

<img width="908" height="326" alt="image" src="https://github.com/user-attachments/assets/ffd1e1a3-b558-4148-ba1e-443bbc991276" />

<img width="751" height="358" alt="image" src="https://github.com/user-attachments/assets/4064aa81-a4ab-4279-ad21-a83c645216da" />

<img width="584" height="229" alt="image" src="https://github.com/user-attachments/assets/7c42db72-68cc-4aa2-80a5-97bb0cd338df" />

<img width="446" height="373" alt="image" src="https://github.com/user-attachments/assets/4cbe4d48-2d5e-4a40-988a-e816ade13005" />

<img width="561" height="263" alt="image" src="https://github.com/user-attachments/assets/cc97c172-ee53-4351-b924-35ffb0056633" />

<img width="546" height="208" alt="image" src="https://github.com/user-attachments/assets/ed7cd93b-4fe8-41fe-9aaf-6c31c596aba2" />
