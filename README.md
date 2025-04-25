# Streaming-News-Insights-From-Ingestion-to-Cloud-based-Visualization
This project demonstrates a real-time ETL (Extract, Transform, Load) pipeline for ingesting, processing, storing, and analyzing live news data using modern big data and NLP tools.

the pipeline integrates:

Apache Kafka for real-time ingestion

Apache Spark Structured Streaming for transformation

Google Cloud Storage (GCS) for raw and aggregated data storage

Vertex AI Workbench for NLP analytics and visualization

## Tech Stack

* Ingestion: Kafka Producer with Python + NewsAPI

* Streaming: Apache Kafka & Spark Structured Streaming

* Cloud Storage: Google Cloud Storage (GCS)

* Analytics: Vertex AI Workbench (Python3 - Jupyter Notebook)

* Visualization: Matplotlib, Seaborn

* NLP: spaCy (NER), TextBlob (Sentiment Analysis)

## Pipeline Components


### 1. Real-Time Ingestion with Kafka

Python script fetches news articles via NewsAPI

Articles are transformed into structured JSON format

Sent to news-stream Kafka topic for downstream processing

### 2. Parallel Spark Jobs

* Job 1 – Aggregation: Counts articles by source per 2-minute window, writes to Parquet files

* Job 2 – Raw Storage: Stores unprocessed articles in GCS for analysis

### 3. NLP in Vertex AI Notebooks

Analyzed news titles for:

Sentiment (Positive/Neutral/Negative) using TextBlob

Named Entity Recognition (Persons, Locations, Orgs) using spaCy

Generated:

Word clouds

Sentiment bar charts

Top entity visualizations
