import time
from newsapi import NewsApiClient
from kafka import KafkaProducer
import json
 
# List of API keys to handle rate limits
API_KEYS = [
    "key1",
    "key2",
    "key3",
    "key-n"]
 
# Track current API key index
current_api_index = 0
newsapi = NewsApiClient(api_key=API_KEYS[current_api_index])
 
# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)
 
# Switch API key if rate limited
def switch_api_key():
    global current_api_index, newsapi
    current_api_index = (current_api_index + 1) % len(API_KEYS)
    print(f"\n[INFO] Switching to API Key: {API_KEYS[current_api_index]}")
    newsapi = NewsApiClient(api_key=API_KEYS[current_api_index])
 
# Fetch and send articles
def fetch_news():
    try:
        response = newsapi.get_everything(
            q="technology OR finance OR AI OR crypto OR climate",
            language="en",
            sort_by="publishedAt",
            page_size=100
        )
 
        if response.get("status") == "error" and "maximum" in response.get("message", "").lower():
            print("[WARN] Rate limit reached. Switching API key.")
            switch_api_key()
            return fetch_news()
 
        articles = response.get("articles", [])
        print(f"\n[INFO] Fetched {len(articles)} articles.")
 
        for article in articles:
            send_to_kafka(article)
            print(f"[SENT] {article.get('publishedAt')} | {article.get('source', {}).get('name')} | {article.get('title')}")
 
    except Exception as e:
        print(f"[ERROR] Error fetching articles: {e}")
 
# Send to Kafka
def send_to_kafka(article):
    producer.send('news-stream', article)
    producer.flush()
 
# Loop every 60 seconds
while True:
    fetch_news()
    time.sleep(60)
 