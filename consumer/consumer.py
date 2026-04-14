from kafka import KafkaConsumer
import json
import psycopg2
import os
from dotenv import load_dotenv

# DB connection
conn = psycopg2.connect(
    os.getenv("DB_HOST"),
    database=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    port=5432
)
cursor = conn.cursor()

# Create table
cursor.execute("""
CREATE TABLE IF NOT EXISTS ticks (
    symbol TEXT,
    price FLOAT,
    timestamp DOUBLE PRECISION
)
""")
conn.commit()

print("Table ensured...")

# Kafka consumer
consumer = KafkaConsumer(
    'market_ticks',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("Consumer started...")

for message in consumer:
    data = message.value
    print("Received:", data)

    try:
        cursor.execute(
            "INSERT INTO ticks (symbol, price, timestamp) VALUES (%s, %s, %s)",
            (data['symbol'], data['price'], data['timestamp'])
        )
        conn.commit()
        print(f"Inserted {data['symbol']} into DB ✅")
    except Exception as e:
        print("DB ERROR:", e)
        # CRITICAL: This clears the 'aborted transaction' state
        conn.rollback()