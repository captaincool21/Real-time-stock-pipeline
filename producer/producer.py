import websocket
import json
import psycopg2
import requests
import threading
import time
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
from kafka import KafkaProducer

# ==============================
# 🔐 CONFIG
# ==============================
API_KEY = os.getenv("ALPACA_API_KEY")
SECRET_KEY = os.getenv("ALPACA_SECRET_KEY")

# Kafka Config
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# ==============================
# 📂 DUMMY DATA MAPPING (20 Stocks)
# ==============================
STOCK_MAP = {
    "google": "GOOGL", "alphabet": "GOOGL",
    "apple": "AAPL", "iphone": "AAPL",
    "tesla": "TSLA", "elon": "TSLA",
    "microsoft": "MSFT", "windows": "MSFT",
    "nvidia": "NVDA", "ai": "NVDA",
    "amazon": "AMZN", "netflix": "NFLX",
    "meta": "META", "facebook": "META",
    "disney": "DIS", "starbucks": "SBUX",
    "visa": "V", "nike": "NKE",
    "amd": "AMD", "adobe": "ADBE",
    "palantir": "PLTR", "berkshire": "BRK.B"
}

def get_ticker_locally(user_query):
    query = user_query.lower().strip()
    # Check if user typed the name (e.g., "Google")
    if query in STOCK_MAP:
        return STOCK_MAP[query]
    # If not in map, assume they typed the ticker directly (e.g., "AAPL")
    return query.upper()

# ==============================
# 📊 HISTORICAL DATA FETCH (With Error Handling)
# ==============================
def fetch_historical_data(symbol):
    print(f"⏳ Fetching 1-year history for {symbol}...")
    url = f"https://data.alpaca.markets/v2/stocks/bars"
    headers = {"APCA-API-KEY-ID": API_KEY, "APCA-API-SECRET-KEY": SECRET_KEY}
    
    params = {
        "symbols": symbol,
        "timeframe": "1Day",
        "start": (datetime.now() - timedelta(days=365)).isoformat() + "Z",
        "limit": 1000
    }
    
    try:
        resp = requests.get(url, headers=headers, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        bars = data.get('bars', {}).get(symbol, [])
        
        for bar in bars:
            kafka_data = {
                "symbol": symbol, "price": bar['c'],
                "timestamp": bar['t'], "type": "historical"
            }
            producer.send('market_ticks', kafka_data)
        print(f"✅ Sent {len(bars)} historical records to Kafka.")
    except Exception as e:
        print(f"❌ Alpaca History Error: {e}")

# ==============================
# 🔌 DYNAMIC WEBSOCKET CLASS
# ==============================
class AlpacaStreamer:
    def __init__(self):
        self.ws = None
        self.current_subscriptions = set()
        self.authenticated = False

    def on_message(self, ws, message):
        data = json.loads(message)
        if isinstance(data, list) and data[0].get("msg") == "authenticated":
            self.authenticated = True
            print("✅ Alpaca Authenticated")
            self.update_subscription()
            
        for trade in data:
            if trade.get("T") == "t":
                kafka_data = {
                    "symbol": trade["S"], "price": trade["p"],
                    "timestamp": trade["t"], "type": "live"
                }
                producer.send('market_ticks', kafka_data)
                print(f"📤 Sent Live: {trade['S']} @ {trade['p']}")

    def on_open(self, ws):
        print("🔌 Websocket Connected")
        auth = {"action": "auth", "key": API_KEY, "secret": SECRET_KEY}
        ws.send(json.dumps(auth))

    def update_subscription(self):
        if not self.authenticated or not self.ws: return
        try:
            conn = psycopg2.connect(host="localhost", database="market_data", user="postgres", password="password")
            cur = conn.cursor()
            cur.execute("SELECT symbol FROM selected_stocks")
            db_symbols = set(row[0] for row in cur.fetchall())
            conn.close()

            if db_symbols != self.current_subscriptions:
                sub = {"action": "subscribe", "trades": list(db_symbols)}
                self.ws.send(json.dumps(sub))
                self.current_subscriptions = db_symbols
                print(f"📡 Now streaming: {self.current_subscriptions}")
        except Exception as e:
            print(f"❌ DB Sync Error: {e}")

    def start(self):
        self.ws = websocket.WebSocketApp(
            "wss://stream.data.alpaca.markets/v2/iex",
            on_open=self.on_open, on_message=self.on_message
        )
        self.ws.run_forever()

# ==============================
# 🚀 MAIN LOOP
# ==============================
streamer = AlpacaStreamer()
threading.Thread(target=streamer.start, daemon=True).start()

print("--- Market Pipeline Active (Dummy Map Mode) ---")
while True:
    query = input("\nEnter stock name (e.g. 'Google') or Ticker (e.g. 'AAPL'): ")
    if query.lower() == 'exit': break
    
    ticker = get_ticker_locally(query)

    # 1. Save to DB
    try:
        conn = psycopg2.connect(host="localhost", database="market_data", user="postgres", password="password")
        cur = conn.cursor()
        cur.execute("INSERT INTO selected_stocks (symbol) VALUES (%s) ON CONFLICT DO NOTHING", (ticker,))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"❌ DB Error: {e}")

    # 2. Historical & Live Update
    fetch_historical_data(ticker)
    streamer.update_subscription()