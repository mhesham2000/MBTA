import httpx
import json
import time
from datetime import datetime, timezone
from confluent_kafka import Producer

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "route_traffic"  

producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

API_URL = "https://api-v3.mbta.com/predictions"
params = {
    "api_key": "47c878cabce54e2e8d395b7450cff77f",
    "include": "route,trip,stop,vehicle,alerts",
    "filter[route]": "15",
    "page[limit]": 1000
}

def fetch_predictions():
    try:
        with httpx.Client(timeout=10.0) as client:
            res = client.get(API_URL, params=params)
            res.raise_for_status()
            return res.json()
    except Exception as e:
        print("HTTPX Error:", str(e))
        return None

def flatten_predictions(raw):
    timestamp = datetime.now(timezone.utc).isoformat()
    flat = []

    for p in raw.get("data", []):
        attr = p.get("attributes", {})
        rel = p.get("relationships", {})

        def rel_object(rel_type):
            try:
                rel_id = rel[rel_type]["data"]["id"]
                rel_type_name = rel[rel_type]["data"]["type"]
                full_data = next(
                    (i for i in raw.get("included", []) if i["id"] == rel_id and i["type"] == rel_type_name),
                    None
                )
                return full_data["attributes"] | {"id": rel_id} if full_data else {"id": rel_id}
            except:
                return None

        flat.append({
            "prediction_id": p.get("id"),
            "arrival_time": attr.get("arrival_time"),
            "departure_time": attr.get("departure_time"),
            "stop_sequence": attr.get("stop_sequence"),
            "direction_id": attr.get("direction_id"),
            "route": rel_object("route"),
            "trip": rel_object("trip"),
            "stop": rel_object("stop"),
            "vehicle": rel_object("vehicle"),
            "timestamp": timestamp
        })

    return flat

# === MAIN ===
while True:
    try:
        for i in range(5):
            print(f"Try {i+1}")
            raw = fetch_predictions()
            if raw and raw.get("data"):
                flat = flatten_predictions(raw)
                print(f"Found {len(flat)} predictions")

                for record in flat:
                    producer.produce(KAFKA_TOPIC, json.dumps(record).encode("utf-8"))
                producer.flush()
                print(f"✅ Sent {len(flat)} records to Kafka topic '{KAFKA_TOPIC}'")
                time.sleep(10)

                break
            
        else:
            print("No predictions after retries")
    except Exception as e:
        print("Global error:", e)
        time.sleep(10)
