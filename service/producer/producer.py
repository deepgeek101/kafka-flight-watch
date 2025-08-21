import os
import time
import requests
import json
from kafka import KafkaProducer

# --- Configuration ---
# You can get bounding box values from sites like https://boundingbox.latlong.net/
# Example: Bhutan and surrounding area
BHUTAN_BBOX = "lamin=26.7&lomin=88.7&lamax=28.3&lomax=92.1"
OPENSKY_API_URL = f"https://opensky-network.org/api/states/all?{BHUTAN_BBOX}"
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = "raw_flight_states"
SLEEP_TIME_SECONDS = 30 # Time to wait between API calls

# --- Kafka Producer Setup ---
try:
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print("✅ Kafka Producer connected successfully.")
except Exception as e:
    print(f"❌ Could not connect to Kafka Producer: {e}")
    exit()

# --- Main Loop ---
print(f"📡 Starting to fetch data from OpenSky for topic '{KAFKA_TOPIC}'...")
while True:
    try:
        # 1. Fetch data from the OpenSky Network API
        response = requests.get(OPENSKY_API_URL)
        response.raise_for_status() # Raise an exception for bad status codes (4xx or 5xx)

        data = response.json()
        
        if data['states']:
            print(f"✈️  Found {len(data['states'])} aircraft. Sending to Kafka...")
            # 2. Send the entire list of states to the Kafka topic
            producer.send(KAFKA_TOPIC, value=data)
            producer.flush() # Ensure all messages are sent
        else:
            print("⚪ No aircraft found in the specified area.")

    except requests.exceptions.RequestException as e:
        print(f"❌ Error fetching data from OpenSky API: {e}")
    except Exception as e:
        print(f"❌ An unexpected error occurred: {e}")

    # Wait before the next API call
    print(f"Sleeping for {SLEEP_TIME_SECONDS} seconds...")
    time.sleep(SLEEP_TIME_SECONDS)