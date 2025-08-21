import json
import os
from kafka import KafkaConsumer, KafkaProducer
from shapely.geometry import shape, Point

# --- Configuration ---
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
RAW_TOPIC = "raw_flight_states"
ENRICHED_TOPIC = "enriched_flight_states"
GEOJSON_FILE_PATH = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'countries.geojson')

# --- Helper Functions ---

def load_countries(geojson_file):
    """
    Loads country polygons from a GeoJSON file into a list of (name, shape) tuples.
    """
    countries = []
    try:
        with open(geojson_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        for feature in data['features']:
            country_name = feature['properties']['ADMIN']
            geometry = feature['geometry']
            if geometry:
                countries.append((country_name, shape(geometry)))
        print(f"🌍 Successfully loaded {len(countries)} country polygons.")
    except Exception as e:
        print(f"❌ Error loading GeoJSON file: {e}")
    return countries

def find_country(lon, lat, countries):
    """
    Finds the country that contains the given longitude and latitude.
    """
    if lon is None or lat is None:
        return None
    point = Point(lon, lat)
    for name, country_shape in countries:
        if country_shape.contains(point):
            return name
    return "Over Ocean" # Default if no country is found

# --- Main Logic ---

# 1. Load the country shapes into memory
country_polygons = load_countries(GEOJSON_FILE_PATH)

# 2. Set up Kafka Consumer and Producer
try:
    consumer = KafkaConsumer(
        RAW_TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset='earliest', # Start reading from the beginning of the topic
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print("✅ Kafka connections established for Enricher.")
except Exception as e:
    print(f"❌ Could not connect to Kafka: {e}")
    exit()

# 3. Main processing loop
print("🚀 Enricher service started. Waiting for messages...")
for message in consumer:
    raw_data = message.value
    states = raw_data.get('states')
    
    if not states:
        continue

    enriched_states = []
    for state in states:
        # According to OpenSky API docs: lon is at index 5, lat is at index 6
        lon, lat = state[5], state[6]
        country = find_country(lon, lat, country_polygons)
        
        # Create a more readable dictionary structure
        enriched_state = {
            "icao24": state[0],
            "callsign": state[1].strip() if state[1] else None,
            "origin_country": state[2],
            "longitude": lon,
            "latitude": lat,
            "on_ground": state[8],
            "velocity": state[9],
            "true_track": state[10],
            "vertical_rate": state[11],
            "geo_altitude": state[13],
            "country": country # Our new enriched field
        }
        enriched_states.append(enriched_state)
    
    # Send the enriched data to the new topic
    producer.send(ENRICHED_TOPIC, value=enriched_states)
    print(f"✨ Enriched {len(enriched_states)} flight records. Added country: '{enriched_states[0]['country']}'")