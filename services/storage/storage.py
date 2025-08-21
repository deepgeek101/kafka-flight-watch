import os
import json
import time
import psycopg2
from kafka import KafkaConsumer

# --- Configuration ---
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
ENRICHED_TOPIC = "enriched_flight_states"

# --- Database Configuration ---
DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_NAME = os.environ.get("DB_NAME", "flightdb")
DB_USER = os.environ.get("DB_USER", "flightuser")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "flightpassword")

def get_db_connection():
    """
    Connects to the PostgreSQL database. Retries a few times if the DB is not ready.
    """
    retries = 5
    while retries > 0:
        try:
            conn = psycopg2.connect(
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
                host=DB_HOST
            )
            print("✅ Database connection successful.")
            return conn
        except psycopg2.OperationalError as e:
            print(f"❌ Could not connect to database: {e}. Retrying...")
            retries -= 1
            time.sleep(5)
    return None

def setup_database(conn):
    """
    Ensures the flights table exists.
    """
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS flights (
                id SERIAL PRIMARY KEY,
                icao24 VARCHAR(24) NOT NULL,
                callsign VARCHAR(24),
                origin_country VARCHAR(255),
                longitude FLOAT,
                latitude FLOAT,
                on_ground BOOLEAN,
                velocity FLOAT,
                true_track FLOAT,
                vertical_rate FLOAT,
                geo_altitude FLOAT,
                country VARCHAR(255),
                recorded_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            );
        """)
        conn.commit()
    print("📖 Flights table is ready.")

def main():
    """
    Main function to consume from Kafka and write to PostgreSQL.
    """
    # Connect to Database
    conn = get_db_connection()
    if not conn:
        print("❌ Could not establish database connection after multiple retries. Exiting.")
        return
    
    # Create table if it doesn't exist
    setup_database(conn)

    # Connect to Kafka
    consumer = KafkaConsumer(
        ENRICHED_TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    print("🚀 Storage service started. Waiting for messages...")

    # Main loop
    with conn.cursor() as cur:
        for message in consumer:
            enriched_flights = message.value
            for flight in enriched_flights:
                try:
                    cur.execute("""
                        INSERT INTO flights (icao24, callsign, origin_country, longitude, latitude, on_ground, velocity, true_track, vertical_rate, geo_altitude, country)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        flight.get('icao24'), flight.get('callsign'), flight.get('origin_country'),
                        flight.get('longitude'), flight.get('latitude'), flight.get('on_ground'),
                        flight.get('velocity'), flight.get('true_track'), flight.get('vertical_rate'),
                        flight.get('geo_altitude'), flight.get('country')
                    ))
                    print(f"💾 Inserted flight {flight.get('icao24')} into DB.")
                except Exception as e:
                    print(f"❌ Error inserting flight data: {e}")
            conn.commit()

if __name__ == "__main__":
    main()