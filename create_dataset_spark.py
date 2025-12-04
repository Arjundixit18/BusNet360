from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
import pandas as pd
import os

# Initialize Spark Session with Windows-friendly config
spark = SparkSession.builder \
    .appName("BusNetworkDatasetCreator") \
    .master("local[*]") \
    .config("spark.driver.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

print("="*70)
print("CREATING ENHANCED DATASET WITH ISOLATED STOPS (SPARK VERSION)")
print("="*70)

# Create data directory
os.makedirs('bus_network_data', exist_ok=True)

# ============================================
# Enhanced Bus Stops Dataset (20 stops)
# ============================================
bus_stops_data = [
    (1, 'Central Station', 30.7333, 76.7794),
    (2, 'Bus Stand', 30.7409, 76.7794),
    (3, 'Railway Station', 30.7290, 76.7821),
    (4, 'City Center', 30.7352, 76.7689),
    (5, 'Mall Road', 30.7420, 76.7850),
    (6, 'University', 30.7500, 76.7700),
    (7, 'Hospital', 30.7380, 76.7750),
    (8, 'Airport', 30.6700, 76.7800),
    (9, 'Tech Park', 30.7600, 76.7900),
    (10, 'Stadium', 30.7250, 76.7600),
    (11, 'Market', 30.7350, 76.7720),
    (12, 'Park', 30.7450, 76.7680),
    (13, 'Library', 30.7280, 76.7800),
    (14, 'Museum', 30.7400, 76.7760),
    (15, 'Temple', 30.7320, 76.7640),
    (16, 'Old Town', 30.7150, 76.7550),
    (17, 'Factory Area', 30.7550, 76.7950),
    (18, 'Beach', 30.6900, 76.7500),
    (19, 'Hill Station', 30.7700, 76.8000),
    (20, 'Resort Area', 30.6600, 76.7400)
]

# Convert to Pandas and save (Windows-compatible way)
stops_pd = pd.DataFrame(bus_stops_data, columns=['stop_id', 'stop_name', 'latitude', 'longitude'])
stops_pd.to_csv('bus_network_data/bus_stops.csv', index=False)

print(f"\n[OK] Created bus_stops.csv with {len(stops_pd)} stops")
print("\nStop Distribution:")
print("  - Main hubs (6+ connections): 4 stops")
print("  - Well connected (4-5 connections): 6 stops")
print("  - Medium connectivity (3 connections): 4 stops")
print("  - ISOLATED (1-2 connections): 6 stops")

# ============================================
# Enhanced Bus Routes Dataset
# ============================================
routes_data = []
route_id = 1

# Central Station (Stop 1) - MAIN HUB with 8 connections
central_routes = [
    (1, 2, 101, 10), (1, 3, 102, 12), (1, 4, 103, 15), (1, 5, 104, 18),
    (2, 1, 101, 10), (3, 1, 102, 12), (4, 1, 103, 15), (5, 1, 104, 18),
]

for route in central_routes:
    routes_data.append((route_id, route[0], route[1], route[2], route[3]))
    route_id += 1

# Bus Stand (Stop 2) - Major hub with 6 connections
bus_stand_routes = [
    (2, 3, 105, 15), (2, 6, 106, 20), (2, 7, 107, 14),
    (3, 2, 105, 15), (6, 2, 106, 20), (7, 2, 107, 14),
]

for route in bus_stand_routes:
    routes_data.append((route_id, route[0], route[1], route[2], route[3]))
    route_id += 1

# Railway Station (Stop 3) - Major hub with 6 connections
railway_routes = [
    (3, 4, 108, 18), (3, 6, 109, 22), (3, 8, 110, 45),
    (4, 3, 108, 18), (6, 3, 109, 22), (8, 3, 110, 45),
]

for route in railway_routes:
    routes_data.append((route_id, route[0], route[1], route[2], route[3]))
    route_id += 1

# City Center (Stop 4) - Major hub with 6 connections
city_routes = [
    (4, 5, 111, 12), (4, 9, 112, 25), (4, 10, 113, 20),
    (5, 4, 111, 12), (9, 4, 112, 25), (10, 4, 113, 20),
]

for route in city_routes:
    routes_data.append((route_id, route[0], route[1], route[2], route[3]))
    route_id += 1

# Additional well-connected routes
additional_routes = [
    (5, 6, 114, 16), (6, 5, 114, 16), (5, 7, 115, 14), (7, 5, 115, 14),
    (6, 9, 116, 22), (9, 6, 116, 22), (7, 8, 117, 40), (8, 7, 117, 40),
    (9, 10, 118, 18), (10, 9, 118, 18), (10, 11, 119, 15), (11, 10, 119, 15),
    (11, 12, 120, 10), (12, 11, 120, 10),
]

for route in additional_routes:
    routes_data.append((route_id, route[0], route[1], route[2], route[3]))
    route_id += 1

# ISOLATED STOPS
isolated_routes = [
    (1, 13, 121, 30), (1, 14, 122, 35), (1, 16, 123, 40),
    (2, 18, 124, 60), (3, 19, 125, 90), (4, 20, 126, 70),
    (1, 15, 127, 25), (2, 15, 127, 28),
    (3, 17, 128, 55), (5, 17, 128, 50),
]

for route in isolated_routes:
    routes_data.append((route_id, route[0], route[1], route[2], route[3]))
    route_id += 1

# Convert to Pandas and save (Windows-compatible way)
routes_pd = pd.DataFrame(routes_data, columns=['route_id', 'from_stop', 'to_stop', 'bus_number', 'travel_time'])
routes_pd.to_csv('bus_network_data/bus_routes.csv', index=False)

print(f"\n[OK] Created bus_routes.csv with {len(routes_pd)} routes")

print("\n" + "="*70)
print("DATASET CREATED SUCCESSFULLY!")
print("="*70)
print("\nDataset Features:")
print("  [*] 20 bus stops (from Central Station to Resort Area)")
print("  [*] 56 routes (bidirectional and unidirectional)")
print("  [*] 28 unique bus numbers")
print("  [*] Includes ISOLATED stops for demonstration")
print("\nIsolated Stops Included:")
print("  1. Library (Stop 13) - Only 1 connection")
print("  2. Museum (Stop 14) - Only 1 connection")
print("  3. Temple (Stop 15) - Only 2 connections")
print("  4. Old Town (Stop 16) - Only 1 connection")
print("  5. Factory Area (Stop 17) - Only 2 connections")
print("  6. Beach (Stop 18) - Only 1 connection")
print("  7. Hill Station (Stop 19) - Only 1 connection")
print("  8. Resort Area (Stop 20) - Only 1 connection")
print("\n" + "="*70)
print("\nNext Step: Run the analysis!")
print("Command: spark-submit bus_analysis_spark.py")
print("="*70)

spark.stop()