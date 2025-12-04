from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, max as spark_max, min as spark_min
import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt
import warnings
warnings.filterwarnings('ignore')

print("="*70)
print("BUS NETWORK ANALYSIS - SPARK VERSION")
print("="*70)

# Initialize Spark Session with Windows-friendly config
spark = SparkSession.builder \
    .appName("BusNetworkAnalysis") \
    .master("local[*]") \
    .config("spark.driver.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

# ============================================
# TASK 1: LOAD DATASETS USING SPARK
# ============================================
print("\n[TASK 1/7] Loading bus data with Spark...")
stops_df_spark = spark.read.option("header", "true").option("inferSchema", "true").csv("bus_network_data/bus_stops.csv")
routes_df_spark = spark.read.option("header", "true").option("inferSchema", "true").csv("bus_network_data/bus_routes.csv")

print(f"   [OK] Loaded {stops_df_spark.count()} bus stops")
print(f"   [OK] Loaded {routes_df_spark.count()} bus routes")

print("\n   Sample Bus Stops (first 5):")
stops_df_spark.show(5, truncate=False)

print("\n   Sample Bus Routes (first 5):")
routes_df_spark.show(5, truncate=False)

# Convert to Pandas for NetworkX processing
stops_df = stops_df_spark.toPandas()
routes_df = routes_df_spark.toPandas()

# ============================================
# TASK 2: BUILD DIRECTED GRAPH
# ============================================
print("\n[TASK 2/7] Building directed graph...")

# Create NetworkX directed graph
G = nx.DiGraph()

# Add nodes (bus stops)
for _, row in stops_df.iterrows():
    G.add_node(row['stop_id'], 
               name=row['stop_name'],
               lat=row['latitude'],
               lon=row['longitude'])

# Add edges (routes)
for _, row in routes_df.iterrows():
    G.add_edge(row['from_stop'], 
               row['to_stop'], 
               bus=row['bus_number'],
               time=row['travel_time'])

print(f"   [OK] Directed graph constructed:")
print(f"     - Nodes (bus stops): {G.number_of_nodes()}")
print(f"     - Edges (direct routes): {G.number_of_edges()}")
print(f"     - Graph type: Directed")

# ============================================
# TASK 3: COMPUTE CONNECTIONS USING SPARK
# ============================================
print("\n[TASK 3/7] Computing connectivity metrics using Spark...")

# Count outgoing connections (from_stop)
outgoing = routes_df_spark.groupBy("from_stop").agg(count("*").alias("outgoing"))

# Count incoming connections (to_stop)
incoming = routes_df_spark.groupBy("to_stop").agg(count("*").alias("incoming"))

# Get all stops and join with connection counts
all_stops = stops_df_spark.select("stop_id", "stop_name")
connections_spark = all_stops \
    .join(outgoing, all_stops.stop_id == outgoing.from_stop, "left") \
    .join(incoming, all_stops.stop_id == incoming.to_stop, "left") \
    .select(
        all_stops.stop_id,
        all_stops.stop_name,
        col("outgoing").cast("int"),
        col("incoming").cast("int")
    ) \
    .na.fill(0, ["outgoing", "incoming"])

# Add total connections
connections_spark = connections_spark.withColumn(
    "total_connections", 
    col("outgoing") + col("incoming")
)

# Cache for reuse
connections_spark.cache()

# Calculate statistics using Spark
stats = connections_spark.agg(
    avg("total_connections").alias("avg_degree"),
    spark_max("total_connections").alias("max_degree"),
    spark_min("total_connections").alias("min_degree")
).collect()[0]

avg_degree = stats['avg_degree']
max_degree = stats['max_degree']
min_degree = stats['min_degree']

print(f"   [OK] Connectivity computed for all stops")
print(f"     - Average degree: {avg_degree:.2f}")
print(f"     - Maximum degree: {max_degree}")
print(f"     - Minimum degree: {min_degree}")

# Sort by total connections
connections_sorted = connections_spark.orderBy(col("total_connections").desc())

print("\n   Full Connectivity Rankings:")
connections_sorted.show(20, truncate=False)

# Convert to Pandas for detailed processing
connections = connections_sorted.toPandas()

# ============================================
# TASK 4: FIND MOST CONNECTED BUS STOP (MAIN HUB)
# ============================================
print("\n[TASK 4/7] Identifying MAIN HUB (Most Connected Stop)...")

main_hub = connections.iloc[0]

print("\n" + "="*70)
print(f"*** MAIN HUB IDENTIFIED: {main_hub['stop_name']} ***")
print(f"   Stop ID: {main_hub['stop_id']}")
print(f"   Outgoing Routes: {main_hub['outgoing']}")
print(f"   Incoming Routes: {main_hub['incoming']}")
print(f"   Total Connections: {main_hub['total_connections']}")
print(f"   Status: Primary Transport Interchange")
print("="*70)

# Show top 5 hubs
print("\n   Top 5 Most Connected Stops:")
for i, (_, stop) in enumerate(connections.head(5).iterrows(), 1):
    print(f"   {i}. {stop['stop_name']}: {stop['total_connections']} connections")

# ============================================
# TASK 5: IDENTIFY LEAST ACCESSIBLE STOPS
# ============================================
print("\n[TASK 5/7] Identifying stops with low connectivity...")

print("   Accessibility Criteria:")
print("   - Isolated: 0-2 connections (CRITICAL)")
print("   - Low: 3 connections (CONCERN)")
print("   - Medium: 4-5 connections (ACCEPTABLE)")
print("   - High: 6+ connections (EXCELLENT)")

isolated = connections[connections['total_connections'] <= 2]
low_access = connections[connections['total_connections'] == 3]
medium_access = connections[(connections['total_connections'] >= 4) & (connections['total_connections'] <= 5)]
high_access = connections[connections['total_connections'] >= 6]

print(f"\n   *** ISOLATED STOPS (0-2 connections): {len(isolated)} ***")
if len(isolated) > 0:
    print(isolated[['stop_name', 'total_connections']].to_string(index=False))
    print("   [!] CRITICAL: These stops need immediate attention!")
else:
    print("   None found - Excellent network coverage!")

print(f"\n   *** LOW ACCESSIBILITY (3 connections): {len(low_access)} ***")
if len(low_access) > 0:
    print(low_access[['stop_name', 'total_connections']].to_string(index=False))
    print("   Recommendation: Consider adding routes to improve connectivity")
else:
    print("   None found")

print(f"\n   Summary:")
print(f"   - High Accessibility: {len(high_access)} stops")
print(f"   - Medium Accessibility: {len(medium_access)} stops")
print(f"   - Low Accessibility: {len(low_access)} stops")
print(f"   - Isolated: {len(isolated)} stops")

# ============================================
# TASK 6: ADDITIONAL NETWORK ANALYTICS WITH SPARK
# ============================================
print("\n[TASK 6/7] Computing additional network metrics using Spark...")

total_stops = stops_df_spark.count()
total_routes = routes_df_spark.count()

# Travel time statistics using Spark
travel_stats = routes_df_spark.agg(
    avg("travel_time").alias("avg_travel"),
    spark_max("travel_time").alias("max_travel"),
    spark_min("travel_time").alias("min_travel")
).collect()[0]

avg_travel = travel_stats['avg_travel']
max_travel = travel_stats['max_travel']
min_travel = travel_stats['min_travel']

unique_buses = routes_df_spark.select("bus_number").distinct().count()
network_density = (total_routes / (total_stops * (total_stops - 1))) * 100

print(f"\n   NETWORK STATISTICS:")
print(f"   - Total Bus Stops: {total_stops}")
print(f"   - Total Routes: {total_routes}")
print(f"   - Unique Bus Numbers: {unique_buses}")
print(f"   - Average Travel Time: {avg_travel:.2f} minutes")
print(f"   - Min Travel Time: {min_travel} minutes")
print(f"   - Max Travel Time: {max_travel} minutes")
print(f"   - Network Density: {network_density:.2f}%")

# Top 3 longest routes
print(f"\n   TOP 3 LONGEST ROUTES (by travel time):")
longest_routes = routes_df.nlargest(3, 'travel_time')
for i, (_, route) in enumerate(longest_routes.iterrows(), 1):
    from_name = stops_df[stops_df['stop_id'] == route['from_stop']]['stop_name'].values[0]
    to_name = stops_df[stops_df['stop_id'] == route['to_stop']]['stop_name'].values[0]
    print(f"   {i}. Bus {route['bus_number']}: {from_name} -> {to_name} ({route['travel_time']} min)")

# Top 3 shortest routes
print(f"\n   TOP 3 SHORTEST ROUTES (by travel time):")
shortest_routes = routes_df.nsmallest(3, 'travel_time')
for i, (_, route) in enumerate(shortest_routes.iterrows(), 1):
    from_name = stops_df[stops_df['stop_id'] == route['from_stop']]['stop_name'].values[0]
    to_name = stops_df[stops_df['stop_id'] == route['to_stop']]['stop_name'].values[0]
    print(f"   {i}. Bus {route['bus_number']}: {from_name} -> {to_name} ({route['travel_time']} min)")

# Bus frequency analysis with Spark
print(f"\n   BUS FREQUENCY ANALYSIS:")
bus_frequency_spark = routes_df_spark.groupBy("bus_number").agg(count("*").alias("route_count")).orderBy(col("route_count").desc())
bus_frequency = bus_frequency_spark.toPandas()

top_bus = bus_frequency.iloc[0]
print(f"   - Most Active Bus: #{top_bus['bus_number']} with {top_bus['route_count']} routes")
print(f"\n   Top 5 Busiest Bus Numbers:")
for i, (_, bus) in enumerate(bus_frequency.head(5).iterrows(), 1):
    print(f"   {i}. Bus #{bus['bus_number']}: {bus['route_count']} routes")

# ============================================
# TASK 7: NETWORK VISUALIZATION
# ============================================
print("\n[TASK 7/7] Creating enhanced network visualizations...")

# Position nodes using spring layout
pos = nx.spring_layout(G, k=2, iterations=50, seed=42)

# Determine node colors and sizes based on connectivity
node_colors = []
node_sizes = []
for node in G.nodes():
    node_data = connections[connections['stop_id'] == node]
    if len(node_data) > 0:
        conn_count = node_data['total_connections'].values[0]
    else:
        conn_count = 0
    
    node_sizes.append(conn_count * 150 + 300)
    
    if conn_count >= 6:
        node_colors.append('#E74C3C')
    elif conn_count >= 4:
        node_colors.append('#F39C12')
    elif conn_count >= 3:
        node_colors.append('#27AE60')
    else:
        node_colors.append('#3498DB')

# Edge colors based on travel time
edge_colors = []
edge_widths = []
for edge in G.edges():
    travel_time = G[edge[0]][edge[1]]['time']
    if travel_time > 30:
        edge_colors.append('#E74C3C')
        edge_widths.append(3)
    elif travel_time > 15:
        edge_colors.append('#F39C12')
        edge_widths.append(2)
    else:
        edge_colors.append('#95A5A6')
        edge_widths.append(1.5)

# Create main network visualization
plt.figure(figsize=(18, 12))

nx.draw_networkx_edges(G, pos, 
                       edge_color=edge_colors,
                       width=edge_widths,
                       arrows=True, 
                       arrowsize=20,
                       arrowstyle='->',
                       alpha=0.6,
                       connectionstyle='arc3,rad=0.1')

nx.draw_networkx_nodes(G, pos, 
                       node_color=node_colors, 
                       node_size=node_sizes, 
                       alpha=0.9,
                       edgecolors='black',
                       linewidths=3)

labels = {row['stop_id']: row['stop_name'] for _, row in stops_df.iterrows()}
nx.draw_networkx_labels(G, pos, labels, font_size=9, font_weight='bold')

from matplotlib.patches import Patch
legend_elements = [
    Patch(facecolor='#E74C3C', label='High (6+ connections)', edgecolor='black', linewidth=2),
    Patch(facecolor='#F39C12', label='Medium (4-5 connections)', edgecolor='black', linewidth=2),
    Patch(facecolor='#27AE60', label='Low-Medium (3 connections)', edgecolor='black', linewidth=2),
    Patch(facecolor='#3498DB', label='Isolated (0-2 connections)', edgecolor='black', linewidth=2),
    Patch(facecolor='white', label='', edgecolor='white'),
    Patch(facecolor='#E74C3C', label='Long Route (>30 min)', edgecolor='black', linewidth=2),
    Patch(facecolor='#F39C12', label='Medium Route (15-30 min)', edgecolor='black', linewidth=2),
    Patch(facecolor='#95A5A6', label='Short Route (<15 min)', edgecolor='black', linewidth=2)
]
plt.legend(handles=legend_elements, loc='upper left', fontsize=11, framealpha=0.95)

plt.title('BUS NETWORK CONNECTIVITY MAP - SPARK ANALYSIS\n(Node Size = Connection Count | Edge Color = Travel Time)', 
          fontsize=18, fontweight='bold', pad=20)
plt.axis('off')
plt.tight_layout()
plt.savefig('bus_network_map_spark.png', dpi=300, bbox_inches='tight', facecolor='white')
print("   [OK] Saved: bus_network_map_spark.png")
plt.show()

# Create connectivity bar chart
print("   Creating connectivity bar chart...")
plt.figure(figsize=(14, 10))

chart_data = connections.sort_values('total_connections', ascending=True)

bar_colors = []
for conn in chart_data['total_connections']:
    if conn >= 6:
        bar_colors.append('#E74C3C')
    elif conn >= 4:
        bar_colors.append('#F39C12')
    elif conn >= 3:
        bar_colors.append('#27AE60')
    else:
        bar_colors.append('#3498DB')

plt.barh(chart_data['stop_name'], chart_data['total_connections'], color=bar_colors, edgecolor='black', linewidth=1.5)
plt.xlabel('Total Connections (Incoming + Outgoing)', fontsize=13, fontweight='bold')
plt.ylabel('Bus Stop', fontsize=13, fontweight='bold')
plt.title('BUS STOP CONNECTIVITY RANKING - SPARK ANALYSIS', fontsize=18, fontweight='bold', pad=20)
plt.grid(axis='x', alpha=0.3, linestyle='--')

for i, (name, value) in enumerate(zip(chart_data['stop_name'], chart_data['total_connections'])):
    plt.text(value + 0.15, i, str(int(value)), va='center', fontweight='bold', fontsize=10)

plt.axvline(avg_degree, color='red', linestyle='--', linewidth=2, label=f'Average: {avg_degree:.1f}')
plt.legend(fontsize=11)

plt.tight_layout()
plt.savefig('connectivity_chart_spark.png', dpi=300, bbox_inches='tight', facecolor='white')
print("   [OK] Saved: connectivity_chart_spark.png")
plt.show()

# ============================================
# GENERATE COMPREHENSIVE REPORT
# ============================================
print("\n[REPORT] Generating comprehensive summary report...")

report = f"""
{"="*75}
            LOCAL TRANSPORT BUS NETWORK ANALYSIS
            COMPREHENSIVE SUMMARY REPORT (SPARK VERSION)
{"="*75}

NETWORK OVERVIEW:
   * Total Bus Stops: {total_stops}
   * Total Routes: {total_routes}
   * Unique Bus Numbers: {unique_buses}
   * Average Travel Time: {avg_travel:.2f} minutes
   * Network Density: {network_density:.2f}%

GRAPH STRUCTURE:
   * Graph Type: Directed
   * Nodes: {G.number_of_nodes()} (bus stops)
   * Edges: {G.number_of_edges()} (direct routes)
   * Average Degree: {avg_degree:.2f}
   * Maximum Degree: {max_degree}
   * Minimum Degree: {min_degree}

MAIN HUB IDENTIFICATION:
   * Name: {main_hub['stop_name']}
   * Stop ID: {main_hub['stop_id']}
   * Outgoing Routes: {main_hub['outgoing']}
   * Incoming Routes: {main_hub['incoming']}
   * Total Connections: {main_hub['total_connections']}
   * Status: Primary Transport Interchange

ACCESSIBILITY ANALYSIS:
   * High Accessibility Stops (6+ connections): {len(high_access)}
   * Medium Accessibility Stops (4-5 connections): {len(medium_access)}
   * Low Accessibility Stops (3 connections): {len(low_access)}
   * Isolated Stops (0-2 connections): {len(isolated)}
   * Recommendation: {"Network well-connected!" if len(isolated) == 0 else "Add routes to isolated stops"}

TOP 5 MOST CONNECTED STOPS:
"""

for i, (_, stop) in enumerate(connections.head(5).iterrows(), 1):
    report += f"   {i}. {stop['stop_name']} - {stop['total_connections']} connections "
    report += f"(Out: {stop['outgoing']}, In: {stop['incoming']})\n"

report += f"""
ROUTE ANALYSIS:
   * Longest Route: {max_travel} minutes
   * Shortest Route: {min_travel} minutes
   * Most Active Bus: #{top_bus['bus_number']} ({top_bus['route_count']} routes)

{"="*75}
ALL TASKS COMPLETED SUCCESSFULLY WITH APACHE SPARK!
Check generated PNG files for detailed visualizations
{"="*75}
"""

print(report)

# Save report
with open('network_analysis_report_spark.txt', 'w', encoding='utf-8') as f:
    f.write(report)
print("   [OK] Saved: network_analysis_report_spark.txt")

# Save detailed results to CSV using Pandas (Windows-compatible)
connections.to_csv('connectivity_results_spark.csv', index=False)
print("   [OK] Saved: connectivity_results_spark.csv")

bus_frequency.to_csv('bus_frequency_spark.csv', index=False)
print("   [OK] Saved: bus_frequency_spark.csv")

print("\n" + "="*75)
print("SPARK ANALYSIS COMPLETE!")
print("="*75)
print("\nGenerated Files:")
print("1. bus_network_map_spark.png - Enhanced network graph")
print("2. connectivity_chart_spark.png - Detailed bar chart")
print("3. network_analysis_report_spark.txt - Comprehensive report")
print("4. connectivity_results_spark.csv - Full connectivity data")
print("5. bus_frequency_spark.csv - Bus frequency statistics")
print("="*75)
print("\nKEY FINDINGS:")
print(f"* Main Hub: {main_hub['stop_name']} ({main_hub['total_connections']} connections)")
print(f"* Network Density: {network_density:.2f}%")
print(f"* Isolated Stops: {len(isolated)}")
print(f"* Average Connectivity: {avg_degree:.2f} connections per stop")
print("="*75)

# Cleanup Spark
connections_spark.unpersist()
spark.stop()