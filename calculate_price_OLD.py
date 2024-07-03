from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from graphframes import GraphFrame
import pandas as pd

# Initialize a Spark session
spark = SparkSession.builder \
    .appName("GraphFramesExample") \
    .getOrCreate()

def create_graph():
    # Define vertices
    vertices = pd.DataFrame([
        ("A", "Neighborhood A"),
        ("B", "Neighborhood B"),
        ("C", "Neighborhood C"),
        ("D", "Neighborhood D"),
        ("E", "Neighborhood E")
    ], columns=["id", "name"])

    # Define edges
    edges = pd.DataFrame([
        ("A", "B", 5),
        ("A", "C", 10),
        ("B", "C", 3),
        ("B", "D", 9),
        ("C", "D", 1),
        ("D", "E", 2),
        ("E", "A", 7)
    ], columns=["src", "dst", "weight"])

    # Create DataFrames
    v = spark.createDataFrame(vertices)
    e = spark.createDataFrame(edges)

    # Create the GraphFrame
    g = GraphFrame(v, e)
    return g

def save_graph(graph, vertices_path, edges_path):
    graph.vertices.write.parquet(vertices_path, mode="overwrite")
    graph.edges.write.parquet(edges_path, mode="overwrite")
    print(f"Graph saved to {vertices_path} and {edges_path}")

def load_graph(spark, vertices_path, edges_path):
    v = spark.read.parquet(vertices_path)
    e = spark.read.parquet(edges_path)
    g = GraphFrame(v, e)
    print(f"Graph loaded from {vertices_path} and {edges_path}")
    return g

def calculate_order_cost(graph, start, end):
    # Use the shortest paths method to calculate the shortest path
    results = graph.shortestPaths(landmarks=[end])
    path_length = results.filter(col("id") == start).select(col("distances").getItem(end)).first()[0]
    return path_length

def main():
    # Create the graph
    graph = create_graph()

    # Save the graph to files
    save_graph(graph, 'vertices.parquet', 'edges.parquet')

    # Load the graph from files
    loaded_graph = load_graph(spark, 'vertices.parquet', 'edges.parquet')

    # Define start and end neighborhoods
    start_neighborhood = 'A'
    end_neighborhood = 'D'

    # Calculate the order cost
    cost = calculate_order_cost(loaded_graph, start_neighborhood, end_neighborhood)

    if cost is not None:
        print(f"The shortest path cost from {start_neighborhood} to {end_neighborhood} is: {cost}")
    else:
        print(f"No path found from {start_neighborhood} to {end_neighborhood}")

if __name__ == "__main__":
    main()
    spark.stop()

    # Vertex DataFrame
    v = spark.createDataFrame([
    ("a", "Alice", 34),
    ("b", "Bob", 36),
    ("c", "Charlie", 30),
    ("d", "David", 29),
    ("e", "Esther", 32),
    ("f", "Fanny", 36),
    ("g", "Gabby", 60)
    ], ["id", "name", "age"])
    # Edge DataFrame
    e = spark.createDataFrame([
    ("a", "b", "friend"),
    ("b", "c", "follow"),
    ("c", "b", "follow"),
    ("f", "c", "follow"),
    ("e", "f", "follow"),
    ("e", "d", "friend"),
    ("d", "a", "friend"),
    ("a", "e", "friend")
    ], ["src", "dst", "relationship"])
    # Create a GraphFrame
    g = GraphFrame(v, e)