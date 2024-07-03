from pyspark.sql import SparkSession
import pandas as pd
import random
from graphframes import *
import os

# Função para criar o grafo da cidade
def calculate_distance(origin, destination):

    os.environ["HADOOP_HOME"] = "C:\\hadoop"
    os.environ["hadoop.home.dir"] = "C:\\hadoop"
    os.environ["PYTHONPATH"] = "C:\\Users\\data_adaggio_2024\\AppData\\Local\\Microsoft\\WindowsApps\\python.exe"

    driver = 'fastdelivery\\graphframes-0.8.3-spark3.5-s_2.12.jar'

    spark = SparkSession.builder \
        .appName("CityGraph") \
        .config("spark.driver.extraClassPath", driver) \
        .getOrCreate()
    
    g = None

    if not os.path.exists(r"fastdelivery/vertices") and not os.path.exists(r"fastdelivery/edges"):
        print("Criando vértices e arestas...")

        # Selecionar um número definido de neighborhoods únicos 
        df = pd.read_csv("fastdelivery/neighborhoods_chosen.csv")
        neighborhoods_chosen = df['neighborhood'].unique()
        
        # Criar DataFrame de vértices
        vertices = spark.createDataFrame(pd.DataFrame(neighborhoods_chosen, columns=["id"]))
        
        # Criar DataFrame de arestas
        edges_list = []
        for neighborhood in neighborhoods_chosen:
            num_frontiers = random.randint(1, 5)
            frontiers = random.sample(list(neighborhoods_chosen), num_frontiers)
            for frontier in frontiers:
                if neighborhood != frontier:
                    edges_list.append((neighborhood, frontier))
        
        edges = spark.createDataFrame(pd.DataFrame(edges_list, columns=["src", "dst"]))

        # Salvar vértices e arestas
        g = GraphFrame(vertices, edges)
        g.vertices.write.parquet(r"fastdelivery/vertices", mode="append")
        g.edges.write.parquet(r"fastdelivery/edges", mode="append")

    else:
        # Carregar vértices e arestas
        vertices = spark.read.parquet(r"fastdelivery/vertices")
        edges = spark.read.parquet(r"fastdelivery/edges")
        g = GraphFrame(vertices, edges)

    print("Criando o grafo...")
    # Encontrar o menor caminho entre dois bairros
    shortest_path = g.shortestPaths(landmarks=[origin, destination])
    # Selecionar o menor caminho
    shortest_path = shortest_path.select("id", "distances").show()#.collect()

    spark.stop()

    return shortest_path


if __name__ == "__main__":
    distance = calculate_distance(origin="Copacabana", destination="Botafogo")

    print(distance)


