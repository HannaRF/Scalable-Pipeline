from pyspark.sql import SparkSession
import pandas as pd
import random
from graphframes import *
import os

# Função para criar o grafo da cidade
def create_city_graph():

    os.environ["HADOOP_HOME"] = "C:\\hadoop"
    os.environ["hadoop.home.dir"] = "C:\\hadoop"

    driver = 'fastdelivery\\graphframes-0.8.3-spark3.5-s_2.13.jar'

    spark = SparkSession.builder \
        .appName("CityGraph") \
        .config("spark.driver.extraClassPath", driver) \
        .getOrCreate()
    
    # Ler o arquivo CSV
    df = pd.read_csv("fastdelivery/neighborhoods_chosen.csv")
    
    # Selecionar um número definido de neighborhoods únicos
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

    print('Fazendo Grafico')

    g = GraphFrame(vertices, edges)

    # print('Foi!')

    
    # # Mostrar vértices e arestas
    # g.vertices.show()
    # g.edges.show()
    
    # # Aqui, imprimimos a estrutura do grafo
    # g.edges.groupBy("src").count().show()
    
    spark.stop()


if __name__ == "__main__":
    create_city_graph()
