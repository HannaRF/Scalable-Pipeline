from dateutil.relativedelta import relativedelta
from typing import List, Dict, Any
import datetime
import random
import time
import csv
# import psycopg2
import sqlite3

import os
import pandas as pd

def generate_recent_date(hours=6) -> str:
    """
    Generate a random datetime within the last specified number of hours.

    Parameters:
    hours (int): The number of hours before the current datetime to use as the lower limit for date generation.

    Returns:
        A datetime object as a string in ISO format.
    """
    end = datetime.datetime.now()
    start = end - relativedelta(hours=hours)
    random_date = start + (end - start) * random.random()

    return random_date.isoformat()


def create_fast_delivery_db(num_neighborhoods : int = 44) -> None:
    """
    Create a SQLite database to store the FastDelivery ERP system data.

    Returns:
        None
    """
    # 44 neighborhoods
    lst_neighborhoods = ['Vila Isabel', 'Moema', 'Bangu', 'Jardim Botanico', 'Jabaquara', 
                        'Analia Franco', 'Catete', 'Leblon', 'Santana', 'Urca', 'Pinheiros',
                        'Perdizes', 'Gloria', 'Grajau', 'Vila Madalena', 'Pompeia', 'Bela Vista',
                        'Sao Conrado', 'Laranjeiras', 'Santa Teresa', 'Jardins', 'Itaim Bibi',
                        'Liberdade', 'Barra da Tijuca', 'Centro (SP)', 'Centro', 'Freguesia do O',
                        'Lapa', 'Copacabana', 'Brooklin', 'Tatuape', 'Cidade Jardim', 'Recreio dos Bandeirantes',
                        'Morumbi', 'Tijuca', 'Ipanema', 'Butanta', 'Vila Mariana', 'Higienopolis', 'Santo Amaro',
                        'Chacara Flora', 'Botafogo', 'Flamengo', 'Madureira']

    lst_neighborhoods_chosen = random.sample(lst_neighborhoods, num_neighborhoods)

    # Create a connection to the SQLite database
    connection = sqlite3.connect("fastdelivery/fastdelivery.db")

    # connection = psycopg2.connect()
    cursor = connection.cursor()

    # Create the tables
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS consumer (
        consumer_id FLOAT PRIMARY KEY,
        name TEXT,
        surname TEXT,
        neighborhood TEXT,
        birthday TEXT
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS product (
        product_id INTEGER PRIMARY KEY,
        name TEXT,
        price FLOAT,
        weight FLOAT,
        quantity INTEGER
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS quote (
        quote_id INTEGER PRIMARY KEY AUTOINCREMENT,
        consumer_id FLOAT,
        store_id INTEGER,
        product_id INTEGER,
        quantity INTEGER,
        creation_date TEXT,
        status TEXT,
        total_cost FLOAT
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS store (
        store_id INTEGER PRIMARY KEY,
        name TEXT,
        neighborhood TEXT,
        weight_tax FLOAT
    );
    """)

    # insert consumers
    with open("fastdelivery/consumer.csv", "r", encoding="latin1") as file:
        reader = csv.reader(file)
        next(reader)
        for row in reader:
            if row[3] in lst_neighborhoods_chosen:
                cursor.execute("INSERT INTO consumer VALUES (?, ?, ?, ?, ?);", row)

    # insert stores
    with open("fastdelivery/store.csv", "r", encoding="latin1") as file:
        reader = csv.reader(file)
        next(reader)
        for row in reader:
            if row[2] in lst_neighborhoods_chosen:
                cursor.execute("INSERT INTO store VALUES (?, ?, ?, ?);", row)
    
    # insert products
    with open("fastdelivery/product.csv", "r", encoding="latin1") as file:
        reader = csv.reader(file)
        next(reader)
        for row in reader:
            cursor.execute("INSERT INTO product VALUES (?, ?, ?, ?, ?);", row)

    # Commit the transaction
    connection.commit()
    
    # Close the connection
    connection.close()


    # save the neighborhoods chosen
    lst_neighborhoods_chosen = pd.DataFrame(lst_neighborhoods_chosen, columns=["neighborhood"])
    lst_neighborhoods_chosen.to_csv("fastdelivery/neighborhoods_chosen.csv", index=False)


def create_order(delay_max: int = 5) -> None:

    # Make the process take some time, to simulate a real system
    #time.sleep(random.randint(1, delay_max))
    """
    Simulate the FastDelivery ERP system data.

    Parameters:
        num_spreadsheets (int): The number of spreadsheets to be generated for each type of data.
    
    Returns:
        A dictionary with keys for each data type (users, products, inventory, orders)
        and values as lists of dictionaries representing each item or transaction.
    """

    consumer_id = random.randint(1, 200)
    store_id = random.randint(1, 30)
    product_id = random.randint(1, 150)

    creation_date = generate_recent_date(24)  # Focus on the last 24 hours
    
    quote = {"user_id": consumer_id,
            "store_id": store_id,
            "product_id": product_id,
            "quantity": random.randint(1, 10),
            "creation_date": creation_date,
            "status": "created",
            "total_cost": -1}

    # Return the generated data
    return quote


def delete_fast_delivery_db() -> None:
    """
    Delete the FastDelivery SQLite database.

    Returns:
        None
    """
    if os.path.exists("fastdelivery/fastdelivery.db"):
        os.remove("fastdelivery/fastdelivery.db")
    else:
        print("The file does not exist")

if __name__ == "__main__":
    create_fast_delivery_db()
    