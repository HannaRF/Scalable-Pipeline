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

def create_fast_delivery_db() -> None:
    """
    Create a SQLite database to store the FastDelivery ERP system data.

    Returns:
        None
    """
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
        price FLOAT,
        quantity INTEGER,
        creation_date TEXT,
        status TEXT,
        distance FLOAT,
        total_cost FLOAT
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS store (
        store_id INTEGER PRIMARY KEY,
        name TEXT,
        weight_tax FLOAT,
        neighborhood TEXT
    );
    """)

    # insert consumers
    with open("fastdelivery/consumer.csv", "r", encoding="latin1") as file:
        reader = csv.reader(file)
        next(reader)
        for row in reader:
            cursor.execute("INSERT INTO consumer VALUES (?, ?, ?, ?, ?);", row)

    # insert stores
    with open("fastdelivery/store.csv", "r", encoding="latin1") as file:
        reader = csv.reader(file)
        next(reader)
        for row in reader:
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

    consumer_id = random.randint(1, 300)
    store_id = random.randint(1, 300)
    product_id = random.randint(1, 200)

    creation_date = generate_recent_date(24)  # Focus on the last 24 hours

    # read the product price
    connection = sqlite3.connect("fastdelivery/fastdelivery.db")
    cursor = connection.cursor()
    cursor.execute("SELECT price, quantity FROM product WHERE product_id = {}".format(product_id))
    price, quantity = cursor.fetchall()[0]
    connection.close()
    
    quote = {"user_id": consumer_id,
            "store_id": store_id,
            "product_id": product_id,
            "price": price,
            "quantity": random.randint(1, 10),
            "creation_date": creation_date,
            "status": "created",
            "distance": -1,
            "total_cost": -1}

    # Return the generated data
    return quote


    