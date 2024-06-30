from dateutil.relativedelta import relativedelta
from typing import List, Dict, Any
import datetime
import random
import csv
# import psycopg2
import sqlite3
# from threading import Thread, Lock
from multiprocessing import Process
import pika
import json
import os
import pandas as pd


def generate_random_date(start: datetime = datetime.datetime(2020, 1, 1), end: datetime = datetime.datetime.now()) -> str:
    """
    Generate a random datetime between two datetime objects.

    Parameters:
    start (datetime): lower limit for date generation.
    end (datatime): upper limit for date generation.

    Returns:
        A datetime object.
    """

    return start + (end - start) * random.random()


# #################################### Discount coupon implementations ####################################

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


# Dictionary to store the availability status of coupons for users
# coupons_available = {}
# lock = Lock()


# def listen_rabbitmq():
#     """
#     Listens for messages from RabbitMQ about coupon assignments.
#     """
#     connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
#     channel = connection.channel()
#     # Ensure the queue exists and is durable
#     channel.queue_declare(queue='coupon_notifications', durable=True)

#     def callback(ch, method, properties, body):
#         """ Callback function to process the received message. """
#         data = json.loads(body)
#         user_id = data['user_id']
#         with lock:
#             coupons_available[user_id] = True
#             print(f"Coupon received for user {user_id}")

#     # Start consuming messages from the queue
#     channel.basic_consume(queue='coupon_notifications', on_message_callback=callback, auto_ack=True)
#     channel.start_consuming()


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
        consumer_id FLOAT,
        store_id INTEGER,
        product_id INTEGER,
        price FLOAT,
        quantity INTEGER,
        creation_date TEXT,
        status TEXT,
        PRIMARY KEY (consumer_id, store_id, product_id, creation_date)
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
    with open("fastdelivery/consumer.csv", "r") as file:
        reader = csv.reader(file)
        next(reader)
        for row in reader:
            cursor.execute("INSERT INTO consumer VALUES (?, ?, ?, ?, ?);", row)

    # insert stores
    with open("fastdelivery/store.csv", "r") as file:
        reader = csv.reader(file)
        next(reader)
        for row in reader:
            cursor.execute("INSERT INTO store VALUES (?, ?, ?, ?, ?);", row)
    
    # insert products
    with open("fastdelivery/product.csv", "r") as file:
        reader = csv.reader(file)
        next(reader)
        for row in reader:
            cursor.execute("INSERT INTO product VALUES (?, ?, ?, ?, ?);", row)

    # Commit the transaction
    connection.commit()
    
    # Close the connection
    connection.close()


# def update_fast_delivery_db(spreadsheets: Dict[str, List[Dict[str, Any]]]) -> None:
#     """
#     Save a list of log entries to a PostgreSQL database.

#     Parameters:
#         spreadsheets (Dict[str, List[Dict[str, Any]]]): The list of log entries to save.
#         connection (Any): The connection to the PostgreSQL database.

#     Returns:
#         None
#     """
#     # Create a connection to the SQLite database
#     connection = sqlite3.connect("fastdelivery/fastdelivery.db")

#     # connection = psycopg2.connect()
#     cursor = connection.cursor()

#     # update or insert users
#     cursor.executemany("""
#     INSERT OR REPLACE INTO users (user_id, name, surname, address, registration_date, birthday)
#     VALUES (:user_id, :name, :surname, :address, :registration_date, :birthday);
#     """, spreadsheets["user"])

#     # atualiza quantidades do inventario
#     cursor.executemany('''
#             UPDATE inventory
#             SET quantity = :quantity
#             WHERE product_id = :product_id
#             ''', spreadsheets["inventory"])
    
#     # atualiza preço dos produtos
#     cursor.executemany('''
#             UPDATE products
#             SET price = :price
#             WHERE product_id = :product_id
#             ''', spreadsheets["products"])

#     cursor.executemany("""
#     INSERT INTO orders (user_id, store_id, product_id, price, quantity, creation_date, payment_date, delivery_date)
#     VALUES (:user_id, :store_id, :product_id, :price, :quantity, :creation_date, :payment_date, :delivery_date);
#     """, spreadsheets["orders"])

#     # Commit the transaction
#     connection.commit()
    
#     # Close the connection
#     connection.close()


def create_order() -> None:
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
    cursor.execute("SELECT price,quantity FROM product WHERE product_id = ?;", (product_id,))
    price = cursor.fetchone()[0]
    quantity = cursor.fetchone()[1]
    connection.close()
    
    quote = {"user_id": consumer_id,
            "store_id": store_id,
            "product_id": product_id,
            "price": price,
            # garantimos que o produto está no estoque
            "quantity": random.randint(1, quantity),
            "creation_date": creation_date,
            "status": "created"}

    # Joga pro Broker colocar no banco

    # insert_fast_delivery_db(fast_delivery_data)



if __name__ == "__main__":

    # Check if the database exists
    if not os.path.exists("fastdelivery/fastdelivery.db"):
        create_fast_delivery_db()

    # create process to listen to RabbitMQ
    # p = Process(target=listen_rabbitmq)
    # p.start()

    # create process to simulate each consumer
    num_order = 50
    processes = []
    
    for consumer_id in range(1, num_order+1):
        p = Process(target=create_order)
        p.start()
        processes.append(p)

    for p in processes:
        p.join()
    