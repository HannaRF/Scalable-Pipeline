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


    # # insert products from products.csv
    # with open("fast_delivery/fast_delivery.csv", "r") as file:
    #     reader = csv.reader(file)
    #     next(reader)
    #     for row in reader:
    #         cursor.execute("INSERT INTO products VALUES (?, ?, ?, ?, ?);", row)

    # Commit the transaction
    connection.commit()
    
    # Close the connection
    connection.close()


# def insert_fast_delivery_db(spreadsheets: Dict[str, List[Dict[str, Any]]]) -> None:
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


def create_order(consumer_id : int, num_stores :int) -> None:
    """
    Simulate the FastDelivery ERP system data.

    Parameters:
        num_spreadsheets (int): The number of spreadsheets to be generated for each type of data.
    
    Returns:
        A dictionary with keys for each data type (users, products, inventory, orders)
        and values as lists of dictionaries representing each item or transaction.
    """
    spreadsheet_names = ["Andres", 
                         "Harry", 
                         "Shakira", 
                         "Alexandre", 
                         "Thanos", 
                         "Annabeth", 
                         "Deide"]
    
    spreadsheet_surname = ["D'Alessandro", 
                           "Potter", 
                           "Mebarak", 
                           "de Moraes", 
                           "Pereira", 
                           "Chase", 
                           "Costa"]
    
    spreadsheet_neighborhood = ["Centro",
                                "Barra da Tijuca",
                                "Botafogo",
                                "Ipanema",
                                "Leblon",
                                "Laranjeiras",
                                "Gávea"]
    
    # Consumer
    consumer = {"consumer_id": consumer_id,
                "name": random.choice(spreadsheet_names),
                "surname": random.choice(spreadsheet_surname),
                "birthday": generate_random_date(start=datetime.datetime(1950, 1, 1), end=datetime.datetime.now() - relativedelta(years=18)).isoformat(),
                "neighborhood": random.choice(spreadsheet_neighborhood)}

    # choose a store
    store_id = random.randint(1, num_stores)

    # create order
    product_id = random.randint(1, 400)

    creation_date = generate_recent_date(24)  # Focus on the last 24 hours

    quote = {"user_id": consumer_id,
            "store_id": store_id,
            "product_id": product_id,

            ######## falta controle de estoque / produto deve estar disponível em estoque
            "price": random.uniform(10, 100),
            "quantity": random.randint(1, 10),

            "creation_date": creation_date,
            "status": "created"}    
    

    fast_delivery_data =  {"users": consumer,
                        # "products": product,
                        # "inventory": inventory,
                        "orders": quote}

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
    num_consumers = 5
    num_stores = 10
    processes = []
    
    for consumer_id in range(1, num_consumers+1):
        p = Process(target=create_order, args=(consumer_id, num_stores))
        p.start()
        processes.append(p)

    for p in processes:
        p.join()
    