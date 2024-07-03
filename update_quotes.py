import sqlite3
import pandas as pd
import time
import random
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from multiprocessing import Pool, Lock

lock = Lock()

def update_product(product, quantity_to_remove):
    product_id = product
    # Create a connection to the SQLite database
    connection = sqlite3.connect("fastdelivery/fastdelivery.db")
    cursor = connection.cursor()

    # Update the product
    with lock:
        cursor.execute("UPDATE product SET quantity = quantity - ? WHERE product_id = ?;", (quantity_to_remove, product_id))
        connection.commit()
        connection.close()

def update_status_and_total_cost(quote_id, status, total_cost):
    # Create a connection to the SQLite database
    connection = sqlite3.connect("fastdelivery/fastdelivery.db")
    cursor = connection.cursor()

    # Update the quote status
    with lock:
        cursor.execute("UPDATE quote SET status = ?, total_cost = ? WHERE quote_id = ?;", (status, total_cost, quote_id))
        connection.commit()
        connection.close()


def update_quotes():
    # path to jdbc driver
    driver = "fastdelivery/sqlite-jdbc-3.46.0.0.jar"

    # initialize a Spark session for sql operations
    spark = SparkSession.builder \
        .appName("quoteUpdater") \
        .config("spark.driver.extraClassPath", driver) \
        .getOrCreate()
    
    # Read the quote table from the database
    quotes = spark.read.format("jdbc").options(
        url="jdbc:sqlite:fastdelivery/fastdelivery.db",
        dbtable="quote"
    ).load()

    # Read the product table from the database
    products = spark.read.format("jdbc").options(
        url="jdbc:sqlite:fastdelivery/fastdelivery.db",
        dbtable="product"
    ).load()

    # Get only product_id, price and weight columns
    products = products.select("product_id", "price", "weight", "quantity")

    # rename the columns quantity to available_quantity
    products = products.withColumnRenamed("quantity", "available_quantity")

    # Read the store table from the database
    stores = spark.read.format("jdbc").options(
        url="jdbc:sqlite:fastdelivery/fastdelivery.db",
        dbtable="store"
    ).load()

    # Get only store_id, neighborhood and weight_tax columns
    stores = stores.select("store_id", "neighborhood", "weight_tax")

    # Read the consumer table from the database
    consumers = spark.read.format("jdbc").options(
        url="jdbc:sqlite:fastdelivery/fastdelivery.db",
        dbtable="consumer"
    ).load()

    # Get only consumer_id and neighborhood columns
    consumers = consumers.select("consumer_id", "neighborhood")

    # rename the columns neighborhood to consumer_neighborhood
    consumers = consumers.withColumnRenamed("neighborhood", "consumer_neighborhood")

    # filter the quotes that have status 'created'
    #quotes = quotes.filter(col("status") == "created")
    #quotes.show()

    # change all quotes status to 'pending'
    quotes = quotes.withColumn("status", lit("pending"))

    # join the quotes with the products
    quotes = quotes.join(products, "product_id", "inner")

    # join the quotes with the stores
    quotes = quotes.join(stores, "store_id", "inner")

    # join the quotes with the consumers
    quotes = quotes.join(consumers, "consumer_id", "inner")

    # calculate the total cost of the quote
    quotes = quotes.withColumn("total_cost", quotes.price * quotes.quantity + quotes.quantity * quotes.weight / 1000 * quotes.weight_tax)

    quotes.show()

    # Collect the quote_ids and status
    quote_ids = quotes.select("quote_id", "status", "total_cost").collect()

    # put the quotes in a list
    quote_ids = [quote for quote in quote_ids]

    # Update the quotes
    for quote in quote_ids:
        update_status_and_total_cost(quote.quote_id, quote.status, quote.total_cost)

    time.sleep(10)

    # status "cancelled" for quotes that have quantity greater than available_quantity
    quotes_cancelled = quotes.filter(col("quantity") > col("available_quantity"))
    quote_ids = quotes_cancelled.select("quote_id", "status").collect()

    for quote in quote_ids:
        update_status_and_total_cost(quote.quote_id, "cancelled", -1)

    # status "confirmed" for quotes that have quantity less than or equal to available_quantity
    quotes_confirmed = quotes.filter(col("quantity") <= col("available_quantity"))

    # Collect the quote_ids and quantity
    quote_ids = quotes_confirmed.select("quote_id", "product_id", "quantity", "total_cost").collect()

    # there is a chance of 50% to confirm the quote
    for quote in quote_ids:
        if random.random() <= 0.5:
            update_product(quote.product_id, quote.quantity)
            update_status_and_total_cost(quote.quote_id, "confirmed", quote.total_cost)
        else:
            update_status_and_total_cost(quote.quote_id, "refused", -1)

update_quotes()