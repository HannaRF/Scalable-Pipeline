import sqlite3
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

def update_status(quote_id, status):
    # Create a connection to the SQLite database
    connection = sqlite3.connect("fastdelivery/fastdelivery.db")
    cursor = connection.cursor()

    # Update the quote status
    with lock:
        cursor.execute("UPDATE quote SET status = ? WHERE quote_id = ?;", (status, quote_id))
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

    # Read the store table from the database
    stores = spark.read.format("jdbc").options(
        url="jdbc:sqlite:fastdelivery/fastdelivery.db",
        dbtable="store"
    ).load()

    # Read the consumer table from the database
    consumers = spark.read.format("jdbc").options(
        url="jdbc:sqlite:fastdelivery/fastdelivery.db",
        dbtable="consumer"
    ).load()

    # filter the quotes that have status 'created'
    quotes = quotes.filter(col("status") == "created")

    # change all quotes status to 'pending'
    quotes = quotes.withColumn("status", lit("pending"))

    # Collect the quote_ids and status
    quote_ids = quotes.select("quote_id", "status").collect()

    # Create a pool of workers
    pool = Pool()

    # Update the quotes
    for quote in quote_ids:
        pool.apply_async(update_status, args=(quote["quote_id"], quote["status"]))

update_quotes()