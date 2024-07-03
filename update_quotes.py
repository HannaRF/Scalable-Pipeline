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

    # filter the quotes that have status 'created'
    quotes = quotes.filter(col("status") == "created")

    # change all quotes status to 'pending'
    quotes = quotes.withColumn("status", lit("pending"))

    # update the quotes table in the database
    quotes.write.format("jdbc").options(
        url="jdbc:sqlite:fastdelivery/fastdelivery.db",
        dbtable="quote",
        driver="org.sqlite.JDBC"
    ).mode("overwrite").save()

update_quotes()