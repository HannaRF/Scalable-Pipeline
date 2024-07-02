import sqlite3
from multiprocessing import Pool, Lock

lock = Lock()

def update_product(product):
    product_id, quantity = product
    # Create a connection to the SQLite database
    connection = sqlite3.connect("fastdelivery/fastdelivery.db")
    cursor = connection.cursor()

    # Update the product
    with lock:
        cursor.execute("UPDATE product SET quantity = quantity + ? WHERE product_id = ?;", (1, product_id))
        connection.commit()
        connection.close()

def restock_inventory() -> None:
    """
    Restock the inventory of the FastDelivery ERP system data.
    """
    # Create a connection to the SQLite database
    connection = sqlite3.connect("fastdelivery/fastdelivery.db")
    cursor = connection.cursor()

    # Get all products
    cursor.execute("SELECT product_id, quantity FROM product;")
    products = cursor.fetchall()
    connection.close()
    
    # Create a pool of workers
    pool = Pool()

    # Update products in parallel
    pool.map(update_product, products)

    # Close the pool and wait for the work to finish
    pool.close()
    pool.join()

if __name__ == '__main__':
    restock_inventory()
