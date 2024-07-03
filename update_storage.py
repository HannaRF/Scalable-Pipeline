import sqlite3
from multiprocessing import Pool, Lock

lock = Lock()

def update_product(product, quantity_to_add) -> None:
    product_id = product
    # Create a connection to the SQLite database
    connection = sqlite3.connect("fastdelivery/fastdelivery.db")
    cursor = connection.cursor()

    # Update the product
    with lock:
        cursor.execute("UPDATE product SET quantity = quantity + ? WHERE product_id = ?;", (quantity_to_add, product_id))
        connection.commit()
        connection.close()

def restock_inventory(quantity_to_add: int = 100) -> None:
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

    # Update the products
    for product in products:
        pool.apply_async(update_product, args=(product[0], quantity_to_add))

    # Close the pool and wait for the work to finish
    pool.close()
    pool.join()

if __name__ == '__main__':
    restock_inventory(1)
