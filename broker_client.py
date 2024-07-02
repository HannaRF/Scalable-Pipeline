from FastDelivery_Mock import *
from multiprocessing import Process
import pika
import json

def publish_event(event):
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    channel.queue_declare(queue='orders', durable=True)
    
    channel.basic_publish(
        exchange='',
        routing_key='quotes',
        body=json.dumps(event),
        properties=pika.BasicProperties(
            delivery_mode=2,  # make message persistent
        ))
    
    print(f"Sent event {event}")
    connection.close()

def create_order_and_publish(delay_max: int = 30) -> None:
    """
    Simulates the creation of an order in the FastDelivery ERP system.
    """
    # Generate a new order
    order = create_order(delay_max)
    
    # Publish the order event
    publish_event(order)


if __name__ == "__main__":

    # Check if the database exists
    if not os.path.exists("fastdelivery/fastdelivery.db"):
        create_fast_delivery_db()

    # create process to simulate each consumer
    num_order = 10
    processes = []
    
    for consumer_id in range(1, num_order+1):
        p = Process(target=create_order_and_publish)
        p.start()
        processes.append(p)

    for p in processes:
        p.join()