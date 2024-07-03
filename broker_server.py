import json
import pika
import sqlite3
from multiprocessing import Lock

lock = Lock()

def consume_messages_from_rabbitmq():
    def insert_event_into_db(event):
        conn = sqlite3.connect('fastdelivery/fastdelivery.db')
        cursor = conn.cursor()

        query = """INSERT INTO quote (consumer_id, store_id, product_id, quantity, creation_date, status, total_cost)
                    VALUES ({})
        """.format(", ".join("?" * len(event.values())))

        with lock:
            cursor.execute(query, tuple(event.values()))
            conn.commit()
            conn.close()

    def callback(ch, method, properties, body):
        event = json.loads(body)
        insert_event_into_db(event)
        print(f'Inserted event into SQLite DB: {event}')

    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    channel.queue_declare(queue='quotes', durable=True)

    channel.basic_consume(queue='quotes', on_message_callback=callback, auto_ack=True)

    print('Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

if __name__ == '__main__':
    consume_messages_from_rabbitmq()
