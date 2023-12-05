import json
import pika
from pymongo import MongoClient

# Connect to MongoDB
mongo_client = MongoClient('mongodb://mongodb:27017/')

if mongo_client.server_info():
    print("Connected to MongoDB")
else:
    print("Failed to connect to MongoDB")

# Specify the database and collection names
database_name = 'movies_database'
collection_name = 'posts'

# Check if the database exists, create it if not
if database_name not in mongo_client.list_database_names():
    print(f"Database '{database_name}' does not exist. Creating...")
    mongo_client[database_name]

# Switch to the specified database
db = mongo_client[database_name]

# Check if the collection exists, create it if not
if collection_name not in db.list_collection_names():
    print(f"Collection '{collection_name}' does not exist. Creating...")
    db.create_collection(collection_name)

# Access the collection
collection = db[collection_name]

def callback(ch, method, properties, body):
    try:
        post = json.loads(body.decode('utf-8'))

        # Check if the post already exists in the database
        if "@Id" in post:
            existing_post = collection.find_one({"@Id": post["@Id"]})

            if not existing_post:
                # Insert the post into MongoDB
                collection.insert_one(post)
                print(f"Post @Id: {post['@Id']} inserted into MongoDB")
            else:
                print(f"Post already exists in MongoDB, @Id: {post['@Id']}")
        else:
            print("The '@Id' field is absent in the post. The post will not be inserted into MongoDB.")

    except json.JSONDecodeError as e:
        print(f"Error during JSON decoding: {e}")

# Configure the RabbitMQ connection
connection = pika.BlockingConnection(pika.URLParameters("amqp://rabbitmq"))

if connection.is_open:
    print("Connected to RabbitMQ")
else:
    print("Failed to connect to RabbitMQ")

channel = connection.channel()
channel.queue_declare(queue='posts_to_redis')

# Configure the callback
channel.basic_consume(queue='posts_to_redis', on_message_callback=callback, auto_ack=True)

print('Waiting for messages. To stop, press CTRL+C')
channel.start_consuming()
