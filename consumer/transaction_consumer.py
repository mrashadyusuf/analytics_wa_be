import pika
import json
import s3fs
import pandas as pd
import pyarrow as pa 
import pyarrow.parquet as pq
import duckdb
from database import SessionLocal
from schemas.schemas import TransactionCreate
from models.models import Transaction

from routes.transaction import generate_bucket_name, generate_parquet_file_name, aws_access_key, aws_secret_key, get_latest_transaction_file_from_s3
# Function to process transaction and write it to S3 Parquet file



def process_transaction(ch, method, properties, body):
    db = SessionLocal()  # Open a new session for database interaction
    try:
        # Parse the incoming data (if necessary for logging or other purposes)
        transaction_data = json.loads(body)
        print(f"Processing transaction request, but only using data from the database...",transaction_data)

        # Query all existing transactions from the database
        existing_data = db.query(Transaction).all()  # Retrieve all existing transactions
        # Convert existing transactions to a DataFrame
        existing_data_df = pd.DataFrame([{
            'transaction_id': trans.transaction_id,
            'transaction_channel': trans.transaction_channel,
            'transaction_dt': trans.transaction_dt.strftime('%Y-%m-%d'),
            'model_product': trans.model_product,
            'kuantitas': trans.kuantitas,
            'price_product': trans.price_product,
            'no_hp_cust': trans.no_hp_cust,
            'name_cust': trans.name_cust,
            'city_cust': trans.city_cust,
            'prov_cust': trans.prov_cust,
            'address_cust': trans.address_cust,
            'instagram_cust': trans.instagram_cust,
            'created_by': trans.created_by,
            'created_dt': trans.created_dt.strftime('%Y-%m-%d %H:%M:%S'),
            'updated_by': trans.updated_by,
            'updated_dt': trans.updated_dt.strftime('%Y-%m-%d %H:%M:%S')
        } for trans in existing_data])

        # AWS S3 credentials and path configuration (for storing Parquet)
        bucket_name = generate_bucket_name(transaction_data['user_group'])  # Can still use the user_group from the message
        parquet_file_name = generate_parquet_file_name()
        s3_path = f"s3://{bucket_name}/{parquet_file_name}"

        # Create S3 file system object using s3fs
        fs = s3fs.S3FileSystem(key=aws_access_key, secret=aws_secret_key)

        # Write the existing DataFrame (only from the database) back to S3 as a Parquet file
        existing_data_df.to_parquet(s3_path, engine='pyarrow', index=False, filesystem=fs)
        print(f"Parquet file created and saved to S3 at {s3_path}")

        # Acknowledge message so it can be removed from the queue
        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        print(f"Failed to process transaction: {str(e)}")
        ch.basic_nack(delivery_tag=method.delivery_tag)

    finally:
        db.close()  # Close the session

# RabbitMQ Consumer Setup
def start_consumer():
    credentials = pika.PlainCredentials('guest', 'guest')  # Replace with your credentials if needed
    connection_params  = pika.ConnectionParameters('localhost', 5672, '/', credentials)
    # Establish connection using BlockingConnection, not ConnectionParameters
    connection = pika.BlockingConnection(connection_params)

    channel = connection.channel()

    # Declare the queue to ensure it exists
    channel.queue_declare(queue='transaction_queue', durable=True)


    # Set up a consumer to process messages from the queue
    channel.basic_qos(prefetch_count=1)  # Process one message at a time
    channel.basic_consume(queue='transaction_queue', on_message_callback=process_transaction)


    print('Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()


