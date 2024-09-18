import pika
import json
import s3fs
import pandas as pd
import pyarrow as pa 
import pyarrow.parquet as pq
import duckdb

from routes.transaction import generate_bucket_name, generate_parquet_file_name, aws_access_key, aws_secret_key, get_latest_transaction_file_from_s3
# Function to process transaction and write it to S3 Parquet file
def process_transaction(ch, method, properties, body):
    try:
        transaction_data = json.loads(body)
        print(f"Processing transaction: {transaction_data['transaction_id']}")

        # AWS S3 credentials and path configuration
        bucket_name = generate_bucket_name(transaction_data['user_group'])  
        parquet_file_name = generate_parquet_file_name()
        s3_path = f"s3://{bucket_name}/{parquet_file_name}"
        print("parquet_file_name",parquet_file_name,"bucket_name",bucket_name,"aws_access_key",aws_access_key,"aws_secret_key",aws_secret_key)

        latest_parquet_file_name = get_latest_transaction_file_from_s3(bucket_name)
        s3_path_latest = f"s3://{bucket_name}/{latest_parquet_file_name}"

        # Load existing transactions from S3
        fs = s3fs.S3FileSystem(key=aws_access_key, secret=aws_secret_key)
        try:
            existing_data_df = pd.read_parquet(s3_path_latest, filesystem=fs)
            print("Existing data loaded from S3.")
        except Exception:
            print("Parquet file not found or unreadable. Creating a new one.")
            existing_data_df = pd.DataFrame()

        # Convert transaction data to DataFrame
        new_transaction_df = pd.DataFrame([{
            'transaction_id': transaction_data['transaction_id'],
            'transaction_channel': transaction_data['transaction_channel'],
            'transaction_dt': pd.to_datetime(transaction_data['transaction_dt']),
            'model_product': transaction_data['model_product'],
            'kuantitas': transaction_data['kuantitas'],
            'price_product': transaction_data['price_product'],
            'no_hp_cust': transaction_data['no_hp_cust'],
            'name_cust': transaction_data['name_cust'],
            'city_cust': transaction_data['city_cust'],
            'prov_cust': transaction_data['prov_cust'],
            'address_cust': transaction_data['address_cust'],
            'instagram_cust': transaction_data['instagram_cust'],
            'created_by': transaction_data['created_by'],
            'created_dt': transaction_data['created_dt'],
            'updated_by': transaction_data['updated_by'],
            'updated_dt': transaction_data['updated_dt'],
        }])

        # Append new transaction to the existing data
        if not existing_data_df.empty:
            updated_df = pd.concat([existing_data_df, new_transaction_df], ignore_index=True)
        else:
            updated_df = new_transaction_df

        # Write the updated DataFrame back to S3
        updated_df.to_parquet(s3_path, engine='pyarrow', index=False, filesystem=fs)
        print(f"Parquet file updated and saved to S3 at {s3_path}")

        # Acknowledge message so it can be removed from the queue
        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        print(f"Failed to process transaction: {str(e)}")
        ch.basic_nack(delivery_tag=method.delivery_tag)

def process_transaction_batch(ch, method, properties, body):
    try:
        transaction_data = json.loads(body)
        print(f"Processing transaction: {transaction_data['transaction_id']}")

        # AWS S3 credentials and path configuration
        bucket_name = generate_bucket_name(transaction_data['created_by'])  # Adjust as needed
        parquet_file_name = generate_parquet_file_name()
        s3_path = f"s3://{bucket_name}/{parquet_file_name}"

        # Load existing transactions from S3
        fs = s3fs.S3FileSystem(key=aws_access_key, secret=aws_secret_key)
        try:
            existing_data_df = pd.read_parquet(s3_path, filesystem=fs)
            print("Existing data loaded from S3.")
        except Exception:
            print("Parquet file not found or unreadable. Creating a new one.")
            existing_data_df = pd.DataFrame()

        # Append new transaction to DataFrame
        new_transaction_df = pd.DataFrame([transaction_data])

        # Combine the new transaction data with the existing data
        if not existing_data_df.empty:
            updated_df = pd.concat([existing_data_df, new_transaction_df], ignore_index=True)
        else:
            updated_df = new_transaction_df

        # Write the updated DataFrame back to S3
        updated_df.to_parquet(s3_path, engine='pyarrow', index=False, filesystem=fs)
        print(f"Parquet file updated and saved to S3 at {s3_path}")

        # Acknowledge message so it can be removed from the queue
        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        print(f"Failed to process transaction: {str(e)}")
        ch.basic_nack(delivery_tag=method.delivery_tag)


def process_update_transaction(ch, method, properties, body):
    try:
        update_data = json.loads(body)
        transaction_id = update_data['transaction_id']
        transaction_data = update_data['transaction_data']
        user_group = update_data['user_group']
        username = update_data['username']

        print("transaction_data in process",transaction_data)

        print(f"Processing update for transaction ID: {transaction_id}")

        # 1. AWS S3 credentials and path configuration
        bucket_name = generate_bucket_name(user_group)

        latest_parquet_file_name = get_latest_transaction_file_from_s3(bucket_name)
        s3_path_latest = f"s3://{bucket_name}/{latest_parquet_file_name}"

        parquet_file_name = generate_parquet_file_name()
        s3_path = f"s3://{bucket_name}/{parquet_file_name}"

        # 2. Load the Parquet file from S3 into a DataFrame
        fs = s3fs.S3FileSystem(key=aws_access_key, secret=aws_secret_key)
        try:
            df = pd.read_parquet(s3_path_latest, filesystem=fs)
            print("Data loaded from S3 parquet file.")
        except Exception:
            raise Exception("Parquet file not found or unreadable.")

        # 3. Fetch the existing transaction record
        transaction_df = df[df['transaction_id'] == transaction_id]
        if transaction_df.empty:
            raise Exception(f"Transaction with ID {transaction_id} not found.")

        # 4. Update the transaction fields
        for key, value in transaction_data.items():
            print("value1",value)
            transaction_df.at[transaction_df.index[0], key] = value

        # 5. Combine the updated transaction with the rest of the DataFrame
        df.update(transaction_df)

        # 6. Write the updated DataFrame back to S3 in a new Parquet file
        df.to_parquet(s3_path, engine='pyarrow', index=False, filesystem=fs)
        print(f"New Parquet file created and saved to S3 at {s3_path}")

        # Acknowledge the message
        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        print(f"Failed to process transaction update: {str(e)}")
        ch.basic_nack(delivery_tag=method.delivery_tag)


def process_delete_transaction(ch, method, properties, body):
    try:
        # Parse the message body to get delete data
        delete_data = json.loads(body)
        transaction_id = delete_data['transaction_id']
        user_group = delete_data['user_group']
        username = delete_data['username']

        print(f"Processing delete for transaction ID: {transaction_id}")

        # 1. AWS S3 credentials and path configuration
        bucket_name = generate_bucket_name(user_group)
        latest_parquet_file_name = get_latest_transaction_file_from_s3(bucket_name)
        s3_path_latest = f"s3://{bucket_name}/{latest_parquet_file_name}"

        parquet_file_name = generate_parquet_file_name()
        s3_path = f"s3://{bucket_name}/{parquet_file_name}"

        # 2. Load the Parquet file from S3 into a DataFrame using s3fs and Pandas
        fs = s3fs.S3FileSystem(key=aws_access_key, secret=aws_secret_key)
        try:
            # Load the Parquet file into a Pandas DataFrame
            df = pd.read_parquet(s3_path_latest, filesystem=fs)
            print("Data loaded from S3 parquet file.")
        except Exception as e:
            print(f"Failed to read parquet file from S3: {str(e)}")
            ch.basic_nack(delivery_tag=method.delivery_tag)  # Nack the message in case of failure
            return

        # 3. Find the transaction by transaction_id and delete it
        transaction_df = df[df['transaction_id'] == transaction_id]
        if transaction_df.empty:
            print(f"Transaction with ID {transaction_id} not found.")
            ch.basic_nack(delivery_tag=method.delivery_tag)  # Nack if not found
            return

        # Remove the transaction from the DataFrame
        df = df[df['transaction_id'] != transaction_id]

        # 4. Write the updated DataFrame back to S3 in a new Parquet file using PyArrow and s3fs
        try:
            df.to_parquet(s3_path, engine='pyarrow', index=False, filesystem=fs)
            print(f"Parquet file updated and saved to S3 at {s3_path}")
            ch.basic_ack(delivery_tag=method.delivery_tag)  # Acknowledge the message upon success
        except Exception as e:
            print(f"Failed to write updated parquet file to S3: {str(e)}")
            ch.basic_nack(delivery_tag=method.delivery_tag)  # Nack the message if there's an error

    except Exception as e:
        print(f"Failed to process transaction deletion: {str(e)}")
        ch.basic_nack(delivery_tag=method.delivery_tag)
        
# RabbitMQ Consumer Setup
def start_consumer():
    credentials = pika.PlainCredentials('guest', 'guest')  # Replace with your credentials if needed
    connection_params  = pika.ConnectionParameters('localhost', 5672, '/', credentials)
    # Establish connection using BlockingConnection, not ConnectionParameters
    connection = pika.BlockingConnection(connection_params)

    channel = connection.channel()

    # Declare the queue to ensure it exists
    channel.queue_declare(queue='transaction_queue', durable=True)
    channel.queue_declare(queue='transaction_batch_queue', durable=True)
    channel.queue_declare(queue='transaction_update_queue', durable=True)
    channel.queue_declare(queue='transaction_delete_queue', durable=True)

    # Set up a consumer to process messages from the queue
    channel.basic_qos(prefetch_count=1)  # Process one message at a time
    channel.basic_consume(queue='transaction_queue', on_message_callback=process_transaction)
    channel.basic_consume(queue='transaction_batch_queue', on_message_callback=process_transaction_batch)
    channel.basic_consume(queue='transaction_update_queue', on_message_callback=process_update_transaction)
    channel.basic_consume(queue='transaction_delete_queue', on_message_callback=process_delete_transaction)

    print('Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()


