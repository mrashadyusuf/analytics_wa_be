from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from sqlalchemy import desc  
from typing import List

from database import get_db
from models.models import Transaction
from schemas.schemas import TransactionCreate, TransactionUpdate, TransactionInDB
from auth import get_current_user, User

router = APIRouter()

from datetime import date, datetime
import pandas as pd
import duckdb
from io import BytesIO
from fastapi import HTTPException, status
from sqlalchemy.orm import Session
from sqlalchemy import desc
import os
import boto3
from botocore.exceptions import ClientError

import pyarrow as pa
import pyarrow.parquet as pq
import s3fs

import pika
import json

# Retrieve AWS credentials from environment variables
aws_access_key = os.getenv("ACCESS_KEY")
aws_secret_key = os.getenv("SECRET_KEY")
aws_region = os.getenv("REGION")


def get_duckdb_connection():

    if not aws_access_key or not aws_secret_key:
        raise ValueError("AWS credentials are not set in the environment variables.")

    # Create a DuckDB connection
    duckdb_conn = duckdb.connect()
    print("DuckDB connection established.")

    # Configure S3 credentials for DuckDB
    duckdb_conn.execute(f"""
        CREATE SECRET (
            TYPE S3,
            KEY_ID '{aws_access_key}',
            SECRET '{aws_secret_key}',
            REGION '{aws_region}'
        );
    """)
    print("AWS S3 credentials configured.")
    
    return duckdb_conn

duckdb_conn = get_duckdb_connection()

def generate_bucket_name(user_group: str) -> str:
    return f'customer-{user_group.lower()}'

prefix_parquet_file_name= "TRANSACTION/transaction-"
# parquet_file_name = prefix_parquet_file_name+ f"{ datetime.now()}.parquet"  # S3 folder is now "TRANSACTION"

def generate_parquet_file_name() -> str:
    return prefix_parquet_file_name+ f"{ datetime.now()}.parquet"



# Separate function to get the latest file from S3 based on the prefix
def get_latest_transaction_file_from_s3(bucket_name: str):
    # 1. AWS S3 setup with boto3 client
    s3_client = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key, region_name=aws_region)
    
    # 2. List all files in the bucket with the specified prefix
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix_parquet_file_name)
    files = response.get('Contents', [])

    # 3. Find the file with the most recent LastModified timestamp
    if not files:
        print("No files found")
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No files found in the bucket.")

    latest_file = max(files, key=lambda f: f['LastModified'])  # Find the file with the latest LastModified
    latest_file_key = latest_file['Key']  # Get the S3 key of the latest file

    print(f"Latest file selected: {latest_file_key}")
    return latest_file_key  # Return the key of the latest file



@router.post("/", status_code=status.HTTP_200_OK)
def create_transaction(
    transaction: TransactionCreate, 
    current_user: User = Depends(get_current_user)  # JWT authentication
):
    try:
        print("Starting transaction queuing process...")
        user_group = current_user.group

        # 1. AWS S3 credentials and path configuration
        bucket_name = generate_bucket_name(user_group)

        # variable to get the latest parquet file
        latest_parquet_file_name = get_latest_transaction_file_from_s3(bucket_name)
        s3_path_latest = f"s3://{bucket_name}/{latest_parquet_file_name}"

        # 2. Try to read the existing Parquet file from S3
        fs = s3fs.S3FileSystem(key=aws_access_key, secret=aws_secret_key)
        try:
            existing_data_df = pd.read_parquet(s3_path_latest, filesystem=fs)
            print("Existing data loaded from S3.")
        except Exception:
            print("Parquet file not found or unreadable. Creating a new one.")
            existing_data_df = pd.DataFrame()

        # 3. Generate new_num_id based on the last transaction in the existing Parquet file
        if not existing_data_df.empty:
            last_transaction = existing_data_df.iloc[-1]
            last_num_id = int(last_transaction['transaction_id'][2:6])  # Extract the numeric part from 'TTxxxx'
            new_num_id = str(last_num_id + 1).zfill(4)  # Increment and zero-fill to 4 digits
        else:
            new_num_id = '0001'  # Start with '0001' if no transactions exist

        print(f"Generated new_num_id: {new_num_id}")

        # 4. Generate Transaction ID
        transaction_date_str = transaction.transaction_dt.strftime('%d%m%y')
        channel_code = transaction.transaction_channel[:2].upper()
        transaction_id = f'TT{new_num_id}{transaction_date_str}{channel_code}'
        
        print(f"Generated transaction ID: {transaction_id}")

        # Prepare transaction data to be queued
        transaction_data = {
            'transaction_id': transaction_id,
            'transaction_channel': transaction.transaction_channel,
            'transaction_dt': transaction.transaction_dt.strftime('%Y-%m-%d %H:%M:%S'),
            'model_product': transaction.model_product,
            'kuantitas': transaction.kuantitas,
            'price_product': transaction.price_product,
            'no_hp_cust': transaction.no_hp_cust,
            'name_cust': transaction.name_cust,
            'city_cust': transaction.city_cust,
            'prov_cust': transaction.prov_cust,
            'address_cust': transaction.address_cust,
            'instagram_cust': transaction.instagram_cust,
            'created_by': current_user.username,
            'created_dt': transaction.created_dt.strftime('%Y-%m-%d %H:%M:%S'),
            'updated_by': current_user.username,
            'updated_dt': transaction.updated_dt.strftime('%Y-%m-%d %H:%M:%S')
        }
        transaction_data['user_group'] = user_group
        # 5. Connect to RabbitMQ and publish transaction data
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))  # Adjust RabbitMQ server address if necessary
        channel = connection.channel()

        # Declare a queue (if not already exists)
        channel.queue_declare(queue='transaction_queue', durable=True)

        # Publish the message
        channel.basic_publish(
            exchange='',
            routing_key='transaction_queue',
            body=json.dumps(transaction_data),
            properties=pika.BasicProperties(
                delivery_mode=2,  # Make the message persistent
            )
        )

        print(f"Transaction queued successfully with ID: {transaction_id}")

        # Close the connection
        connection.close()

        return {"status": f"Transaction {transaction_id} queued successfully."}

    except Exception as e:
        print(f"Internal Server Error: {str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal Server Error")


@router.post("/group", status_code=status.HTTP_200_OK)
def create_transactions_in_batch(
    transactions: List[TransactionCreate],  # Accept an array of transactions
    current_user: User = Depends(get_current_user)  # JWT authentication
):
    try:
        print("Starting batch transaction creation process...")
        user_group = current_user.group

        # Set up RabbitMQ connection and channel
        credentials = pika.PlainCredentials('guest', 'guest')  # Modify if you use different credentials
        connection_params = pika.ConnectionParameters('localhost', 5672, '/', credentials)  # Modify 'localhost' if necessary
        connection = pika.BlockingConnection(connection_params)
        channel = connection.channel()

        # Declare the queue
        channel.queue_declare(queue='transaction_queue', durable=True)

        # Process each transaction in the batch
        for transaction in transactions:
            # Prepare transaction data to be sent to RabbitMQ
            transaction_data = {
                'transaction_channel': transaction.transaction_channel,
                'transaction_dt': transaction.transaction_dt.strftime('%Y-%m-%d %H:%M:%S'),
                'model_product': transaction.model_product,
                'kuantitas': transaction.kuantitas,
                'price_product': transaction.price_product,
                'no_hp_cust': transaction.no_hp_cust,
                'name_cust': transaction.name_cust,
                'city_cust': transaction.city_cust,
                'prov_cust': transaction.prov_cust,
                'address_cust': transaction.address_cust,
                'instagram_cust': transaction.instagram_cust,
                'created_by': current_user.username,
                'created_dt': transaction.created_dt.strftime('%Y-%m-%d %H:%M:%S'),
                'updated_by': current_user.username,
                'updated_dt': transaction.updated_dt.strftime('%Y-%m-%d %H:%M:%S'),
                'user_group': user_group  # Add user_group to transaction
            }

            # Publish the transaction to RabbitMQ
            channel.basic_publish(
                exchange='',
                routing_key='transaction_batch_queue',
                body=json.dumps(transaction_data),  # Serialize transaction_data as JSON
                properties=pika.BasicProperties(
                    delivery_mode=2,  # Make the message persistent
                )
            )
            print(f"Published transaction for {transaction.name_cust} to RabbitMQ.")

        # Close the RabbitMQ connection
        connection.close()

        print("Batch transactions sent to RabbitMQ successfully.")
        return {"status": "Batch transactions sent to RabbitMQ"}

    except Exception as e:
        print(f"Internal Server Error: {str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal Server Error")


@router.get("/", status_code=status.HTTP_200_OK)
def read_transactions(
    offset: int = 0,  # Pagination offset
    limit: int = 10,  # Pagination limit
    search: str = "",  # Search query
    db: Session = Depends(get_db),  # This might not be needed, remove if not used elsewhere
    current_user: User = Depends(get_current_user)  # JWT authentication
):
    try:
        print("Starting transaction retrieval process...")

        bucket_name = generate_bucket_name(current_user.group) 
        latest_file_key = get_latest_transaction_file_from_s3(bucket_name)
        print(f"Latest file selected: {latest_file_key}")



        # 4. Form the S3 path for the latest file
        s3_path = f"s3://{bucket_name}/{latest_file_key}"

        # 5. Load the Parquet file from S3 into a DataFrame
        query = f"SELECT * FROM read_parquet('{s3_path}')"

        # 6. Apply search filter if a search term is provided
        if search:
            search_conditions = f"""
            WHERE
                transaction_id ILIKE '%{search}%' OR
                transaction_channel ILIKE '%{search}%' OR
                model_product ILIKE '%{search}%' OR
                price_product ILIKE '%{search}%' OR
                no_hp_cust ILIKE '%{search}%' OR
                name_cust ILIKE '%{search}%' OR
                city_cust ILIKE '%{search}%' OR
                prov_cust ILIKE '%{search}%' OR
                address_cust ILIKE '%{search}%' OR
                instagram_cust ILIKE '%{search}%' OR
                created_by ILIKE '%{search}%' OR
                updated_by ILIKE '%{search}%'
            """
            query += search_conditions

        # 7. Add sorting and pagination
        query += f" ORDER BY updated_dt DESC LIMIT {limit} OFFSET {offset}"

        # Execute the query
        df = duckdb_conn.execute(query).fetchdf()
        print("Data loaded and filtered from S3 parquet file.")

        # 8. Get the total count of the filtered transactions
        count_query = f"SELECT COUNT(*) FROM read_parquet('{s3_path}')"
        if search:
            count_query += search_conditions

        total_all_data = duckdb_conn.execute(count_query).fetchone()[0]

        total_data = len(df)

        # Convert DataFrame to a list of dictionaries to return as JSON
        transactions = df.to_dict(orient='records')

        # 9. Return the transactions, the total count, the total count of all data, and the offset
        return {
            "total_data": total_data,
            "total_all_data": total_all_data,
            "offset": offset,
            "transactions": transactions
        }

    except Exception as e:
        print(f"Internal Server Error: {str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal Server Error")


@router.get("/{transaction_id}", response_model=TransactionInDB,  status_code=status.HTTP_200_OK)
def read_transaction_by_id(
    transaction_id: str,  # Change type to str since transaction_id is not an integer
    db: Session = Depends(get_db),  # This might not be needed, remove if not used elsewhere
    current_user: User = Depends(get_current_user)  # JWT authentication
):
    try:
        print("Starting transaction retrieval process...")

        # 1. Get the DuckDB connection using the helper function
        ()

        # 2. Form the S3 path
        user_group = current_user.group
        bucket_name = generate_bucket_name(user_group) 
        parquet_file_name = get_latest_transaction_file_from_s3(bucket_name)
        s3_path = f"s3://{bucket_name}/{parquet_file_name}"

        # 3. Load the Parquet file from S3 and filter by transaction_id using a SQL WHERE clause
        try:
            query = f"""
            SELECT * FROM read_parquet('{s3_path}')
            WHERE transaction_id = '{transaction_id}'
            """
            transaction_df = duckdb_conn.execute(query).fetchdf()
            print("Transaction data loaded from S3 parquet file.")
        except Exception as e:
            print(f"Failed to read parquet file from S3: {str(e)}")
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to read parquet file from S3")

        # 4. Check if the transaction was found
        if transaction_df.empty:
            raise HTTPException(status_code=404, detail="Transaction not found")

        # 5. Check if the transaction was found
        if transaction_df.empty:
            raise HTTPException(status_code=404, detail="Transaction not found")

        # Convert the first (and only) result to a dictionary
        transaction_data = transaction_df.iloc[0].to_dict()

        # 6. Return the transaction data
        return TransactionInDB(**transaction_data)

    except Exception as e:
        print(f"Internal Server Error: {str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal Server Error")


@router.put("/{transaction_id}", status_code=status.HTTP_200_OK)
def update_transaction(
    transaction_id: str,  # Existing transaction ID to be updated
    transaction: TransactionUpdate, 
    current_user: User = Depends(get_current_user)  # JWT authentication
):
    try:
        print("Publishing transaction update request to RabbitMQ...")
        print("transaction update",transaction)
        transaction.transaction_dt = transaction.transaction_dt.strftime('%Y-%m-%d')
        transaction.created_dt = transaction.created_dt.strftime('%Y-%m-%d %H:%M:%S')
        transaction.updated_dt = transaction.updated_dt.strftime('%Y-%m-%d %H:%M:%S')
        # Prepare the data to be sent to RabbitMQ
        update_data = {
            'transaction_id': transaction_id,
            'transaction_data': transaction.dict(exclude_unset=True),
            'user_group': current_user.group,
            'username': current_user.username
        }

        # Setup RabbitMQ connection and publish the update message
        credentials = pika.PlainCredentials('guest', 'guest')
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost', 5672, '/', credentials))
        channel = connection.channel()

        # Declare the update queue if it doesn't already exist
        channel.queue_declare(queue='transaction_update_queue', durable=True)

        # Publish the update message to RabbitMQ
        channel.basic_publish(
            exchange='',
            routing_key='transaction_update_queue',
            body=json.dumps(update_data),  # Serialize the update_data to JSON
            properties=pika.BasicProperties(
                delivery_mode=2,  # Make message persistent
            )
        )

        print(f"Update request for transaction ID {transaction_id} sent to RabbitMQ.")
        connection.close()

        return {"status": "Update request sent to RabbitMQ", "transaction_id": transaction_id}

    except Exception as e:
        print(f"Internal Server Error: {str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal Server Error")


@router.delete("/{transaction_id}",   status_code=status.HTTP_200_OK)
def delete_transaction(
    transaction_id: str,  # Existing transaction ID to delete
    current_user: User = Depends(get_current_user)  # JWT authentication
):
    try:
        print("Publishing delete transaction request to RabbitMQ...")

        # Prepare the data to be sent to RabbitMQ
        delete_data = {
            'transaction_id': transaction_id,
            'user_group': current_user.group,
            'username': current_user.username
        }

        # Setup RabbitMQ connection and publish the delete message
        credentials = pika.PlainCredentials('guest', 'guest')
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost', 5672, '/', credentials))
        channel = connection.channel()

        # Declare the delete queue if it doesn't already exist
        channel.queue_declare(queue='transaction_delete_queue', durable=True)

        # Publish the delete message to RabbitMQ
        channel.basic_publish(
            exchange='',
            routing_key='transaction_delete_queue',
            body=json.dumps(delete_data),  # Serialize the delete_data to JSON
            properties=pika.BasicProperties(
                delivery_mode=2,  # Make message persistent
            )
        )

        print(f"Delete request for transaction ID {transaction_id} sent to RabbitMQ.")
        connection.close()

        return {"status": "Delete request sent to RabbitMQ", "transaction_id": transaction_id}

    except Exception as e:
        print(f"Internal Server Error: {str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal Server Error")
