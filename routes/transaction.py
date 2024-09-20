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
from io import BytesIO
from fastapi import HTTPException, status
from sqlalchemy.orm import Session
from sqlalchemy import desc,inspect, or_
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


@router.post("/", response_model=TransactionInDB, status_code=status.HTTP_200_OK)
def create_transaction(
    transaction: TransactionCreate, 
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)  # JWT authentication
):
    try:
        print("Starting transaction process with PostgreSQL and RabbitMQ...")
        # 1. Retrieve the last created transaction in PostgreSQL
        last_transaction = db.query(Transaction).order_by(
            desc(Transaction.created_dt),
            desc(Transaction.transaction_id)
        ).first()
        
        if last_transaction:
            # Extract the numeric part from the last transaction ID
            last_num_id = int(last_transaction.transaction_id[2:6])  # e.g., 0005 -> 5
            # Increment the numeric part by 1
            new_num_id = str(last_num_id + 1).zfill(4)  # e.g., '0006'
        else:
            # If no previous transactions exist, start with '0001'
            new_num_id = '0001'

        # 2. Format the transaction date to 'ddmmyy'
        transaction_date_str = transaction.transaction_dt.strftime('%d%m%y')  # e.g., '210824'

        # 3. Extract the first two characters from the transaction channel
        channel_code = transaction.transaction_channel[:2].upper()  # e.g., 'ON'

        # 4. Construct the transaction_id
        transaction_id = f'TT{new_num_id}{transaction_date_str}{channel_code}'

        # 5. Check if the generated transaction_id already exists in PostgreSQL
        existing_transaction = db.query(Transaction).filter(Transaction.transaction_id == transaction_id).first()
        if existing_transaction:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Transaction ID already exists")

        # 6. Prepare the data for insertion into PostgreSQL
        transaction_data = transaction.dict(exclude={'transaction_id'})

        # 7. Insert the new transaction into PostgreSQL
        db_transaction = Transaction(
            transaction_id=transaction_id,
            **transaction_data
        )
        db.add(db_transaction)
        db.commit()
        db.refresh(db_transaction)

        print(f"Transaction inserted into PostgreSQL with ID: {transaction_id}")

        # 8. Prepare transaction data to be queued in RabbitMQ for Parquet insertion
        transaction_data_for_queue = {
            'transaction_id': transaction_id,
            'transaction_channel': transaction.transaction_channel,
            'transaction_dt': transaction.transaction_dt.strftime('%Y-%m-%d'),
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
            'user_group': current_user.group
        }

        # 9. Connect to RabbitMQ and publish transaction data
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))  # Adjust RabbitMQ server address if necessary
        channel = connection.channel()

        # Declare a queue (if not already exists)
        channel.queue_declare(queue='transaction_queue', durable=True)

        # Publish the message to RabbitMQ
        channel.basic_publish(
            exchange='',
            routing_key='transaction_queue',
            body=json.dumps(transaction_data_for_queue),
            properties=pika.BasicProperties(
                delivery_mode=2,  # Make the message persistent
            )
        )

        print(f"Transaction queued successfully with ID: {transaction_id} for Parquet insertion")

        # Close the RabbitMQ connection
        connection.close()

        # 10. Return the newly created transaction from PostgreSQL
        return db_transaction
    
    except HTTPException as http_exc:
        # Re-raise the HTTPException to ensure FastAPI handles it
        raise http_exc
    except Exception as e:
        print(f"Internal Server Error: {str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal Server Error")


@router.post("/group", status_code=status.HTTP_200_OK)
def create_transactions_in_batch(
    transactions: List[TransactionCreate],  # Accept an array of transactions
    db: Session = Depends(get_db),
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
        print("11")

        # Declare the queue
        channel.queue_declare(queue='transaction_queue', durable=True)

        # Prepare a list to collect all transaction data for the batch
        transaction_batch_data = []

        # Process each transaction in the batch
        for transaction in transactions:
            print("loop")
            # Retrieve the last created transaction in PostgreSQL
            last_transaction = db.query(Transaction).order_by(
                desc(Transaction.created_dt),
                desc(Transaction.transaction_id)
            ).first()
            print("22")
            if last_transaction:
                last_num_id = int(last_transaction.transaction_id[2:6])  # Extract numeric part, e.g., '0005' -> 5
                new_num_id = str(last_num_id + 1).zfill(4)  # Increment by 1 and zero-fill, e.g., '0006'
            else:
                new_num_id = '0001'  # If no previous transactions, start with '0001'
            print("33")
            # Format the transaction date and extract channel code
            transaction_date_str = transaction.transaction_dt.strftime('%d%m%y')  # Format 'ddmmyy'
            channel_code = transaction.transaction_channel[:2].upper()  # Get first two characters of the channel

            # Construct the transaction_id
            transaction_id = f'TT{new_num_id}{transaction_date_str}{channel_code}'

            # Check if transaction ID already exists
            existing_transaction = db.query(Transaction).filter(Transaction.transaction_id == transaction_id).first()
            print("existing_transaction",existing_transaction)
            if existing_transaction:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Transaction ID already exists")

            # Prepare the transaction data for PostgreSQL insertion
            transaction_data = transaction.dict(exclude={'transaction_id'})
            db_transaction = Transaction(
                transaction_id=transaction_id,
                **transaction_data
            )

            # Insert into PostgreSQL
            db.add(db_transaction)
            db.commit()
            db.refresh(db_transaction)

            print(f"Transaction inserted into PostgreSQL with ID: {transaction_id}")

            # Prepare transaction data to be queued in RabbitMQ for Parquet insertion
            transaction_data_for_queue = {
                'transaction_id': transaction_id,
                'transaction_channel': transaction.transaction_channel,
                'transaction_dt': transaction.transaction_dt.strftime('%Y-%m-%d'),
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
                'user_group': user_group  # Add user_group to the transaction data
            }

            print("transaction",transaction)

            # Append each transaction data to the batch list
            transaction_batch_data.append(transaction_data_for_queue)
            print("finish loop  ")
        print("transaction_batch_data",transaction_batch_data[0])
        # Publish the entire batch of transactions as an array to RabbitMQ
        channel.basic_publish(
            exchange='',
            routing_key='transaction_queue',
            body=json.dumps(transaction_batch_data[0]),  # Serialize transaction_batch_data as JSON array
            properties=pika.BasicProperties(
                delivery_mode=2,  # Make the message persistent
            )
        )
        print(f"Published batch of {len(transactions)} transactions to RabbitMQ.")

        # Close the RabbitMQ connection
        connection.close()

        print("Batch transactions sent to RabbitMQ successfully.")
        return {"status": f"Batch of {len(transactions)} transactions sent to RabbitMQ"}

    except HTTPException as http_exc:
        # Re-raise the HTTPException to ensure FastAPI handles it
        raise http_exc
    except Exception as e:
        print(f"Internal Server Error: {str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal Server Error")


@router.get("/", status_code=status.HTTP_200_OK)
def read_transactions(
    offset: int = 0,  # Changed parameter name from skip to offset
    limit: int = 10, 
    search: str = "",  # Added search parameter
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)  # JWT authentication
):
    # Base query
    query = db.query(Transaction)
    
    # Apply search filter if search term is provided
    if search:
        search_filter = or_(
            Transaction.transaction_id.ilike(f"%{search}%"),
            Transaction.transaction_channel.ilike(f"%{search}%"),
            Transaction.model_product.ilike(f"%{search}%"),
            Transaction.price_product.ilike(f"%{search}%"),
            Transaction.no_hp_cust.ilike(f"%{search}%"),
            Transaction.name_cust.ilike(f"%{search}%"),
            Transaction.city_cust.ilike(f"%{search}%"),
            Transaction.prov_cust.ilike(f"%{search}%"),
            Transaction.address_cust.ilike(f"%{search}%"),
            Transaction.instagram_cust.ilike(f"%{search}%"),
            Transaction.created_by.ilike(f"%{search}%"),
            Transaction.updated_by.ilike(f"%{search}%"),
            # You can add more fields as needed
        )
        query = query.filter(search_filter)
    
    # Query for the total count of the filtered transactions
    total_all_data = query.count()
        
    # Query for the paginated transactions
    transactions = query.order_by(desc(Transaction.created_dt)).offset(offset).limit(limit).all()
    
    total_data = len(transactions)

    # Return the transactions, the total count, the total count of all data, and the offset
    return {
        "total_data": total_data,
        "total_all_data": total_all_data,
        "offset": offset,
        "transactions": transactions
    }



@router.get("/{transaction_id}", response_model=TransactionInDB,  status_code=status.HTTP_200_OK)
def read_transaction(
    transaction_id: str,  # Change type to str since transaction_id is not an integer
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)  # JWT authentication
):
    transaction = db.query(Transaction).filter(Transaction.transaction_id == transaction_id).first()
    if not transaction:
        raise HTTPException(status_code=404, detail="Transaction not found")
    return transaction

@router.put("/{transaction_id}", response_model=TransactionInDB, status_code=status.HTTP_200_OK)
def update_transaction(
    transaction_id: str,  # Existing transaction ID to be updated
    transaction: TransactionUpdate, 
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)  # JWT authentication
):
    try:
        print(f"Starting transaction update for transaction ID {transaction_id}...")

        # 1. Fetch the existing transaction record from PostgreSQL
        db_transaction = db.query(Transaction).filter(Transaction.transaction_id == transaction_id).first()
        print("db_transaction",db_transaction)
        if not db_transaction:
            raise HTTPException(status_code=404, detail="Transaction not found")
        print("11")

        # 2. Extract the original num_id from the existing transaction_id
        original_num_id = transaction_id[2:6]  # Extract characters 3 to 6
        print("22")

        # 3. Format the transaction date to 'ddmmyy'
        transaction_date_str = transaction.transaction_dt.strftime('%d%m%y')  # e.g., '210824'

        # 4. Extract the first two characters from the transaction channel
        channel_code = transaction.transaction_channel[:2].upper()  # e.g., 'ON'

        # 5. Construct the new transaction_id using the original num_id
        new_transaction_id = f'TT{original_num_id}{transaction_date_str}{channel_code}'
        print("startupdate_data")
        # 6. Update the transaction fields
        update_data = transaction.dict(exclude_unset=True)  # Get the transaction data excluding unset fields
        for key, value in update_data.items():
            setattr(db_transaction, key, value)

        # 7. Update the transaction_id
        db_transaction.transaction_id = new_transaction_id

        # 8. Update the timestamps
        db_transaction.updated_by = current_user.username

        # 9. Commit the transaction and refresh the record in PostgreSQL
        db.commit()
        db.refresh(db_transaction)
        

        print(f"Transaction {transaction_id} updated in PostgreSQL with new ID {new_transaction_id}.")
        # 10. Prepare the data to be sent to RabbitMQ
        update_data_for_queue = {
            'transaction_id': transaction_id,
            'transaction_channel': update_data['transaction_channel'],
            'transaction_dt': update_data['transaction_dt'].strftime('%Y-%m-%d'),
            'model_product': update_data['model_product'],
            'kuantitas': update_data['kuantitas'],
            'price_product': update_data['price_product'],
            'no_hp_cust': update_data['no_hp_cust'],
            'name_cust': update_data['name_cust'],
            'city_cust': update_data['city_cust'],
            'prov_cust': update_data['prov_cust'],
            'address_cust': update_data['address_cust'],
            'instagram_cust': update_data['instagram_cust'],
            'created_by': current_user.username,
            'created_dt': update_data['created_dt'].strftime('%Y-%m-%d %H:%M:%S'),
            'updated_by': current_user.username,
            'updated_dt': update_data['updated_dt'].strftime('%Y-%m-%d %H:%M:%S'),
            'user_group': current_user.group
        }

        # 11. Setup RabbitMQ connection and publish the update message
        credentials = pika.PlainCredentials('guest', 'guest')
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost', 5672, '/', credentials))
        channel = connection.channel()

        # Declare the update queue if it doesn't already exist
        channel.queue_declare(queue='transaction_queue', durable=True)

        # Publish the update message to RabbitMQ
        channel.basic_publish(
            exchange='',
            routing_key='transaction_queue',
            body=json.dumps(update_data_for_queue),  # Serialize the update_data to JSON
            properties=pika.BasicProperties(
                delivery_mode=2,  # Make message persistent
            )
        )

        print(f"Update request for transaction ID {new_transaction_id} sent to RabbitMQ.")
        connection.close()

        # 12. Return the updated transaction
        return db_transaction
    except HTTPException as http_exc:
        # Re-raise the HTTPException to ensure FastAPI handles it
        raise http_exc
    except Exception as e:
        print(f"Internal Server Error: {str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal Server Error")


def object_as_dict(obj):
    return {c.key: getattr(obj, c.key) for c in inspect(obj).mapper.column_attrs}

@router.delete("/{transaction_id}", status_code=status.HTTP_200_OK)
def delete_transaction(
    transaction_id: str,  # Change type to str since transaction_id is not an integer
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)  # JWT authentication
):
    try:
        # 1. Fetch the transaction to be deleted
        db_transaction = db.query(Transaction).filter(Transaction.transaction_id == transaction_id).first()

        if not db_transaction:
            raise HTTPException(status_code=404, detail="Transaction not found")

        # Convert transaction object to dictionary for sending to RabbitMQ
        transaction_data = object_as_dict(db_transaction)
        print("db_transaction data:", transaction_data)

        delete_data_for_queue = {
            'transaction_id': transaction_id,
            'transaction_channel': transaction_data['transaction_channel'],
            'transaction_dt': transaction_data['transaction_dt'].strftime('%Y-%m-%d'),
            'model_product': transaction_data['model_product'],
            'kuantitas': transaction_data['kuantitas'],
            'price_product': transaction_data['price_product'],
            'no_hp_cust': transaction_data['no_hp_cust'],
            'name_cust': transaction_data['name_cust'],
            'city_cust': transaction_data['city_cust'],
            'prov_cust': transaction_data['prov_cust'],
            'address_cust': transaction_data['address_cust'],
            'instagram_cust': transaction_data['instagram_cust'],
            'created_by': current_user.username,
            'created_dt': transaction_data['created_dt'].strftime('%Y-%m-%d %H:%M:%S'),
            'updated_by': current_user.username,
            'updated_dt': transaction_data['updated_dt'].strftime('%Y-%m-%d %H:%M:%S'),
            'user_group': current_user.group
        }

        # 2. Delete the transaction from the database
        db.delete(db_transaction)
        db.commit()

        # 3. Setup RabbitMQ connection and publish the delete message
        try:
            credentials = pika.PlainCredentials('guest', 'guest')
            connection = pika.BlockingConnection(pika.ConnectionParameters('localhost', 5672, '/', credentials))
            channel = connection.channel()

            # Declare the queue if it doesn't already exist
            channel.queue_declare(queue='transaction_queue', durable=True)

            # Publish the delete message to RabbitMQ
            channel.basic_publish(
                exchange='',
                routing_key='transaction_queue',
                body=json.dumps(delete_data_for_queue),  # Serialize the delete_data to JSON
                properties=pika.BasicProperties(
                    delivery_mode=2,  # Make message persistent
                )
            )
            print(f"Transaction {transaction_id} delete message sent to RabbitMQ.")
            connection.close()

        except Exception as rabbitmq_error:
            print(f"Failed to publish delete message to RabbitMQ: {str(rabbitmq_error)}")
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to publish delete message to RabbitMQ")

        # 4. Return success response
        return {"status": "Delete Transaction Success", "transaction_id": transaction_id}

    except HTTPException as http_exc:
        # Handle specific HTTP exceptions and re-raise them
        raise http_exc
    except Exception as e:
        print(f"Internal Server Error: {str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal Server Error")
