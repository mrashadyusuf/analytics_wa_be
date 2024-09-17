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

    print("S3 response:")

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
        print("Starting transaction creation process...")
        user_group = current_user.group

        # 1. AWS S3 credentials and path configuration
        bucket_name = generate_bucket_name(user_group)

        # variable to get latest parquet file
        latest_parquet_file_name = get_latest_transaction_file_from_s3(bucket_name)
        s3_path_latest= f"s3://{bucket_name}/{latest_parquet_file_name}"

        # variable to create new  parquet file
        parquet_file_name =  generate_parquet_file_name()
        s3_path = f"s3://{bucket_name}/{parquet_file_name}"

        # 2. Create S3 client and ensure the bucket exists
        s3_client = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key, region_name=aws_region)

        try:
            s3_client.head_bucket(Bucket=bucket_name)
            print(f"Bucket {bucket_name} exists.")
        except ClientError:
            print(f"Bucket {bucket_name} does not exist. Creating bucket.")
            try:
                s3_client.create_bucket(
                    Bucket=bucket_name,
                    CreateBucketConfiguration={'LocationConstraint': aws_region}
                )
                print(f"Bucket {bucket_name} created successfully.")
            except ClientError as e:
                raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to create S3 bucket")

        # 3. Try to read the existing Parquet file from S3
        fs = s3fs.S3FileSystem(key=aws_access_key, secret=aws_secret_key)
        try:
            existing_data_df = pd.read_parquet(s3_path_latest, filesystem=fs)
            print("Existing data loaded from S3.")
        except Exception:
            print("Parquet file not found or unreadable. Creating a new one.")
            existing_data_df = pd.DataFrame()

        # 4. Generate a new transaction ID
        if not existing_data_df.empty:
            last_transaction = existing_data_df.iloc[-1]
            last_num_id = int(last_transaction['transaction_id'][2:6])
            new_num_id = str(last_num_id + 1).zfill(4)
        else:
            new_num_id = '0001'

        print("transaction_dt.st", transaction.transaction_dt, " type", type(transaction.transaction_dt))
        transaction_date_str = transaction.transaction_dt.strftime('%d%m%y')
        channel_code = transaction.transaction_channel[:2].upper()
        transaction_id = f'TT{new_num_id}{transaction_date_str}{channel_code}'
        
        print(f"Generated transaction ID: {transaction_id}")

        # print("Type of 'transaction_dt' in the first row:", type(existing_data_df.iloc[0]['transaction_dt']))
        print("Type of 'transaction_dt' new ", type(transaction.transaction_dt))
        transaction_dt_converted = pd.to_datetime(transaction.transaction_dt)


        # 5. Create a DataFrame for the new transaction
        new_transaction_df = pd.DataFrame([{
            'transaction_id': transaction_id,
            'transaction_channel': transaction.transaction_channel,
            'transaction_dt': transaction_dt_converted,
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
            'created_dt': transaction.created_dt.strftime('%Y-%m-%d %H:%M:%S'),  # Convert to custom string format
            'updated_by': current_user.username,
            'updated_dt': transaction.updated_dt.strftime('%Y-%m-%d %H:%M:%S'),  # Convert to custom string format,
        }])

        print("Transaction Date (transaction_dt):", type(new_transaction_df['transaction_dt'].iloc[0]))

        # 6. Combine the new transaction data with the existing data
        if not existing_data_df.empty:
            updated_df = pd.concat([existing_data_df, new_transaction_df], ignore_index=True)
        else:
            updated_df = new_transaction_df

        print("New transaction appended to existing data.")

        # 7. Write the updated DataFrame back to S3 in Parquet format using PyArrow
        updated_df.to_parquet(s3_path, engine='pyarrow', index=False, filesystem=fs)
        print(f"Parquet file updated and saved to S3 at {s3_path}")

        print("Transaction process completed successfully.")
        return {"status": "Transaction successfully added to S3"}

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

        # 1. AWS S3 credentials and path configuration
        bucket_name = generate_bucket_name(user_group)

        # Variable to get the latest parquet file
        latest_parquet_file_name = get_latest_transaction_file_from_s3(bucket_name)
        s3_path_latest = f"s3://{bucket_name}/{latest_parquet_file_name}"

        # Variable to create new parquet file
        parquet_file_name = generate_parquet_file_name()
        s3_path = f"s3://{bucket_name}/{parquet_file_name}"

        # 2. Create S3 client and ensure the bucket exists
        s3_client = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key, region_name=aws_region)

        try:
            s3_client.head_bucket(Bucket=bucket_name)
            print(f"Bucket {bucket_name} exists.")
        except ClientError:
            print(f"Bucket {bucket_name} does not exist. Creating bucket.")
            try:
                s3_client.create_bucket(
                    Bucket=bucket_name,
                    CreateBucketConfiguration={'LocationConstraint': aws_region}
                )
                print(f"Bucket {bucket_name} created successfully.")
            except ClientError as e:
                raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to create S3 bucket")

        # 3. Try to read the existing Parquet file from S3
        fs = s3fs.S3FileSystem(key=aws_access_key, secret=aws_secret_key)
        try:
            existing_data_df = pd.read_parquet(s3_path_latest, filesystem=fs)
            print("Existing data loaded from S3.")
        except Exception:
            print("Parquet file not found or unreadable. Creating a new one.")
            existing_data_df = pd.DataFrame()

        # 4. Initialize the sequence number
        if not existing_data_df.empty:
            # Get the last num_id from the last transaction in the existing data
            last_transaction = existing_data_df.iloc[-1]
            last_num_id = int(last_transaction['transaction_id'][2:6])
        else:
            last_num_id = 0  # Start with 0001 if there are no previous transactions

        # 5. Process each transaction in the batch
        new_transactions = []
        for transaction in transactions:
            # Increment the num_id for each transaction in the batch
            last_num_id += 1
            new_num_id = str(last_num_id).zfill(4)

            # Format the transaction date to 'ddmmyy'
            transaction_date_str = transaction.transaction_dt.strftime('%d%m%y')

            # Extract the first two characters from the transaction channel
            channel_code = transaction.transaction_channel[:2].upper()

            # Construct the new transaction_id
            transaction_id = f'TT{new_num_id}{transaction_date_str}{channel_code}'

            print(f"Generated transaction ID: {transaction_id}")

            transaction_dt_converted = pd.to_datetime(transaction.transaction_dt)

            # Create a DataFrame for the new transaction
            new_transaction_df = pd.DataFrame([{
                'transaction_id': transaction_id,
                'transaction_channel': transaction.transaction_channel,
                'transaction_dt': transaction_dt_converted,
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
                'created_dt': transaction.created_dt.strftime('%Y-%m-%d %H:%M:%S'),  # Convert to custom string format
                'updated_by': current_user.username,
                'updated_dt': transaction.updated_dt.strftime('%Y-%m-%d %H:%M:%S'),
            }])

            new_transactions.append(new_transaction_df)

        # 6. Combine all the new transactions into a single DataFrame
        new_transactions_df = pd.concat(new_transactions, ignore_index=True)

        # 7. Combine the new transaction data with the existing data
        if not existing_data_df.empty:
            updated_df = pd.concat([existing_data_df, new_transactions_df], ignore_index=True)
        else:
            updated_df = new_transactions_df

        print("New transactions appended to existing data.")

        # 8. Write the updated DataFrame back to S3 in Parquet format using PyArrow
        updated_df.to_parquet(s3_path, engine='pyarrow', index=False, filesystem=fs)
        print(f"Parquet file updated and saved to S3 at {s3_path}")

        print("Batch transaction process completed successfully.")
        return {"status": "Batch transactions successfully added to S3"}

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

@router.put("/{transaction_id}", response_model=TransactionInDB, status_code=status.HTTP_200_OK)
def update_transaction(
    transaction_id: str,  # Existing transaction ID to be updated
    transaction: TransactionUpdate, 
    current_user: User = Depends(get_current_user)  # JWT authentication
):
    try:
        print("Starting transaction update process...")
        user_group = current_user.group

        # 1. AWS S3 credentials and path configuration
        bucket_name = generate_bucket_name(user_group)

        # Get the latest Parquet file and new Parquet file names
        latest_parquet_file_name = get_latest_transaction_file_from_s3(bucket_name)
        s3_path_latest = f"s3://{bucket_name}/{latest_parquet_file_name}"

        parquet_file_name = generate_parquet_file_name()
        s3_path = f"s3://{bucket_name}/{parquet_file_name}"

        # 2. Create S3 client and ensure the bucket exists
        s3_client = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key, region_name=aws_region)
        
        try:
            s3_client.head_bucket(Bucket=bucket_name)
            print(f"Bucket {bucket_name} exists.")
        except ClientError:
            print(f"Bucket {bucket_name} does not exist. Creating bucket.")
            try:
                s3_client.create_bucket(
                    Bucket=bucket_name,
                    CreateBucketConfiguration={'LocationConstraint': aws_region}
                )
                print(f"Bucket {bucket_name} created successfully.")
            except ClientError as e:
                raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to create S3 bucket")

        # 3. Load the Parquet file from S3 into a DataFrame using PyArrow and s3fs
        fs = s3fs.S3FileSystem(key=aws_access_key, secret=aws_secret_key)
        try:
            df = pd.read_parquet(s3_path_latest, filesystem=fs)
            print("Data loaded from S3 parquet file.")
        except Exception:
            raise HTTPException(status_code=404, detail="Parquet file not found or unreadable.")

        # 4. Fetch the existing transaction record
        transaction_df = df[df['transaction_id'] == transaction_id]
        print("Type of 'created_dt' in the first row:", type(transaction_df.iloc[0]['created_dt']))
        print("Transaction 'created_dt' type:", type(transaction.created_dt))

        transaction.created_dt = transaction.created_dt.strftime('%Y-%m-%d %H:%M:%S')
        transaction.updated_dt = transaction.updated_dt.strftime('%Y-%m-%d %H:%M:%S')

        if transaction_df.empty:
            raise HTTPException(status_code=404, detail="Transaction not found")

        # 5. Extract the original num_id from the existing transaction_id
        original_num_id = transaction_id[2:6]  # Extract characters 3 to 6

        # 6. Format the transaction date to 'ddmmyy'
        transaction_date_str = transaction.transaction_dt.strftime('%d%m%y')  # e.g., '210824'
        
        # 7. Extract the first two characters from the transaction channel
        channel_code = transaction.transaction_channel[:2].upper()  # e.g., 'ON'
        
        # 8. Construct the new transaction_id using the original num_id
        new_transaction_id = f'TT{original_num_id}{transaction_date_str}{channel_code}'

        # 9. Update the transaction fields
        for key, value in transaction.dict(exclude_unset=True).items():
            transaction_df.at[transaction_df.index[0], key] = value

        
        # 10. Update the transaction_id
        transaction_df.at[transaction_df.index[0], 'transaction_id'] = new_transaction_id

        # 11. Combine the updated transaction with the rest of the DataFrame
        df.update(transaction_df)

        # 12. Write the updated DataFrame back to S3 in a new Parquet file using PyArrow and s3fs
        try:
            df.to_parquet(s3_path, engine='pyarrow', index=False, filesystem=fs)
            print(f"New Parquet file created and saved to S3 at {s3_path}")
        except Exception as e:
            print(f"Failed to write updated parquet file to S3: {str(e)}")
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to update parquet file in S3")

        # 13. Return the updated transaction
        updated_transaction = transaction_df.iloc[0].to_dict()
        return updated_transaction

    except Exception as e:
        print(f"Internal Server Error: {str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal Server Error")


@router.delete("/{transaction_id}",   status_code=status.HTTP_200_OK)
def delete_transaction(
    transaction_id: str,  # Change type to str since transaction_id is not an integer
    current_user: User = Depends(get_current_user)  # JWT authentication
):
    try:
        # 1. Get the DuckDB connection using the helper function
        ()

        # 2. Form the S3 path
        user_group = current_user.group
        bucket_name = generate_bucket_name(user_group) 
        parquet_file_name = generate_parquet_file_name()
        s3_path = f"s3://{bucket_name}/{parquet_file_name}"

        # 3. Load the Parquet file from S3 into a DataFrame
        try:
            df = duckdb_conn.execute(f"SELECT * FROM read_parquet('{s3_path}')").fetchdf()
            print("Data loaded from S3 parquet file.")
        except Exception as e:
            print(f"Failed to read parquet file from S3: {str(e)}")
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to read parquet file from S3")

        # 4. Find the transaction by transaction_id
        transaction_df = df[df['transaction_id'] == transaction_id]

        # 5. If transaction not found, raise a 404 error
        if transaction_df.empty:
            raise HTTPException(status_code=404, detail="Transaction not found")

        # 6. Remove the transaction from the DataFrame
        df = df[df['transaction_id'] != transaction_id]

        # 7. Write the updated DataFrame back to the Parquet file in S3 using DuckDB
        try:
            # Register the updated DataFrame to DuckDB
            duckdb_conn.register("updated_transactions", df)
            
            # Write the updated DataFrame to the S3 Parquet file
            duckdb_conn.execute(f"COPY updated_transactions TO '{s3_path}' (FORMAT 'parquet');")
            print(f"Parquet file updated and saved to S3: {s3_path}")
        except Exception as e:
            print(f"Failed to write updated parquet file to S3: {str(e)}")
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to update parquet file in S3")

        # 8. Return success message
        return {"message": "Delete Transaction Success"}

    except Exception as e:
        print(f"Internal Server Error: {str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal Server Error")
