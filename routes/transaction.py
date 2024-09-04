from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from sqlalchemy import desc  
from typing import List

from database import get_db
from models.models import Transaction
from schemas.schemas import TransactionCreate, TransactionUpdate, TransactionInDB
from auth import get_current_user, User

router = APIRouter()


import pandas as pd
import duckdb
from io import BytesIO
from fastapi import HTTPException, status
from sqlalchemy.orm import Session
from sqlalchemy import desc
import os
import boto3
from botocore.exceptions import ClientError

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

parquet_file_name = "TRANSACTION/transaction.parquet"  # S3 folder is now "TRANSACTION"


@router.post("/", status_code=status.HTTP_200_OK)
def create_transaction(
    transaction: TransactionCreate, 
    current_user: User = Depends(get_current_user)  # JWT authentication
):
    try:
        print("Starting transaction creation process...")
        
        # 1. Create a DuckDB connection
        ()
        print("AWS S3 credentials configured.")

        # 4. Form the S3 path
        user_group = current_user.group
        bucket_name = generate_bucket_name(user_group) 
        s3_path = f"s3://{bucket_name}/{parquet_file_name}"

        # 5. Ensure the S3 bucket exists, create if it doesn't
        s3_client = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key, region_name=aws_region)

        try:
            s3_client.head_bucket(Bucket=bucket_name)
            print(f"Bucket {bucket_name} exists.")
        except ClientError:
            # If the bucket does not exist, create it
            print(f"Bucket {bucket_name} does not exist. Creating bucket.")
            try:
                s3_client.create_bucket(
                    Bucket=bucket_name,
                    CreateBucketConfiguration={'LocationConstraint': aws_region}
                )
                print(f"Bucket {bucket_name} created successfully.")
            except ClientError as e:
                print(f"Failed to create bucket: {str(e)}")
                raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to create S3 bucket")

        # 6. Try to read the Parquet file from S3
        try:
            existing_data_df = duckdb_conn.execute(f"SELECT * FROM read_parquet('{s3_path}')").fetchdf()
            print("Existing data loaded from S3.")
        except Exception as e:
            # If the file does not exist or there is an error reading it, start fresh
            print("Parquet file not found or unreadable. Creating a new one.")
            existing_data_df = pd.DataFrame()

        # 7. Generate a new transaction ID
        if not existing_data_df.empty:
            last_transaction = existing_data_df.iloc[-1]
            last_num_id = int(last_transaction['transaction_id'][2:6])
            new_num_id = str(last_num_id + 1).zfill(4)
        else:
            new_num_id = '0001'

        transaction_date_str = transaction.transaction_dt.strftime('%d%m%y')
        channel_code = transaction.transaction_channel[:2].upper()
        transaction_id = f'TT{new_num_id}{transaction_date_str}{channel_code}'
        
        print(f"Generated transaction ID: {transaction_id}")
        
        # 8. Create a DataFrame for the new transaction
        new_transaction_df = pd.DataFrame([{
            'transaction_id': transaction_id,
            'transaction_channel': transaction.transaction_channel,
            'transaction_dt': transaction.transaction_dt,
            'model_product': transaction.model_product,
            'kuantitas':transaction.kuantitas,
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
        print("DataFrame content:", new_transaction_df)
        print("DataFrame created from transaction data.")

        # 9. Combine the new transaction data with the existing data
        if not existing_data_df.empty:
            updated_df = pd.concat([existing_data_df, new_transaction_df], ignore_index=True)
        else:
            updated_df = new_transaction_df

        print("New transaction appended to existing data.")
        
        # 10. Register the combined DataFrame as a table in DuckDB
        duckdb_conn.register("updated_transaction_df", updated_df)
        print("Combined DataFrame registered as table in DuckDB.")

        # 11. Write the combined DataFrame back to S3 in Parquet format using COPY
        duckdb_conn.execute(f"COPY updated_transaction_df TO '{s3_path}' (FORMAT 'parquet');")
        print(f"Parquet file updated and saved to S3: {s3_path}")

        print("Transaction process completed successfully.")
        
        return {"status": "Transaction successfully added to S3"}

    except Exception as e:
        print(f"Internal Server Error: {str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal Server Error")

@router.get("/", status_code=status.HTTP_200_OK)
def read_transactions(
    offset: int = 0,  # Changed parameter name from skip to offset
    limit: int = 10, 
    search: str = "",  # Added search parameter
    db: Session = Depends(get_db),  # This might not be needed, remove if not used elsewhere
    current_user: User = Depends(get_current_user)  # JWT authentication
):
    try:
        print("Starting transaction retrieval process...")

        # 1. Create a DuckDB connection
        ()
        print("AWS S3 credentials configured.")

        # 4. Form the S3 path
        user_group = current_user.group
        bucket_name = generate_bucket_name(user_group) 
        s3_path = f"s3://{bucket_name}/{parquet_file_name}"

        # 5. Load the Parquet file from S3 into a DataFrame
        # Build the SQL query with filtering, sorting, and pagination
        try:
            # Initialize the base query
            query = f"""
            SELECT * FROM read_parquet('{s3_path}')
            """

            # 6. Apply search filter if search term is provided
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

            # 7. Sorting and pagination
            query += f"""
            ORDER BY updated_dt DESC
            LIMIT {limit} OFFSET {offset}
            """

            # Execute the query
            df = duckdb_conn.execute(query).fetchdf()
            print("Data loaded and filtered from S3 parquet file.")

            # 7. Query for the total count of the filtered transactions
            # If you need the total count for pagination purposes
            count_query = f"""
            SELECT COUNT(*) FROM read_parquet('{s3_path}')
            """
            if search:
                count_query += search_conditions

            total_all_data = duckdb_conn.execute(count_query).fetchone()[0]
            
        except Exception as e:
            print(f"Failed to read and filter parquet file from S3: {str(e)}")
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to read parquet file from S3")

        
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
        # 1. Get the DuckDB connection using the helper function
        ()

        # 2. Form the S3 path
        user_group = current_user.group
        bucket_name = generate_bucket_name(user_group) 
        s3_path = f"s3://{bucket_name}/{parquet_file_name}"

        # 3. Load the Parquet file from S3 into a DataFrame
        try:
            df = duckdb_conn.execute(f"SELECT * FROM read_parquet('{s3_path}')").fetchdf()
            print("Data loaded from S3 parquet file.")
        except Exception as e:
            print(f"Failed to read parquet file from S3: {str(e)}")
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to read parquet file from S3")

        # 4. Fetch the existing transaction record
        transaction_df = df[df['transaction_id'] == transaction_id]

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

        # 11. Replace the updated transaction back into the original DataFrame
        df.update(transaction_df)

        # 12. Write the updated DataFrame back to the Parquet file in S3 using DuckDB
        try:
            # Register the updated DataFrame to DuckDB
            duckdb_conn.register("updated_transactions", df)
            
            # Write the updated DataFrame to the S3 Parquet file
            duckdb_conn.execute(f"COPY updated_transactions TO '{s3_path}' (FORMAT 'parquet');")
            print(f"Parquet file updated and saved to S3: {s3_path}")
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
