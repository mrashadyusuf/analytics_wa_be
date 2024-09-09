from fastapi import Depends, HTTPException, status
import os
import boto3
import json
from datetime import datetime
import uuid
from auth import get_current_user, User
import duckdb
from botocore.exceptions import ClientError
import pandas as pd
from io import BytesIO
import pyarrow.parquet as pq

# Retrieve AWS credentials from environment variables
aws_access_key = os.getenv("ACCESS_KEY")
aws_secret_key = os.getenv("SECRET_KEY")
aws_region = os.getenv("REGION")


def generate_bucket_name():
    
    user_group = "teman-thrifty"
    return f'customer-{user_group.lower()}'


def getJsonFromAws(scheduler):
    session = boto3.Session(
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=aws_region
    )

    s3 = session.client('s3')

    # Nama bucket
    bucket_name = 'wa-scraper-tle'
    folder_name = '6282319000027-Febriyanto/'

    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_name)

    chat = []
    today = datetime.now().strftime('%Y-%m-%d')
    
    # Loop melalui semua objek di bucket
    for obj in response.get('Contents', []):
        # Cek jika file adalah file JSON dan berdasarkan tanggal LastModified
        if obj['Key'].endswith('.json') :
            print(f"Memproses extract file json: {obj['Key']}")
            if scheduler :
                print(f"=== Mulai Menjalankan scheduler chat wa ===")
                lastmodified = obj['LastModified'].strftime('%Y-%m-%d')

                if lastmodified == today :
                    # Mendapatkan file JSON dari S3
                    file_response = s3.get_object(Bucket=bucket_name, Key=obj['Key'])

                    # Membaca konten file
                    json_content = file_response['Body'].read().decode('utf-8')

                    # Parse konten JSON
                    data = json.loads(json_content)
                    kontak_name = obj['Key'].split('/')[1]

                    # Mengambil informasi yang diinginkan dari file JSON
                    try:
                        fromMe = data['_data']['id']['fromMe']
                        t = datetime.fromtimestamp(data['_data']['t'])
                        from_ = data['_data']['from']
                        to = data['_data']['to']
                        nama = kontak_name

                        no_hp = from_
                        if fromMe:
                            no_hp = to

                        chat.append({
                            'id': str(uuid.uuid4()),
                            'nama': nama,
                            'no_hp': no_hp.partition('@')[0],
                            'tgl': t
                        })
                        
                    except KeyError as e:
                        print(f"Error parsing JSON: {e}")
                print(f"=== Selesai Menjalankan scheduler chat wa ===")
            else:
                
                # Mendapatkan file JSON dari S3
                file_response = s3.get_object(Bucket=bucket_name, Key=obj['Key'])

                # Membaca konten file
                json_content = file_response['Body'].read().decode('utf-8')

                # Parse konten JSON
                data = json.loads(json_content)
                kontak_name = obj['Key'].split('/')[1]

                # Mengambil informasi yang diinginkan dari file JSON
                try:
                    fromMe = data['_data']['id']['fromMe']
                    t = datetime.fromtimestamp(data['_data']['t'])
                    from_ = data['_data']['from']
                    to = data['_data']['to']
                    nama = kontak_name

                    no_hp = from_
                    if fromMe:
                        no_hp = to

                    chat.append({
                        'id': str(uuid.uuid4()),
                        'nama': nama,
                        'no_hp': no_hp.partition('@')[0],
                        'tgl': t
                    })
                    
                except KeyError as e:
                    print(f"Error parsing JSON: {e}")
    
    return chat

def get_duckdb_connection():
    
    if not aws_access_key or not aws_secret_key:
        raise ValueError("AWS credentials are not set in the environment variables.")

    # Create a DuckDB connection
    bucket_name = generate_bucket_name()
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

def s3_path(transaction):
    parquet_prefix = "ETL/"
    if transaction:
        parquet_prefix = "TRANSACTION/"

    bucket_name = generate_bucket_name() 
    path = f"s3://{bucket_name}/{parquet_prefix}"

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

    return path

def getParquetFromAws(scheduler):
    session = boto3.Session(
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=aws_region
    )

    s3 = session.client('s3')

    # Nama bucket
    bucket_name = 'customer-teman-thrifty'
    folder_name = 'WA_SCRAPPER /Rashad Simpati/'

    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_name)

    chat = []
    today = datetime.now().strftime('%Y-%m-%d')
    
    # Loop melalui semua objek di bucket
    for obj in response.get('Contents', []):
        # Cek jika file adalah file parquet dan berdasarkan tanggal LastModified
        if obj['Key'].endswith('.parquet') :
            kontak_name = obj['Key'].split('/')[1]
            if scheduler :
                print(f"=== Mulai Menjalankan scheduler chat wa ===")
                lastmodified = obj['LastModified'].strftime('%Y-%m-%d')

                if lastmodified == today :
                    # Mendapatkan file parquet dari S3
                    file_response = s3.get_object(Bucket=bucket_name, Key=obj['Key'])
                    file_stream = BytesIO(file_response['Body'].read())
                    df = pd.read_parquet(file_stream)

                    # Mengambil informasi yang diinginkan dari file parquet
                    try:
                        fromMe = df['fromMe'].iloc[0]
                        t = df['timestamp'].iloc[0].strftime('%Y-%m-%d %H:%M:%S')
                        from_ = df['from'].iloc[0]
                        to = df['to'].iloc[0]
                        nama = kontak_name

                        no_hp = from_
                        if fromMe:
                            no_hp = to

                        chat.append({
                            'id': str(uuid.uuid4()),
                            'nama': nama,
                            'no_hp': no_hp,
                            'tanggal': t,
                            'is_end_chat': None,
                            'status': None
                        })
                        
                    except KeyError as e:
                        print(f"Error parsing JSON: {e}")
            else:
                # Mendapatkan file parquet dari S3
                file_response = s3.get_object(Bucket=bucket_name, Key=obj['Key'])
                file_stream = BytesIO(file_response['Body'].read())
                df = pd.read_parquet(file_stream)
                
                # Mengambil informasi yang diinginkan dari file parquet
                try:
                    fromMe = df['fromMe'].iloc[0]
                    t = df['timestamp'].iloc[0].strftime('%Y-%m-%d %H:%M:%S')
                    from_ = df['from'].iloc[0]
                    to = df['to'].iloc[0]
                    nama = kontak_name

                    no_hp = from_
                    if fromMe:
                        no_hp = to

                    chat.append({
                        'id': str(uuid.uuid4()),
                        'nama': nama,
                        'no_hp': no_hp,
                        'tanggal': t,
                        'is_end_chat': None,
                        'status': None
                    })
                    
                except KeyError as e:
                    print(f"Error parsing JSON: {e}")

    return chat