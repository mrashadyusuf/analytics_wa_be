from instaloader import Instaloader, Profile
from fastapi import APIRouter, Depends
from database import conn
from sqlalchemy.sql import text
import datetime
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import os
from instaloader.exceptions import TwoFactorAuthRequiredException
from models.etl_models import SumCustomerFollower
from utilities.aws import s3_path, get_duckdb_connection

load_dotenv()
username_ig = os.getenv('USERNAME_IG')
password = os.getenv('PASSWORD_IG')

async def schedule_get_follower(bucket):
    path = s3_path(bucket, None)
    tb_path = f"{path}tb_sum_customer_follower.parquet"
    duckdb_conn = get_duckdb_connection()
    query = """
            SELECT nama, instagram, follower FROM tb_sum_customer_follower WHERE follower IS NULL LIMIT 50
        """
    query = text(query)
    data = conn.execute(query).fetchall()

    # untuk update jumlah follower jika semua akun ig sudah di scraping
    if len(data) == 0 :
        query = """
                SELECT nama, instagram, follower FROM tb_sum_customer_follower WHERE updated_dt ::date < CURRENT_DATE LIMIT 50
            """
        query = text(query)
        data = conn.execute(query).fetchall()

    tanggal = datetime.datetime.now().strftime("%Y-%m-%d  %H:%M:%S")
    if len(data) > 0:
        L = Instaloader()
        try:
            L.login(username_ig, password)
        except TwoFactorAuthRequiredException:
            L.two_factor_login(2)

        qp = f"""
                SELECT nama, instagram, follower
                FROM read_parquet('{tb_path}') 
                        """
        df = duckdb_conn.execute(qp).fetchdf()
        
        for ig in data :
            username = ig.instagram
            if '/' in username:
                splitname = username.split('/')
                username = splitname[0]
            url = f'https://www.instagram.com/{username}/'
            response = requests.get(url)            
            soup = BeautifulSoup(response.text, 'html.parser')
            meta = soup.find('meta', attrs={'name': 'description'})
            if meta:
                try:
                    profile = Profile.from_username(L.context, username)
                    if ig.follower != profile.followers:
                        query = SumCustomerFollower.update().values(
                                        follower=profile.followers,
                                        updated_dt=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        ).where(SumCustomerFollower.c.instagram == ig.instagram, SumCustomerFollower.c.nama == ig.nama)
                        conn.execute(query)
                        conn.commit()
                        
                        # update parquetnya
                        df.loc[(df['nama'] == ig.nama) & (df['instagram'] == ig.instagram), 'follower'] = profile.followers
                        duckdb_conn.execute(f"COPY df TO '{tb_path}' (FORMAT PARQUET)")

                except Exception:
                    print(f'\033[1GAGAL\033[0m get follower akun instagram {username} tanggal {tanggal}')

        duckdb_conn.close()
            
    selesai = datetime.datetime.now().strftime("%Y-%m-%d  %H:%M:%S")
    print(f"\033[1mSukses\033[0m update data follower, sebanyak {len(data)} username. mulai {tanggal} selesai {selesai}")
    