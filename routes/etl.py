from fastapi import APIRouter, Response
from models.etl_models import SumAverageSales,ChatWa,SumCustomer,Transaksi,SumCustomerFollower,SumModel,SumRegion,SumSalesTrend,SumSalesTrendPertanggal,SumStore,SumTopProduk,SumTransaksi,SumWa
from database import conn, engine
import duckdb
from sqlalchemy.sql import text
import pandas as pd
import datetime
# from utilities.duckdb import duckdb_conn, parquet_file_name
from utilities.aws import getJsonFromAws, s3_path, get_duckdb_connection, getParquetFromAws


etl = APIRouter()
# duckdb_conn = duckdb.connect('teman-trifty.duckdb')

@etl.get('/', description="Menampilkan detail data")
async def find_role(response: Response):
    
    scheduler = False
    mulai = datetime.datetime.now()
    
    await data_transaksi(scheduler)
    await chat_wa(scheduler)
    await sum_transaksi()
    await sum_average_sales()
    await sum_customer()
    await sum_model()
    await sum_region()
    await sum_sales_trend()
    await sum_sales_trend_pertanggal()
    await sum_store()
    await sum_top_produk()
    await sum_wa()
    await sum_customer_follower()
    
    selesai = datetime.datetime.now()
    durasi = selesai - mulai
    print("selama ", durasi)
    response = {"message": f"sukses menjalankan semua fungsi ETL dengan durasi {durasi}" }
    return response


async def data_transaksi(scheduler):    
    path = s3_path(transaction = "transaksi")
    tb_path = f"{path}transaction.parquet"
    duckdb_conn = get_duckdb_connection()
    query = f"""
        SELECT transaction_id, transaction_dt, STRFTIME(transaction_dt, '%d') AS tanggal, 
        STRFTIME(transaction_dt, '%b') bulan, STRFTIME(transaction_dt,'%Y') tahun, name_cust, 
        model_product, address_cust, no_hp_cust, prov_cust, city_cust,
        LOWER(SPLIT_PART(REGEXP_REPLACE(instagram_cust, '^[: @]+', ''), ' ', 1)) AS instagram, transaction_channel, 
        price_product, kuantitas
        FROM read_parquet('{tb_path}')
    """
    if scheduler :
        query = query + \
           f""" WHERE created_dt::date = CURRENT_DATE
        """

    q = """SELECT id, tgl_transaksi, tanggal, bulan, tahun, nama, model, alamat, no_telp, provinsi, kota_kab, 
        LOWER(SPLIT_PART(REGEXP_REPLACE(instagram, '^[: @]+', ''), ' ', 1)) AS instagram, store, harga, kuantitas
        FROM tb_transaksi"""
    q = text(q)
    dataPostgre = conn.execute(q).fetchall()

    data = duckdb_conn.execute(query).fetchall()
    
    dataTransaksi = dataPostgre + data

    df = pd.DataFrame(dataTransaksi, columns=[
        'id', 'tgl_transaksi', 'tanggal', 
        'bulan', 'tahun', 'nama', 'model', 
        'alamat', 'no_telp', 'provinsi', 'kota_kab',
        'instagram', 'store', 'harga', 'kuantitas'
    ])
    duckdb_conn.register('transaction_view', df)
    duckdb_conn.execute("""
                    CREATE TABLE IF NOT EXISTS tb_transaksi (
                    id varchar,
                    tgl_transaksi date,
                    tanggal varchar,
                    bulan varchar,
                    tahun varchar,
                    nama varchar,
                    model varchar,
                    alamat varchar,
                    no_telp varchar,
                    provinsi varchar,
                    kota_kab varchar,
                    instagram varchar,
                    store varchar,
                    harga int4,
                    kuantitas int4
        )
    """)
    path = s3_path(transaction = None)
    # duckdb_conn.execute("CREATE TABLE IF NOT EXISTS tb_transaksi AS SELECT * FROM transaction_view")
    duckdb_conn.execute("INSERT INTO tb_transaksi SELECT * FROM transaction_view WHERE id NOT IN (SELECT id FROM tb_transaksi)")
    duckdb_conn.execute(f"COPY tb_transaksi TO '{path}tb_transaksi.parquet' (FORMAT PARQUET)")
    
    # Tutup koneksi setelah selesai
    duckdb_conn.close()
    

async def sum_transaksi():
    path = s3_path(transaction = None)
    tb_path = f"{path}tb_transaksi.parquet"
    duckdb_conn = get_duckdb_connection()
    pq = f"""
        SELECT strftime(tgl_transaksi, '%Y %m') AS tgl, tahun, provinsi, model, store, 
        SUM(harga) total_harga, COUNT(*) jumlah, CASE 
            WHEN SUM(kuantitas) IS NULL THEN
                0
            ELSE
                SUM(kuantitas)
        END AS kuantitas
        FROM read_parquet('{tb_path}')
        WHERE provinsi IS NOT NULL AND model IS NOT NULL
        GROUP BY 1,2,3,4,5
    """

    resParquet = duckdb_conn.execute(pq).fetchall()

    df = pd.DataFrame(resParquet, columns=[
        'tgl', 'tahun', 'provinsi', 'model', 'store', 'total_harga', 'jumlah', 'kuantitas'
    ])

    duckdb_conn.register('tb_sum_transaksi_view', df)
    duckdb_conn.execute("""
                    CREATE TABLE IF NOT EXISTS tb_sum_transaksi (
                    tgl varchar,
                    tahun varchar,
                    provinsi varchar,
                    model varchar,
                    store varchar,
                    total_harga int8,
                    jumlah int8,
                    kuantitas int4
        )
    """)
    duckdb_conn.execute("DELETE FROM tb_sum_transaksi")
    duckdb_conn.execute("INSERT INTO tb_sum_transaksi SELECT * FROM tb_sum_transaksi_view ")
    duckdb_conn.execute(f"COPY tb_sum_transaksi TO '{path}tb_sum_transaksi.parquet' (FORMAT PARQUET)")

    if len(resParquet) > 0 :
        trunc = """ TRUNCATE TABLE tb_sum_transaksi;"""
        textQuery = text(trunc)
        conn.execute(textQuery)
        conn.commit()
        for s in resParquet:
            query = SumTransaksi.insert().values(
                tgl = s[0],
                tahun = s[1],
                provinsi = s[2],
                model = s[3],
                store = s[4],
                total_harga = s[5],
                jumlah = s[6],
                kuantitas = s[6],
                updated_dt = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            )
            conn.execute(query)
            conn.commit()
    duckdb_conn.close()

async def chat_wa(scheduler):
    duckdb_conn = get_duckdb_connection()
    path = s3_path(transaction = None)
    
    # Membuat tabel jika belum ada
    duckdb_conn.execute("""
        CREATE TABLE IF NOT EXISTS tb_chat_wa (
            id varchar,
            nama varchar,
            no_hp varchar,
            tanggal datetime,
            is_end_chat boolean default null,
            status varchar default null
        )
    """)
    # chatFile = getJsonFromAws(scheduler)
    chatFile = getParquetFromAws(scheduler)
    
    df = pd.DataFrame(chatFile, columns=[
        'id', 'nama', 'no_hp', 'tanggal', 'is_end_chat','status'
    ])

    duckdb_conn.register('tb_chat_wa_view', df)

    if len(chatFile) > 0 :
        duckdb_conn.execute("INSERT INTO tb_chat_wa SELECT * FROM tb_chat_wa_view WHERE nama NOT IN (SELECT nama FROM tb_chat_wa) AND no_hp NOT IN (SELECT no_hp FROM tb_chat_wa) AND tanggal NOT IN (SELECT CAST(tanggal AS VARCHAR) FROM tb_chat_wa)")
        duckdb_conn.execute(f"COPY tb_chat_wa TO '{path}tb_chat_wa.parquet' (FORMAT PARQUET)")
    
    duckdb_conn.close()



async def sum_wa():
    path = s3_path(transaction = None)
    tb_path = f"{path}tb_chat_wa.parquet"
    duckdb_conn = get_duckdb_connection()
    query = f"""
        SELECT *
            FROM (
                SELECT DISTINCT ON (no_hp, nama)
                no_hp, nama, tanggal, TRIM(CASE
                    WHEN EXTRACT(YEAR FROM age(NOW(), tanggal)) > 0 THEN EXTRACT(YEAR FROM age(NOW(), tanggal)) || ' tahun '
                    ELSE ''
                    END ||
                    CASE
                    WHEN EXTRACT(MONTH FROM age(NOW(), tanggal)) > 0 THEN EXTRACT(MONTH FROM age(NOW(), tanggal)) || ' bulan '
                    ELSE ''
                    END ||
                    CASE
                    WHEN EXTRACT(DAY FROM age(NOW(), tanggal)) > 0 THEN EXTRACT(DAY FROM age(NOW(), tanggal)) || ' hari '
                    ELSE '0 hari '
                    END
                ) AS terakhir_dihubungi
                FROM read_parquet('{tb_path}')
                WHERE tanggal IS NOT NULL
                ORDER BY no_hp, nama, tanggal DESC
            ) AS subquery
        ORDER BY tanggal ASC
    """
    summ = duckdb_conn.execute(query).fetchall()
    
    df = pd.DataFrame(summ, columns=[
        'no_hp', 'nama', 'tanggal', 'terakhir_dihubungi'
    ])
    duckdb_conn.register('tb_sum_wa_view', df)
    duckdb_conn.execute("""
                    CREATE TABLE IF NOT EXISTS tb_sum_wa (
                    no_hp varchar,
                    nama varchar,
                    tanggal datetime,
                    terakhir_dihubungi varchar
        )
    """)
    duckdb_conn.execute("DELETE FROM tb_sum_wa")
    duckdb_conn.execute("INSERT INTO tb_sum_wa SELECT * FROM tb_sum_wa_view ")
    duckdb_conn.execute(f"COPY tb_sum_wa TO '{path}tb_sum_wa.parquet' (FORMAT PARQUET)")

    if len(summ) > 0 :
        trunc = """ TRUNCATE TABLE tb_sum_wa;"""
        textQuery = text(trunc)
        conn.execute(textQuery)
        conn.commit()
        for s in summ:
            query = SumWa.insert().values(
                no_hp = s[0],
                nama = s[1],
                tanggal = s[2],
                terakhir_dihubungi = s[3],
                updated_dt = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            )
            conn.execute(query)
            conn.commit()
    duckdb_conn.close()
    
async def sum_average_sales():
    path = s3_path(transaction = None)
    tb_path = f"{path}tb_transaksi.parquet"
    duckdb_conn = get_duckdb_connection()
    query = f"""
        SELECT x.tgl AS tanggal, x.tahun, x.provinsi, x.store AS channel, x.total_harga, CASE 
            WHEN x.kuantitas IS NULL THEN
                0
            ELSE
                x.kuantitas
        END AS kuantitas, x.jumlah_transaksi, x.avg_bill, ROUND(AVG(x.jumlah_produk),2) avg_basket
        FROM
        (
        SELECT strftime(tgl_transaksi, '%Y %m') AS tgl, tahun, provinsi, store, SUM(kuantitas) kuantitas, 
        SUM(harga) total_harga, COUNT(*) jumlah_transaksi, 
        COUNT(model) jumlah_produk, ROUND(AVG(harga),2) avg_bill, (COUNT(model) / COUNT(*)) avg_basket
        FROM read_parquet('{tb_path}')
        WHERE provinsi IS NOT NULL
        GROUP BY 1,2,3,4
        ) as x
        GROUP BY 1,2,3,4,5,6,7,8
        ORDER BY 2
    """
    summ = duckdb_conn.execute(query).fetchall()
    df = pd.DataFrame(summ, columns=[
        'tanggal', 'tahun', 'provinsi', 'channel', 'total_harga', 
        'kuantitas', 'jumlah_transaksi', 'avg_bill', 'avg_basket'
    ])
    duckdb_conn.register('tb_sum_average_sales_view', df)
    duckdb_conn.execute("""
                    CREATE TABLE IF NOT EXISTS tb_sum_average_sales (
                    tanggal varchar,
                    tahun varchar,
                    provinsi varchar,
                    channel varchar,
                    total_harga int8,
                    kuantitas int4,
                    jumlah_transaksi int8,
                    avg_bill float4,
                    avg_basket float4,
        )
    """)
    duckdb_conn.execute("DELETE FROM tb_sum_average_sales")
    duckdb_conn.execute("INSERT INTO tb_sum_average_sales SELECT * FROM tb_sum_average_sales_view ")
    duckdb_conn.execute(f"COPY tb_sum_average_sales TO '{path}tb_sum_average_sales.parquet' (FORMAT PARQUET)")
    if len(summ) > 0 :
        trunc = """ TRUNCATE TABLE tb_sum_average_sales;"""
        textQuery = text(trunc)
        conn.execute(textQuery)
        conn.commit()
        for s in summ:
            query = SumAverageSales.insert().values(
                tanggal = s[0],
                tahun = s[1],
                provinsi = s[2],
                channel = s[3],
                total_harga = s[4],
                kuantitas = s[5],
                jumlah_transaksi = s[6],    
                avg_bill = s[7],
                avg_basket = s[8],       
                updated_dt = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            )
            conn.execute(query)
            conn.commit()
    duckdb_conn.close()

async def sum_customer():
    path = s3_path(transaction = None)
    tb_path = f"{path}tb_transaksi.parquet"
    duckdb_conn = get_duckdb_connection()
    query = f"""
        SELECT nama AS customer, tahun, provinsi, SUM(harga) total, CASE 
            WHEN SUM(kuantitas) IS NULL THEN
                0
            ELSE
                SUM(kuantitas)
        END AS kuantitas
        FROM read_parquet('{tb_path}')
        WHERE nama IS NOT NULL AND provinsi IS NOT NULL
        GROUP BY 1,2,3
    """
    summ = duckdb_conn.execute(query).fetchall()
    df = pd.DataFrame(summ, columns=[
        'customer', 'tahun', 'provinsi', 'total', 'kuantitas'
    ])
    duckdb_conn.register('tb_sum_customer_view', df)
    duckdb_conn.execute("""
                    CREATE TABLE IF NOT EXISTS tb_sum_customer (
                    customer varchar,
                    tahun varchar,
                    provinsi varchar,
                    total int8,
                    kuantitas int8
        )
    """)
    duckdb_conn.execute("DELETE FROM tb_sum_customer")
    duckdb_conn.execute("INSERT INTO tb_sum_customer SELECT * FROM tb_sum_customer_view ")
    duckdb_conn.execute(f"COPY tb_sum_customer TO '{path}tb_sum_customer.parquet' (FORMAT PARQUET)")
    
    if len(summ) > 0 :
        trunc = """ TRUNCATE TABLE tb_sum_customer;"""
        textQuery = text(trunc)
        conn.execute(textQuery)
        conn.commit()
        for s in summ:
            query = SumCustomer.insert().values(
                customer = s[0],
                tahun = s[1],
                provinsi = s[2],
                total = s[3],
                kuantitas = s[4],
                updated_dt = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            )
            conn.execute(query)
            conn.commit()
    duckdb_conn.close()

async def sum_customer_follower():
    path = s3_path(transaction = None)
    tb_path = f"{path}tb_transaksi.parquet"
    duckdb_conn = get_duckdb_connection()
    query = f"""
        SELECT MIN(REPLACE(nama, E'\r\n', '')) AS nama, LOWER(REPLACE(instagram, E'\r\n', '')) AS instagram, NULL AS follower
        FROM read_parquet('{tb_path}') 
        WHERE instagram IS NOT NULL AND nama IS NOT NULL 
                AND instagram NOT SIMILAR TO '[0-9]+' AND LOWER(instagram) NOT LIKE 'ig:%'
        GROUP BY 2
        ORDER BY 1
    """
    summ = duckdb_conn.execute(query).fetchall()
    
    df = pd.DataFrame(summ, columns=[
        'nama', 'instagram', 'follower'
    ])
    
    duckdb_conn.register('tb_sum_customer_follower_view', df)
    duckdb_conn.execute("""
                    CREATE TABLE IF NOT EXISTS tb_sum_customer_follower (
                    nama varchar,
	                instagram varchar,
                    follower int8
        )
    """)

    duckdb_conn.execute("INSERT INTO tb_sum_customer_follower SELECT * FROM tb_sum_customer_follower_view WHERE nama NOT IN (SELECT nama FROM tb_sum_customer_follower) AND instagram NOT IN (SELECT instagram FROM tb_sum_customer_follower) ")
    duckdb_conn.execute(f"COPY tb_sum_customer_follower TO '{path}tb_sum_customer_follower.parquet' (FORMAT PARQUET)")

    notExist = duckdb_conn.execute("SELECT nama, instagram, follower FROM tb_sum_customer_follower_view WHERE nama NOT IN (SELECT nama FROM tb_sum_customer_follower) AND instagram NOT IN (SELECT instagram FROM tb_sum_customer_follower) ").fetchall()

    if len(notExist) > 0 :
        for s in notExist:
            query = SumCustomerFollower.insert().values(
                nama = s[0],
                instagram = s[1],
                follower = s[2],
                updated_dt = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            )
            conn.execute(query)
            conn.commit()

    duckdb_conn.close()

async def sum_model():
    path = s3_path(transaction = None)
    tb_path = f"{path}tb_transaksi.parquet"
    duckdb_conn = get_duckdb_connection()
    query = f"""
        SELECT model, tahun, provinsi, COUNT(DISTINCT nama) AS total, CASE 
            WHEN SUM(kuantitas) IS NULL THEN
                0
            ELSE
                SUM(kuantitas)
        END AS kuantitas
        FROM read_parquet('{tb_path}')
        WHERE nama IS NOT NULL AND provinsi IS NOT NULL
        GROUP BY 1,2,3
        ORDER BY 4 DESC
    """
    summ = duckdb_conn.execute(query).fetchall()

    df = pd.DataFrame(summ, columns=[
        'model', 'tahun', 'provinsi', 'total', 'kuantitas'
    ])
    duckdb_conn.register('tb_sum_model_view', df)
    duckdb_conn.execute("""
                    CREATE TABLE IF NOT EXISTS tb_sum_model (
                    model varchar,
                    tahun varchar,
                    provinsi varchar,
                    total int8,
                    kuantitas int4
        )
    """)
    duckdb_conn.execute("DELETE FROM tb_sum_model")
    duckdb_conn.execute("INSERT INTO tb_sum_model SELECT * FROM tb_sum_model_view ")
    duckdb_conn.execute(f"COPY tb_sum_model TO '{path}tb_sum_model.parquet' (FORMAT PARQUET)")

    if len(summ) > 0 :
        trunc = """ TRUNCATE TABLE tb_sum_model;"""
        textQuery = text(trunc)
        conn.execute(textQuery)
        conn.commit()
        for s in summ:
            query = SumModel.insert().values(
                model = s[0],
                tahun = s[1],
                provinsi = s[2],
                total = s[3],
                kuantitas = s[4],
                updated_dt = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            )
            conn.execute(query)
            conn.commit()
    duckdb_conn.close()

async def sum_region():
    path = s3_path(transaction = None)
    tb_path = f"{path}tb_transaksi.parquet"
    duckdb_conn = get_duckdb_connection()
    query = f"""
        SELECT provinsi, tahun, COUNT(DISTINCT nama) AS total, CASE 
            WHEN SUM(kuantitas) IS NULL THEN
                0
            ELSE
                SUM(kuantitas)
        END AS kuantitas
        FROM read_parquet('{tb_path}')
        WHERE nama IS NOT NULL AND provinsi IS NOT NULL
        GROUP BY 1,2
        ORDER BY 3 DESC
    """
    summ = duckdb_conn.execute(query).fetchall()
    df = pd.DataFrame(summ, columns=[
        'provinsi', 'tahun', 'total', 'kuantitas'
    ])
    duckdb_conn.register('tb_sum_region_view', df)
    duckdb_conn.execute("""
                    CREATE TABLE IF NOT EXISTS tb_sum_region (
                    provinsi varchar,
                    tahun varchar,
                    total int8,
                    kuantitas int4
        )
    """)
    duckdb_conn.execute("DELETE FROM tb_sum_region")
    duckdb_conn.execute("INSERT INTO tb_sum_region SELECT * FROM tb_sum_region_view ")
    duckdb_conn.execute(f"COPY tb_sum_region TO '{path}tb_sum_region.parquet' (FORMAT PARQUET)")

    if len(summ) > 0 :
        trunc = """ TRUNCATE TABLE tb_sum_region;"""
        textQuery = text(trunc)
        conn.execute(textQuery)
        conn.commit()
        for s in summ:
            query = SumRegion.insert().values(
                provinsi = s[0],
                tahun = s[1],
                total = s[2],
                kuantitas = s[3],
                updated_dt = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            )
            conn.execute(query)
            conn.commit()
    duckdb_conn.close()

async def sum_sales_trend():
    path = s3_path(transaction = None)
    tb_path = f"{path}tb_transaksi.parquet"
    duckdb_conn = get_duckdb_connection()
    query = f"""
        SELECT strftime(tgl_transaksi, '%Y %B') bulan, tahun, 
        provinsi, sum(harga) total, CASE 
            WHEN SUM(kuantitas) IS NULL THEN
                0
            ELSE
                SUM(kuantitas)
        END AS kuantitas
        FROM read_parquet('{tb_path}') 
        WHERE provinsi IS NOT NULL
        GROUP BY 1,2,3
        ORDER BY 1
    """
    summ = duckdb_conn.execute(query).fetchall()
    df = pd.DataFrame(summ, columns=[
        'bulan', 'tahun', 'provinsi', 'total', 'kuantitas'
    ])
    duckdb_conn.register('tb_sum_sales_trend_view', df)
    duckdb_conn.execute("""
                    CREATE TABLE IF NOT EXISTS tb_sum_sales_trend (
                    bulan varchar,
                    tahun varchar,
                    provinsi varchar,
                    total int8,
                    kuantitas int4
        )
    """)
    duckdb_conn.execute("DELETE FROM tb_sum_sales_trend")
    duckdb_conn.execute("INSERT INTO tb_sum_sales_trend SELECT * FROM tb_sum_sales_trend_view ")
    duckdb_conn.execute(f"COPY tb_sum_sales_trend TO '{path}tb_sum_sales_trend.parquet' (FORMAT PARQUET)")

    if len(summ) > 0 :
        trunc = """ TRUNCATE TABLE tb_sum_sales_trend;"""
        textQuery = text(trunc)
        conn.execute(textQuery)
        conn.commit()
        for s in summ:
            query = SumSalesTrend.insert().values(
                bulan = s[0],
                tahun = s[1],
                provinsi = s[2],
                total = s[3],
                kuantitas = s[4],
                updated_dt = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            )
            conn.execute(query)
            conn.commit()
    duckdb_conn.close()

async def sum_sales_trend_pertanggal():
    path = s3_path(transaction = None)
    tb_path = f"{path}tb_transaksi.parquet"
    duckdb_conn = get_duckdb_connection()
    query = f"""
        SELECT tgl_transaksi, model, store, provinsi, sum(harga) total_harga
        FROM read_parquet('{tb_path}') 
        WHERE provinsi IS NOT NULL
        GROUP BY 1,2,3,4
        ORDER BY 1
    """
    summ = duckdb_conn.execute(query).fetchall()
    df = pd.DataFrame(summ, columns=[
        'tgl_transaksi', 'model', 'store', 'provinsi', 'total_harga'
    ])
    duckdb_conn.register('tb_sum_sales_trend_pertanggal_view', df)
    duckdb_conn.execute("""
                    CREATE TABLE IF NOT EXISTS tb_sum_sales_trend_pertanggal (
                    tgl_transaksi date,
                    model varchar,
                    store varchar,
                    provinsi varchar,
                    total_harga int8
        )
    """)
    duckdb_conn.execute("DELETE FROM tb_sum_sales_trend_pertanggal")
    duckdb_conn.execute("INSERT INTO tb_sum_sales_trend_pertanggal SELECT * FROM tb_sum_sales_trend_pertanggal_view ")
    duckdb_conn.execute(f"COPY tb_sum_sales_trend_pertanggal TO '{path}tb_sum_sales_trend_pertanggal.parquet' (FORMAT PARQUET)")

    if len(summ) > 0 :
        trunc = """ TRUNCATE TABLE tb_sum_sales_trend_pertanggal;"""
        textQuery = text(trunc)
        conn.execute(textQuery)
        conn.commit()
        for s in summ:
            query = SumSalesTrendPertanggal.insert().values(
                tgl_transaksi = s[0],
                model = s[1],
                store = s[2],
                provinsi = s[3],
                total_harga = s[4],
                updated_dt = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            )
            conn.execute(query)
            conn.commit()
    duckdb_conn.close()

async def sum_store():
    path = s3_path(transaction = None)
    tb_path = f"{path}tb_transaksi.parquet"
    duckdb_conn = get_duckdb_connection()
    query = f"""
        SELECT tgl_transaksi, store AS channel, tahun, provinsi, SUM(harga) total, COUNT(*) jumlah, CASE 
            WHEN SUM(kuantitas) IS NULL THEN
                0
            ELSE
                SUM(kuantitas)
        END AS kuantitas
        FROM read_parquet('{tb_path}')
        WHERE provinsi IS NOT NULL
        GROUP BY 1,2,3,4
        ORDER BY 2
    """
    summ = duckdb_conn.execute(query).fetchall()
    df = pd.DataFrame(summ, columns=[
        'tgl_transaksi', 'channel', 'tahun', 'provinsi', 'total', 'jumlah', 'kuantitas'
    ])
    duckdb_conn.register('tb_sum_store_view', df)
    duckdb_conn.execute("""
                    CREATE TABLE IF NOT EXISTS tb_sum_store (
                    tgl_transaksi date,
                    channel varchar,
                    tahun varchar,
                    provinsi varchar,
                    total int8,
                    jumlah int8,
                    kuantitas int4
        )
    """)
    duckdb_conn.execute("DELETE FROM tb_sum_store")
    duckdb_conn.execute("INSERT INTO tb_sum_store SELECT * FROM tb_sum_store_view ")
    duckdb_conn.execute(f"COPY tb_sum_store TO '{path}tb_sum_store.parquet' (FORMAT PARQUET)")
    
    if len(summ) > 0 :
        trunc = """ TRUNCATE TABLE tb_sum_store;"""
        textQuery = text(trunc)
        conn.execute(textQuery)
        conn.commit()
        for s in summ:
            query = SumStore.insert().values(
                tgl_transaksi = s[0],
                channel = s[1],
                tahun = s[2],
                provinsi = s[3],
                total = s[4],
                jumlah = s[5],
                kuantitas = s[6],
                updated_dt = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            )
            conn.execute(query)
            conn.commit()

    duckdb_conn.close()

async def sum_top_produk():
    path = s3_path(transaction = None)
    tb_path = f"{path}tb_transaksi.parquet"
    duckdb_conn = get_duckdb_connection()
    query = f"""
        SELECT model AS produk, provinsi, store, tahun, SUM(harga) total_harga, COUNT(*) jumlah, CASE 
            WHEN SUM(kuantitas) IS NULL THEN
                0
            ELSE
                SUM(kuantitas)
        END AS kuantitas
        FROM read_parquet('{tb_path}') 
        WHERE model IS NOT NULL AND provinsi IS NOT NULL
        GROUP BY 1,2,3,4
    """
    summ = duckdb_conn.execute(query).fetchall()
    df = pd.DataFrame(summ, columns=[
        'produk', 'provinsi', 'store', 'tahun', 'total_harga', 'jumlah', 'kuantitas'
    ])
    duckdb_conn.register('tb_sum_top_produk_view', df)
    duckdb_conn.execute("""
                    CREATE TABLE IF NOT EXISTS tb_sum_top_produk (
                    produk varchar,
                    provinsi varchar,
                    store varchar,
                    tahun varchar,
                    total_harga int8,
                    jumlah int8,
                    kuantitas int4
        )
    """)
    duckdb_conn.execute("DELETE FROM tb_sum_top_produk")
    duckdb_conn.execute("INSERT INTO tb_sum_top_produk SELECT * FROM tb_sum_top_produk_view ")
    duckdb_conn.execute(f"COPY tb_sum_top_produk TO '{path}tb_sum_top_produk.parquet' (FORMAT PARQUET)")

    if len(summ) > 0 :
        trunc = """ TRUNCATE TABLE tb_sum_top_produk;"""
        textQuery = text(trunc)
        conn.execute(textQuery)
        conn.commit()
        for s in summ:
            query = SumTopProduk.insert().values(
                produk = s[0],
                provinsi = s[1],
                store = s[2],
                tahun = s[3],
                total_harga = s[4],
                jumlah = s[5],
                kuantitas = s[6],
                updated_dt = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            )
            conn.execute(query)
            conn.commit()
    duckdb_conn.close()