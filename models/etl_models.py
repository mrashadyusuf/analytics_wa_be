from sqlalchemy import (
    Column,
    Integer,
    MetaData,
    String,
    Table,
    Boolean,
    Float
)
from sqlalchemy.sql.sqltypes import Date

metadata = MetaData()

# table 
ChatWa = Table(
    "tb_chat_wa", metadata,
    Column("id", String(50), primary_key=True),
    Column("nama", String(100)),
    Column("no_hp", String(20)),
    Column("tanggal", Date()),
    Column("is_end_chat", Boolean),
    Column("status", String(50)),
)

SumAverageSales = Table(
    "tb_sum_average_sales", metadata,
    Column("tanggal", String(100)),
    Column("tahun", String(20)),
    Column("provinsi", String(100)),
    Column("channel", String(100)),
    Column("total_harga", Integer),
    Column("kuantitas", Integer),
    Column("jumlah_transaksi", Integer),
    Column("avg_bill", Float),
    Column("avg_basket", Float),
    Column("updated_dt", Date()),
)

SumCustomer = Table(
    "tb_sum_customer", metadata,
    Column("customer", String(50)),
    Column("tahun", String(20)),
    Column("provinsi", String(100)),
    Column("total", Integer),
    Column("kuantitas", Integer),
    Column("updated_dt", Date()),
)

SumCustomerFollower = Table(
    "tb_sum_customer_follower", metadata,
    Column("nama", String(100)),
    Column("instagram", String(100)),
    Column("follower", Integer),
    Column("updated_dt", Date()),
)

SumModel = Table(
    "tb_sum_model", metadata,
    Column("model", String(100)),
    Column("tahun", String(20)),
    Column("provinsi", String(100)),
    Column("total", Integer),
    Column("kuantitas", Integer),
    Column("updated_dt", Date()),
)

SumRegion = Table(
    "tb_sum_region", metadata,
    Column("provinsi", String(100)),
    Column("tahun", String(20)),
    Column("total", Integer),
    Column("kuantitas", Integer),
    Column("updated_dt", Date()),
)

SumSalesTrend = Table(
    "tb_sum_sales_trend", metadata,
    Column("bulan", String(50)),
    Column("tahun", String(20)),
    Column("provinsi", String(100)),
    Column("total", Integer),
    Column("kuantitas", Integer),
    Column("updated_dt", Date()),
)

SumSalesTrendPertanggal = Table(
    "tb_sum_sales_trend_pertanggal", metadata,
    Column("tgl_transaksi", Date()),
    Column("model", String(100)),
    Column("store", String(100)),
    Column("provinsi", String(100)),
    Column("total_harga", Integer),
    Column("kuantitas", Integer),
    Column("updated_dt", Date()),
)

SumStore = Table(
    "tb_sum_store", metadata,
    Column("tgl_transaksi", Date()),
    Column("channel", String(20)),
    Column("tahun", String(10)),
    Column("provinsi", String(100)),
    Column("total", Integer),
    Column("jumlah", Integer),
    Column("kuantitas", Integer),
    Column("updated_dt", Date()),
)

SumTopProduk = Table(
    "tb_sum_top_produk", metadata,
    Column("produk", String(50)),
    Column("provinsi", String(100)),
    Column("store", String(100)),
    Column("tahun", String(20)),
    Column("total_harga", Integer),
    Column("jumlah", Integer),
    Column("kuantitas", Integer),
    Column("updated_dt", Date()),
)

SumTransaksi = Table(
    "tb_sum_transaksi", metadata,
    Column("tgl", String(50)),
    Column("tahun", String(20)),
    Column("provinsi", String(100)),
    Column("model", String(100)),
    Column("store", String(50)),
    Column("total_harga", Integer),
    Column("jumlah", Integer),
    Column("kuantitas", Integer),
    Column("updated_dt", Date()),
)

SumWa = Table(
    "tb_sum_wa", metadata,
    Column("no_hp", String(50)),
    Column("nama", String(20)),
    Column("tanggal", Date()),
    Column("terakhir_dihubungi", String(100)),
    Column("updated_dt", Date()),
)

Transaksi = Table(
    "tb_transaksi", metadata,
    Column("id", String(50), primary_key=True),
    Column("tgl_transaksi", Date()),
    Column("tanggal", String(20)),
    Column("bulan", String(20)),
    Column("tahun", String(20)),
    Column("nama", String(50)),
    Column("model", String(100)),
    Column("alamat", String(255)),
    Column("no_telp", String(20)),
    Column("provinsi", String(100)),
    Column("kota_kab", String(100)),
    Column("instagram", String(100)),
    Column("store", String(50)),
    Column("harga", Integer),
    Column("kuantitas", Integer),
)
