from sqlalchemy import Column, String, Date
from database import Base

class Transaction(Base):
    __tablename__ = "tb_transaction"

    # Define transaction_id as the primary key
    transaction_id = Column(String(20), primary_key=True, index=True)
    transaction_channel = Column(String(255), nullable=True)
    model_product = Column(String(255), nullable=True)
    price_product = Column(String(255), nullable=True)
    no_hp_cust = Column(String(255), nullable=True)
    name_cust = Column(String(255), nullable=True)
    city_cust = Column(String(255), nullable=True)
    prov_cust = Column(String(255), nullable=True)
    address_cust = Column(String(255), nullable=True)
    instagram_cust = Column(String(255), nullable=True)
    created_by = Column(String(255), nullable=True)
    created_dt = Column(Date, nullable=True)
    updated_by = Column(String(255), nullable=True)
    updated_dt = Column(Date, nullable=True)
    transaction_dt = Column(Date, nullable=True)
