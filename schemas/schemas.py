from pydantic import BaseModel
from typing import Optional
from datetime import date

# Shared properties
class TransactionBase(BaseModel):
    transaction_id: str
    transaction_channel: str  # Required field
    model_product: str  # Required field
    price_product: str  # Required field
    no_hp_cust: str  # Required field
    name_cust: str  # Required field
    city_cust: str  # Required field
    prov_cust: str  # Required field
    address_cust: str  # Required field
    instagram_cust: Optional[str] = None  # Optional field
    created_by: str  # Required field
    created_dt: date
    updated_by: str  # Required field
    updated_dt: date
    transaction_dt: date

# Properties to receive on item creation
class TransactionCreate(TransactionBase):
    pass

# Properties to receive on item update
class TransactionUpdate(TransactionBase):
    pass

# Properties stored in the database
class TransactionInDB(TransactionBase):
    class Config:
        orm_mode = True
