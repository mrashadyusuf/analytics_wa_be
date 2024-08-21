from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List

from database import get_db
from models.models import Transaction
from schemas.schemas import TransactionCreate, TransactionUpdate, TransactionInDB
from auth import get_current_user, User

router = APIRouter()

@router.post("/", response_model=TransactionInDB, status_code=status.HTTP_200_OK)
def create_transaction(
    transaction: TransactionCreate, 
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)  # JWT authentication
):
    # Check if the transaction_id already exists
    existing_transaction = db.query(Transaction).filter(Transaction.transaction_id == transaction.transaction_id).first()
    if existing_transaction:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Transaction ID already exists")
    
    # If not, proceed with the insertion
    db_transaction = Transaction(**transaction.dict())
    db.add(db_transaction)
    db.commit()
    db.refresh(db_transaction)
    return db_transaction

@router.get("/", response_model=List[TransactionInDB], status_code=status.HTTP_200_OK)
def read_transactions(
    offset: int = 0,  # Changed parameter name from skip to offset
    limit: int = 10, 
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)  # JWT authentication
):
    transactions = db.query(Transaction).offset(offset).limit(limit).all()
    return transactions

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

@router.put("/{transaction_id}", response_model=TransactionInDB,  status_code=status.HTTP_200_OK)
def update_transaction(
    transaction_id: str,  # Change type to str since transaction_id is not an integer
    transaction: TransactionUpdate, 
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)  # JWT authentication
):
    db_transaction = db.query(Transaction).filter(Transaction.transaction_id == transaction_id).first()
    if not db_transaction:
        raise HTTPException(status_code=404, detail="Transaction not found")
    for key, value in transaction.dict(exclude_unset=True).items():
        setattr(db_transaction, key, value)
    db.commit()
    db.refresh(db_transaction)
    return db_transaction

@router.delete("/{transaction_id}",   status_code=status.HTTP_200_OK)
def delete_transaction(
    transaction_id: str,  # Change type to str since transaction_id is not an integer
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)  # JWT authentication
):
    db_transaction = db.query(Transaction).filter(Transaction.transaction_id == transaction_id).first()
    if not db_transaction:
        raise HTTPException(status_code=404, detail="Transaction not found")
    db.delete(db_transaction)
    db.commit()
    return "Delete Transaction Success"
