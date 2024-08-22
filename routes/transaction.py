from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from sqlalchemy import desc  
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
    # 1. Retrieve the last created transaction
    last_transaction = db.query(Transaction).order_by(desc(Transaction.transaction_id)).first()
    
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
    
    # 5. Check if the generated transaction_id already exists (highly unlikely)
    existing_transaction = db.query(Transaction).filter(Transaction.transaction_id == transaction_id).first()
    if existing_transaction:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Transaction ID already exists")
    
    # 6. Prepare the data for insertion without 'transaction_id'
    transaction_data = transaction.dict(exclude={'transaction_id'})
    
    # 7. Proceed with the insertion
    db_transaction = Transaction(
        transaction_id=transaction_id,
        **transaction_data
    )
    db.add(db_transaction)
    db.commit()
    db.refresh(db_transaction)
    
    # 8. Return the newly created transaction
    return db_transaction

@router.get("/", status_code=status.HTTP_200_OK)
def read_transactions(
    offset: int = 0,  # Changed parameter name from skip to offset
    limit: int = 10, 
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)  # JWT authentication
):
    # Query for the total count of all transactions
    total_all_data = db.query(Transaction).count()
        
    # Query for the paginated transactions
    transactions = db.query(Transaction).order_by(desc(Transaction.created_dt)).offset(offset).limit(limit).all()
    
    total_data =  db.query(Transaction).order_by(desc(Transaction.created_dt)).offset(offset).limit(limit).count()

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

@router.put("/{transaction_id}", response_model=TransactionInDB,  status_code=status.HTTP_200_OK)
@router.put("/{transaction_id}", response_model=TransactionInDB, status_code=status.HTTP_200_OK)
def update_transaction(
    transaction_id: str,  # Existing transaction ID to be updated
    transaction: TransactionUpdate, 
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)  # JWT authentication
):
    # 1. Fetch the existing transaction record
    db_transaction = db.query(Transaction).filter(Transaction.transaction_id == transaction_id).first()
    if not db_transaction:
        raise HTTPException(status_code=404, detail="Transaction not found")

    # 2. Extract the original num_id from the existing transaction_id
    original_num_id = transaction_id[2:6]  # Extract characters 3 to 6

    # 3. Format the transaction date to 'ddmmyy'
    transaction_date_str = transaction.transaction_dt.strftime('%d%m%y')  # e.g., '210824'
    
    # 4. Extract the first two characters from the transaction channel
    channel_code = transaction.transaction_channel[:2].upper()  # e.g., 'ON'
    
    # 5. Construct the new transaction_id using the original num_id
    new_transaction_id = f'TT{original_num_id}{transaction_date_str}{channel_code}'
    
    # 6. Update the transaction fields
    for key, value in transaction.dict(exclude_unset=True).items():
        setattr(db_transaction, key, value)
    
    # 7. Update the transaction_id
    db_transaction.transaction_id = new_transaction_id

    # 8. Commit the transaction and refresh the record
    db.commit()
    db.refresh(db_transaction)

    # 9. Return the updated transaction
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
