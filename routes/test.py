from fastapi import APIRouter, Depends
from auth import get_current_user, User  # Adjust the import path based on your project structure

router = APIRouter()

@router.get("/")
def get_transaction(current_user: User = Depends(get_current_user)):  # JWT protected route
    return {"message": "balikan", "user": current_user.username}
