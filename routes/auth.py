from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from auth import (
    authenticate_user, 
    create_access_token, 
    fake_users_db, 
    Token,
    get_password_hash 
)
from datetime import timedelta

router = APIRouter()

class LoginRequest(BaseModel):
    username: str
    password: str

@router.post("/login", response_model=Token)
async def login_for_access_token(login: LoginRequest):
    print("fake_users_db",fake_users_db)
    user = authenticate_user(fake_users_db, login.username, login.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token_expires = timedelta(minutes=30)
    access_token = create_access_token(
        data={"sub": user.username}, expires_delta=access_token_expires
    )
    return {"access_token": access_token, "token_type": "bearer"}

@router.post("/register", response_model=LoginRequest)
async def register_user(user: LoginRequest):
    if user.username in fake_users_db:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Username already registered"
        )
    hashed_password = get_password_hash(user.password)
    user_dict = {"username": user.username, "hashed_password": hashed_password}
    fake_users_db[user.username] = user_dict
    return user

