from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from auth import (
    authenticate_user, 
    create_access_token, 
    Token,
    get_password_hash 
)
from datetime import timedelta
from database import get_db
from sqlalchemy.orm import Session
from sqlalchemy import text  
router = APIRouter()

class LoginRequest(BaseModel):
    username: str
    password: str

class Token(BaseModel):
    access_token: str
    token_type: str
    group: str
    
@router.post("/login", response_model=Token)
async def login_for_access_token(login: LoginRequest, db: Session = Depends(get_db)):
    # Authenticate the user by checking their credentials against the database
    user = authenticate_user(db, login.username, login.password)
    
    if not user:
        # Raise an exception if authentication fails
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    # Set token expiration time
    access_token_expires = timedelta(minutes=60)
    
    # Create the access token (assuming you have a function `create_access_token`)
    access_token = create_access_token(
        data={"sub": user.ms_user_username}, expires_delta=access_token_expires
    )
    
    
    sql_query = text("""
        SELECT mg.ms_group_name
        FROM ms_group mg
        LEFT JOIN ms_user_group mug ON mug.ms_group_id = mg.ms_group_id
        LEFT JOIN ms_user mu ON mu.ms_user_id = mug.ms_user_id
        WHERE mu.ms_user_id = :user_id
        LIMIT 1
    """)

    # Execute the query and fetch the first result
    result = db.execute(sql_query, {"user_id": user.ms_user_id}).fetchone()
    group_name = ""
    if result is not None:
        group_name = result[0]
    # Return the access token, token type, and user group
    return {"access_token": access_token, "token_type": "bearer", "group": group_name}

# @router.post("/register", response_model=LoginRequest)
# async def register_user(user: LoginRequest):
#     if user.username in fake_users_db:
#         raise HTTPException(
#             status_code=status.HTTP_400_BAD_REQUEST,
#             detail="Username already registered"
#         )
#     hashed_password = get_password_hash(user.password)
#     user_dict = {"username": user.username, "hashed_password": hashed_password}
#     fake_users_db[user.username] = user_dict
#     return user

