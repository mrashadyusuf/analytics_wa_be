from datetime import datetime, timedelta
from jose import JWTError, jwt
from passlib.context import CryptContext
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from pydantic import BaseModel
from sqlalchemy.orm import Session
from models.user import User as UserModel  # Adjusted to use UserModel for ORM
import bcrypt
from database import get_db
from sqlalchemy import text

# JWT configuration
SECRET_KEY = "your-secret-key"  # Use a strong secret key in production
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30

# Initialize CryptContext with bcrypt for password hashing
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# OAuth2 scheme for FastAPI (Bearer token)
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/login")

# Token and User schemas for the API
class Token(BaseModel):
    access_token: str
    token_type: str

class TokenData(BaseModel):
    username: str | None = None

class User(BaseModel):
    username: str
    password: str | None = None

class UserInDB(User):
    hashed_password: str
    group: str

class UserData(BaseModel):
    ms_user_id: str
    ms_user_username: str
    ms_user_email: str
    ms_user_name: str
    isactive: str
    username: str
    group: str

# Password utility functions using Passlib's CryptContext
def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)

def get_password_hash(password: str) -> str:
    return pwd_context.hash(password)

# JWT creation function
def create_access_token(data: dict, expires_delta: timedelta | None = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

# Authentication function to check user credentials from the DB
def authenticate_user(db: Session, username: str, password: str):
    # 1. Query the user from the database using the provided username
    user = db.query(UserModel).filter(UserModel.ms_user_username == username).first()
    if not user:
        return False
    
    # 2. Verify the provided password with the hashed password in the DB
    if not verify_password(password, user.ms_user_password):
        return False

    return user  # Return the user object if authentication succeeds

# Function to get the current user based on the JWT token
async def get_current_user(token: str = Depends(oauth2_scheme), db: Session = Depends(get_db)):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Could not validate credentials",
                headers={"WWW-Authenticate": "Bearer"},
            )
        token_data = TokenData(username=username)
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    stmt = text("""SELECT u.*, g.ms_group_name
    FROM ms_user u
    LEFT JOIN ms_user_group ug ON u.ms_user_id = ug.ms_user_id
    LEFT JOIN ms_group g ON ug.ms_group_id = g.ms_group_id
    WHERE u.ms_user_username = :username
    LIMIT 1
    """)
    user = db.execute(stmt, {"username": token_data.username}).fetchone()
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )
    user_data = UserData(
        ms_user_id=user.ms_user_id,
        ms_user_username=user.ms_user_username,
        ms_user_email=user.ms_user_email,
        ms_user_name=user.ms_user_name,
        isactive=user.isactive,
        username=user.ms_user_username ,
        group=user.ms_group_name
    )

    return user_data
