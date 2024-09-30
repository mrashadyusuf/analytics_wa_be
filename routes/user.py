from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from sqlalchemy import desc, or_, text
from typing import List
from database import get_db
from models.user import User as UserModel  # Adjusted to use UserModel for ORM
from schemas.userSchemas import UserCreateUpdate, UserResponse
from auth import get_current_user, User  # Using `User` for authentication
import uuid
from datetime import datetime, timedelta
import bcrypt

router = APIRouter()


# Create User
@router.post("/", response_model=UserResponse, status_code=status.HTTP_200_OK)
def create_user(
    user: UserCreateUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)  # JWT authentication
):
    try:
        print("current_use2r",current_user)
        print("Starting user creation process...")

        # 1. Check if username or email already exists
        existing_user = db.query(UserModel).filter(
            (UserModel.ms_user_username == user.ms_user_username) | (UserModel.ms_user_email == user.ms_user_email)
        ).first()

        if existing_user:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Username or email already exists")

        # 2. Generate a unique ms_user_id
        new_user_id = str(uuid.uuid4())  # You can adjust this logic as needed

        # 3. Hash the user's password
        hashed_password = bcrypt.hashpw(user.ms_user_password.encode('utf-8'), bcrypt.gensalt())

        # 4. Create the new user record for PostgreSQL
        new_user = UserModel(
            ms_user_id=new_user_id,
            ms_user_name=user.ms_user_name,
            ms_user_username=user.ms_user_username,
            ms_user_password=hashed_password.decode('utf-8'),  # Store as a string
            ms_user_email=user.ms_user_email,
            isactive=user.isactive,
            ms_user_token=user.ms_user_token,
            created_by=current_user.username,
            updated_by=current_user.username,
            created_dt=datetime.utcnow() + timedelta(hours=7),
            updated_dt=datetime.utcnow() + timedelta(hours=7),
        )

        # 5. Insert the new user into PostgreSQL
        db.add(new_user)
        db.commit()
        db.refresh(new_user)

        print(f"User inserted into PostgreSQL with ID: {new_user_id}")

        # 6. Return the newly created user
        return new_user

    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        print(f"Internal Server Error: {str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal Server Error")


# Read all users with pagination and search
@router.get("/", status_code=status.HTTP_200_OK)
def get_users(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),  # JWT authentication
    limit: int = 10,  # Number of users per page
    offset: int = 0,  # Offset for pagination
    search: str = "",  # Search keyword
):
    try:
        print("Fetching users with pagination and search...")

        # Base query
        query = db.query(UserModel)

        # Apply search filter if a keyword is provided
        if search:
            search_filter = or_(
                UserModel.ms_user_username.ilike(f"%{search}%"),
                UserModel.ms_user_email.ilike(f"%{search}%"),
                UserModel.ms_user_name.ilike(f"%{search}%"),
            )
            query = query.filter(search_filter)

        # Get total number of users (for pagination metadata)
        total_all_data = query.count()

        # Apply pagination (limit and offset)
        users = query.order_by(desc(UserModel.created_dt)).offset(offset).limit(limit).all()

        # Count the number of users in the current response (for `total_data`)
        total_data = len(users)

        # Format the response as required
        response = {
            "total_data": total_data,  # Number of users in this response
            "total_all_data": total_all_data,  # Total number of users without pagination
            "offset": offset,  # Current offset
            "users": users  # List of users
        }

        return response

    except Exception as e:
        print(f"Internal Server Error: {str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal Server Error")

@router.get("/users-without-group", response_model=List[UserResponse])
def get_users_without_group(db: Session = Depends(get_db)):
    query = text("""
        SELECT * FROM ms_user WHERE ms_user_id NOT IN (
            SELECT ms_user_id FROM ms_user_group
        )
    """)
    
    users_without_group = db.execute(query).fetchall()

    if not users_without_group:
        raise HTTPException(status_code=404, detail="Users not found")

    return users_without_group

# Read a single user by ID
@router.get("/{user_id}", response_model=UserResponse, status_code=status.HTTP_200_OK)
def get_user_by_id(
    user_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)  # JWT authentication
):
    try:
        print(f"Fetching user with ID: {user_id}...")

        # Fetch the user by ID
        user = db.query(UserModel).filter(UserModel.ms_user_id == user_id).first()

        if not user:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")

        return user

    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        print(f"Internal Server Error: {str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal Server Error")



# Update a user
@router.put("/{user_id}", response_model=UserResponse, status_code=status.HTTP_200_OK)
def update_user(
    user_id: str,
    user_update: UserCreateUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)  # JWT authentication
):
    try:
        print(f"Updating user with ID: {user_id}...")

        # Fetch the user
        user = db.query(UserModel).filter(UserModel.ms_user_id == user_id).first()

        if not user:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")

        hashed_password = bcrypt.hashpw(user_update.ms_user_password.encode('utf-8'), bcrypt.gensalt())
        print("hashed_password",hashed_password)
        # Update user details
        user.ms_user_name = user_update.ms_user_name
        user.ms_user_username = user_update.ms_user_username
        user.ms_user_password = hashed_password.decode('utf-8')
        user.ms_user_email = user_update.ms_user_email
        user.isactive = user_update.isactive
        user.ms_user_token = user_update.ms_user_token
        user.updated_by = current_user.username
        user.updated_dt = datetime.utcnow() + timedelta(hours=7)

        # Commit the update
        db.commit()
        db.refresh(user)

        print(f"User updated with ID: {user_id}")

        return user

    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        print(f"Internal Server Error: {str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal Server Error")


# Delete a user
@router.delete("/{user_id}", status_code=status.HTTP_200_OK)
def delete_user(
    user_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)  # JWT authentication
):
    try:
        print(f"Deleting user with ID: {user_id}...")

        # Fetch the user
        user = db.query(UserModel).filter(UserModel.ms_user_id == user_id).first()

        if not user:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")

        # Delete the user
        db.delete(user)
        db.commit()

        print(f"User deleted with ID: {user_id}")

        return {"message": "User deleted successfully"}

    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        print(f"Internal Server Error: {str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal Server Error")
