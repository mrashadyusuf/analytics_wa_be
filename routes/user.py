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
from models.group import Group
from models.user_group import UserGroup

router = APIRouter()


# Create User
@router.post("/", response_model=UserResponse, status_code=status.HTTP_200_OK)
def create_user(
    user: UserCreateUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)  # JWT authentication
):
    try:
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
            role = user.role,
            phone_number = user.phone_number,
            created_by=current_user.username,
            updated_by=current_user.username,
            created_dt=datetime.utcnow() + timedelta(hours=7),
            updated_dt=datetime.utcnow() + timedelta(hours=7),
        )

        if user.ms_group_id:
            new_user_group = UserGroup(
                ms_user_group_id =  str(uuid.uuid4()),
                ms_user_id=new_user_id,
                ms_group_id=user.ms_group_id,
                isactive="Y",
                isdefault="Y",
                createdby=current_user.username,
                created=datetime.utcnow() + timedelta(hours=7),
                updatedby=current_user.username,
                updated=datetime.utcnow() + timedelta(hours=7),
            )
            db.add(new_user_group)


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

        # Base SQL query for selecting all fields from ms_user and ms_group_name from ms_group
        sql_query = """
            SELECT ms_user.*, 
                   ms_group.ms_group_name
            FROM ms_user
            LEFT JOIN ms_user_group ON ms_user.ms_user_id = ms_user_group.ms_user_id
            LEFT JOIN ms_group ON ms_user_group.ms_group_id = ms_group.ms_group_id
        """

        # Search filter (if search keyword is provided)
        if search:
            sql_query += f"""
                WHERE ms_user.ms_user_username ILIKE '%{search}%' 
                   OR ms_user.ms_user_email ILIKE '%{search}%'
                   OR ms_user.ms_user_name ILIKE '%{search}%'
            """

        # Add ORDER BY, LIMIT, and OFFSET for pagination
        sql_query += f" ORDER BY ms_user.created_dt DESC LIMIT {limit} OFFSET {offset}"

        # Execute the query
        result = db.execute(text(sql_query))
        users = result.fetchall()  # Fetch all the results

        # Format users into a list of dictionaries
        formatted_users = [
            {
                "ms_user_id": user.ms_user_id,
                "isactive": user.isactive,
                "ms_user_name": user.ms_user_name,
                "ms_user_username": user.ms_user_username,
                "ms_user_email": user.ms_user_email,
                "role": user.role,
                "phone_number": user.phone_number,
                "created_by": user.created_by,
                "created_dt": user.created_dt,
                "updated_by": user.updated_by,
                "updated_dt": user.updated_dt,
                "ms_group_name": user.ms_group_name,  # Group name from the ms_group table
            }
            for user in users
        ]
        # Count the total number of users matching the search (for pagination metadata)
        count_query = """
            SELECT COUNT(*)
            FROM ms_user
            LEFT JOIN ms_user_group ON ms_user.ms_user_id = ms_user_group.ms_user_id
            LEFT JOIN ms_group ON ms_user_group.ms_group_id = ms_group.ms_group_id
        """

        # Apply the same search condition for counting total results
        if search:
            count_query += f"""
                WHERE ms_user.ms_user_username ILIKE '%{search}%'
                   OR ms_user.ms_user_email ILIKE '%{search}%'
                   OR ms_user.ms_user_name ILIKE '%{search}%'
            """

        total_all_data = db.execute(text(count_query)).scalar()  # Get the total count

        # Format the response
        response = {
            "total_data": len(formatted_users),  # Number of users in the current response
            "total_all_data": total_all_data,  # Total number of users without pagination
            "offset": offset,  # Current offset
            "users": formatted_users  # List of users
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


@router.get("/by-username", status_code=status.HTTP_200_OK)
def get_user_group_by_username(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)  # JWT authentication
):
    try:
        # Fetch the user based on the current user's username
        user = db.query(UserModel).filter(UserModel.ms_user_username == current_user.username).first()

        if not user:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")

        # Fetch the user group based on the user ID
        user_group = db.query(UserGroup).filter(UserGroup.ms_user_id == user.ms_user_id).first()

        if not user_group:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User group not found")

        # Return the user group without pagination, as we're only expecting one result
        return {
                "ms_user_id": user.ms_user_id,
                "isactive": user.isactive,
                "ms_user_name": user.ms_user_name,
                "ms_user_username": user.ms_user_username,
                "ms_user_password": user.ms_user_password,  # Ensure this is securely handled
                "ms_user_email": user.ms_user_email,
                "ms_user_token": user.ms_user_token,
                "created_by": user.created_by,
                "created_dt": user.created_dt,
                "updated_by": user.updated_by,
                "updated_dt": user.updated_dt,
                "role": user.role,
                "phone_number": user.phone_number
            }

    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        print(f"Internal Server Error: {str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal Server Error")


@router.get("/{user_id}", status_code=status.HTTP_200_OK)
def get_user_by_id(
    user_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)  # JWT authentication
):
    try:
        print(f"Fetching user with ID: {user_id}...")

        # Fetch the user by ID with LEFT JOIN on Group to include ms_group_name
        user_data = (
            db.query(UserModel, Group.ms_group_name, Group.ms_group_id)
            .outerjoin(UserGroup, UserModel.ms_user_id == UserGroup.ms_user_id)
            .outerjoin(Group, UserGroup.ms_group_id == Group.ms_group_id)
            .filter(UserModel.ms_user_id == user_id)
            .first()
        )

        if not user_data:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")

        # Unpack the user and group_name from the query result
        user, group_name, group_id = user_data

        # Construct a response dictionary with user data and group name
        user_response = {
            "ms_user_id": user.ms_user_id,
            "isactive": user.isactive,
            "ms_user_name": user.ms_user_name,
            "ms_user_username": user.ms_user_username,
            "ms_user_email": user.ms_user_email,
            "role": user.role,
            "phone_number": user.phone_number,
            "created_by": user.created_by,
            "created_dt": user.created_dt,
            "updated_by": user.updated_by,
            "updated_dt": user.updated_dt,
            "ms_group_name": group_name,
            "ms_group_id": group_id
        }
        print("user_response",user_response)
        return user_response

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

        print("userpass", user_update.ms_user_password)
        if user_update.ms_user_password is not None:
            hashed_password = bcrypt.hashpw(user_update.ms_user_password.encode('utf-8'), bcrypt.gensalt())
            user.ms_user_password = hashed_password.decode('utf-8')

        # Update user details
        user.ms_user_name = user_update.ms_user_name
        user.ms_user_username = user_update.ms_user_username
        user.ms_user_email = user_update.ms_user_email
        user.isactive = user_update.isactive
        user.ms_user_token = user_update.ms_user_token
        user.updated_by = current_user.username
        user.updated_dt = datetime.utcnow() + timedelta(hours=7)
        user.role = user_update.role
        user.phone_number = user_update.phone_number

        # If ms_group_id is provided, update or insert into the user_group table
        if user_update.ms_group_id:
            # Check if user is already in the user_group table
            user_group = db.query(UserGroup).filter(UserGroup.ms_user_id == user_id).first()

            if user_group:
                # If the user exists in the user_group table, update the group ID
                user_group.ms_group_id = user_update.ms_group_id
                user_group.updated_by = current_user.username
                user_group.updated_dt = datetime.utcnow() + timedelta(hours=7)
                print(f"Updated group for user ID: {user_id} with group ID: {user_update.ms_group_id}")
            else:
                # If the user is not in the user_group table, create a new entry
                new_user_group = UserGroup(
                    ms_user_group_id =  str(uuid.uuid4()),
                    ms_user_id=user_id,
                    ms_group_id=user_update.ms_group_id,
                    isactive="Y",
                    isdefault="Y",
                    createdby=current_user.username,
                    created=datetime.utcnow() + timedelta(hours=7),
                    updatedby=current_user.username,
                    updated=datetime.utcnow() + timedelta(hours=7),
                )
                db.add(new_user_group)
                print(f"Added new group for user ID: {user_id} with group ID: {user_update.ms_group_id}")

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

        # Check if the user is in the user_group table
        user_group = db.query(UserGroup).filter(UserGroup.ms_user_id == user_id).first()

        if user_group:
            # Delete the user_group entry if it exists
            db.delete(user_group)
            print(f"Deleted user group entry for user ID: {user_id}")

        # Delete the user
        db.delete(user)
        db.commit()

        print(f"User deleted with ID: {user_id}")

        return {"message": "User and associated group (if any) deleted successfully"}

    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        print(f"Internal Server Error: {str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal Server Error")
