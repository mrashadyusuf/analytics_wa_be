from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from sqlalchemy import desc, or_, text
from database import get_db
from models.user_group import UserGroup  # Assuming you have a UserGroup model defined
from schemas.user_groupSchemas import UserGroupResponse,UserGroupCreate  # Assuming you have relevant Pydantic schemas
from typing import List
import uuid
from datetime import datetime, timedelta
from auth import get_current_user, User

router = APIRouter()


@router.post("/", response_model=UserGroupResponse, status_code=status.HTTP_200_OK)
def create_user_group(
    user_group: UserGroupCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user) 
):
    try:
        # 1. Generate a unique ID for the new user group
        new_user_group_id = str(uuid.uuid4())

        # 2. Create the new user group record for the database
        new_user_group = UserGroup(
            ms_user_group_id=new_user_group_id,
            ms_user_id=user_group.ms_user_id,
            ms_group_id=user_group.ms_group_id,
            isactive=user_group.isactive,
            isdefault=user_group.isdefault,
            createdby=user_group.createdby,
            created=datetime.utcnow() + timedelta(hours=7),
            updatedby=user_group.updatedby,
            updated=datetime.utcnow() + timedelta(hours=7)
        )

        # 3. Insert the new user group into the database
        db.add(new_user_group)
        db.commit()
        db.refresh(new_user_group)

        return new_user_group

    except Exception as e:
        print(f"Internal Server Error: {str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal Server Error")


# Read all user groups with pagination and optional search
@router.get("/", status_code=status.HTTP_200_OK)
def get_user_groups(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user) ,
    limit: int = 10,  # Number of user groups per page
    offset: int = 0,  # Offset for pagination
    search: str = "",  # Optional search filter
):
    try:
        print("Fetching user groups with pagination and search...")

        # Base query
        query = db.query(UserGroup)

        # Apply search filter if a keyword is provided
        if search:
            search_filter = or_(
                UserGroup.ms_user_id.ilike(f"%{search}%"),
                UserGroup.ms_group_id.ilike(f"%{search}%"),
            )
            query = query.filter(search_filter)

        # Get total number of user groups
        total_all_data = query.count()

        # Apply pagination
        user_groups = query.order_by(desc(UserGroup.created)).offset(offset).limit(limit).all()

        # Count number of user groups in the current response
        total_data = len(user_groups)

        # Format the response as required
        response = {
            "total_data": total_data,  # Number of user groups in this response
            "total_all_data": total_all_data,  # Total number of user groups without pagination
            "offset": offset,  # Current offset
            "user_groups": user_groups  # List of user groups
        }

        return response

    except Exception as e:
        print(f"Internal Server Error: {str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal Server Error")

# Read user groups by user_id
@router.get("/by-user/{user_id}", status_code=status.HTTP_200_OK)
def get_user_groups_by_user_id(
    user_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
    limit: int = 10,  # Number of user groups per page
    offset: int = 0  # Offset for pagination
):
    try:
        # Base query: Fetch user groups by user_id
        query = db.query(UserGroup).filter(UserGroup.ms_user_id == user_id)

        # Get total number of user groups
        total_all_data = query.count()

        # Apply pagination
        user_groups = query.order_by(desc(UserGroup.created)).offset(offset).limit(limit).all()

        # Count number of user groups in the current response
        total_data = len(user_groups)

        # Format the response with pagination details
        response = {
            "total_data": total_data,  # Number of user groups in this response
            "total_all_data": total_all_data,  # Total number of user groups for this user_id
            "offset": offset,  # Current offset
            "user_groups": user_groups  # List of user groups
        }

        return response

    except Exception as e:
        print(f"Internal Server Error: {str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal Server Error")

@router.get("/by-group/{group_id}", status_code=status.HTTP_200_OK)
def get_user_groups_by_group_id(
    group_id: str,
    db: Session = Depends(get_db),
    limit: int = 10,  # Number of user groups per page
    offset: int = 0,  # Offset for pagination
    current_user: User = Depends(get_current_user),
):
    try:
        # Define the SQL query for fetching user groups by group_id with pagination
        sql_query = text("""
            SELECT
                mug.ms_user_group_id,
                mu.ms_user_name AS username,
                mg.ms_group_name AS group_name,
                mug.ms_group_id,
                mug.isactive,
                mug.isdefault,
                mug.created,
                mug.updated,
                mug.ms_user_id
            FROM ms_user_group mug
            LEFT JOIN ms_user mu ON mu.ms_user_id = mug.ms_user_id
            LEFT JOIN ms_group mg ON mg.ms_group_id = mug.ms_group_id
            WHERE mug.ms_group_id = :group_id
            ORDER BY mug.created DESC
            LIMIT :limit OFFSET :offset
        """)

        # Execute the query to get the paginated user groups
        result = db.execute(sql_query, {"group_id": group_id, "limit": limit, "offset": offset})

        # Fetch all the records
        user_groups = result.fetchall()

        # Map the result to a dictionary manually
        user_groups_dict = [
            {
                "ms_user_group_id": row.ms_user_group_id,
                "username": row.username,
                "group_name": row.group_name,
                "ms_group_id": row.ms_group_id,
                "ms_user_id": row.ms_user_id,
                "isactive": row.isactive,
                "isdefault": row.isdefault,
                "created": row.created,
                "updated": row.updated
            }
            for row in user_groups
        ]

        # Define another SQL query to count the total number of user groups for the group_id
        count_query = text("""
            SELECT COUNT(*) FROM ms_user_group WHERE ms_group_id = :group_id
        """)

        # Execute the count query
        total_all_data = db.execute(count_query, {"group_id": group_id}).scalar()

        # Get the number of user groups in the current response (for `total_data`)
        total_data = len(user_groups_dict)

        # Format the response with pagination details
        response = {
            "total_data": total_data,  # Number of user groups in this response
            "total_all_data": total_all_data,  # Total number of user groups for this group_id
            "offset": offset,  # Current offset
            "user_groups": user_groups_dict  # List of user groups
        }

        return response

    except Exception as e:
        print(f"Internal Server Error: {str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal Server Error")



# Delete a user group by its ms_user_group_id
@router.delete("/{user_group_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_user_group(user_group_id: str, db: Session = Depends(get_db), current_user: User = Depends(get_current_user)):
    try:
        user_group = db.query(UserGroup).filter(UserGroup.ms_user_group_id == user_group_id).first()

        if not user_group:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User group not found")

        # Delete the user group
        db.delete(user_group)
        db.commit()

        return {"message": "User group deleted successfully"}

    except Exception as e:
        print(f"Internal Server Error: {str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal Server Error")


