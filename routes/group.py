from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from database import get_db
from models.group import Group
from schemas.groupSchemas import GroupCreate, GroupUpdate, GroupResponse
import uuid
from datetime import datetime, timedelta
from sqlalchemy import desc, or_
from auth import get_current_user, User
router = APIRouter()

# Create Group
@router.post("/", response_model=GroupResponse, status_code=status.HTTP_201_CREATED)
def create_group(
    group: GroupCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    # 1. Generate a unique group ID
    new_group_id = str(uuid.uuid4())

    # 2. Create the new group record for the database
    new_group = Group(
        ms_group_id=new_group_id,
        ms_group_name=group.ms_group_name,
        isactive=group.isactive,
        createdby=group.createdby,
        created=datetime.utcnow() + timedelta(hours=7),
        updatedby=current_user,
        updated = datetime.utcnow() + timedelta(hours=7),
    )

    # 3. Insert the new group into the database
    db.add(new_group)
    db.commit()
    db.refresh(new_group)

    return new_group


# Read all Groups
@router.get("/", status_code=status.HTTP_200_OK)
def get_groups(
    db: Session = Depends(get_db),
    limit: int = 10,  # Number of groups per page
    offset: int = 0,  # Offset for pagination
    search: str = "",  # Search keyword
    current_user: User = Depends(get_current_user),
):
    try:
        print("Fetching groups with pagination and search...")

        # Base query
        query = db.query(Group)

        # Apply search filter if a keyword is provided
        if search:
            search_filter = or_(
                Group.ms_group_name.ilike(f"%{search}%"),
            )
            query = query.filter(search_filter)

        # Get total number of groups (for pagination metadata)
        total_all_data = query.count()

        # Apply pagination (limit and offset)
        groups = query.order_by(desc(Group.created)).offset(offset).limit(limit).all()

        # Count the number of groups in the current response (for `total_data`)
        total_data = len(groups)

        # Format the response as required
        response = {
            "total_data": total_data,  # Number of groups in this response
            "total_all_data": total_all_data,  # Total number of groups without pagination
            "offset": offset,  # Current offset
            "groups": groups  # List of groups
        }

        return response

    except Exception as e:
        print(f"Internal Server Error: {str(e)}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal Server Error")

# Read a single Group by ID
@router.get("/{group_id}", response_model=GroupResponse, status_code=status.HTTP_200_OK)
def get_group_by_id(group_id: str, db: Session = Depends(get_db),current_user: User = Depends(get_current_user),):
    group = db.query(Group).filter(Group.ms_group_id == group_id).first()

    if not group:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Group not found")

    return group


# Update Group
@router.put("/{group_id}", response_model=GroupResponse, status_code=status.HTTP_200_OK)
def update_group(
    group_id: str,
    group_update: GroupUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    # Fetch the group
    group = db.query(Group).filter(Group.ms_group_id == group_id).first()

    if not group:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Group not found")

    # Update group details
    group.ms_group_name = group_update.ms_group_name
    group.isactive = group_update.isactive
    group.updatedby = group_update.updatedby
    group.updated = datetime.utcnow() + timedelta(hours=7)

    # Commit the changes
    db.commit()
    db.refresh(group)

    return group


# Delete Group
@router.delete("/{group_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_group(group_id: str, db: Session = Depends(get_db),current_user: User = Depends(get_current_user),):
    group = db.query(Group).filter(Group.ms_group_id == group_id).first()

    if not group:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Group not found")

    # Delete the group
    db.delete(group)
    db.commit()

    return {"message": "Group deleted successfully"}
