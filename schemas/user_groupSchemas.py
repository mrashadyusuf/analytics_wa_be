from pydantic import BaseModel
from datetime import datetime
from typing import Optional

# Schema for creating a new User Group
class UserGroupCreate(BaseModel):
    ms_user_id: str
    ms_group_id: str
    isactive: str  # Use '1' for active, '0' for inactive
    isdefault: str  # Use '1' for default, '0' for non-default
    createdby: str

    class Config:
        orm_mode = True

# Schema for updating an existing User Group
class UserGroupUpdate(BaseModel):
    isactive: str
    isdefault: str
    updatedby: str

    class Config:
        orm_mode = True

# Schema for reading User Group information
class UserGroupResponse(BaseModel):
    ms_user_group_id: str
    ms_user_id: str
    ms_group_id: str
    isactive: str
    isdefault: str
    createdby: str
    created: datetime
    updatedby: Optional[str] = None
    updated: Optional[datetime] = None

    class Config:
        orm_mode = True

class UserGroupCreate(BaseModel):
    ms_user_id: str
    ms_group_id: str
    isactive: str  # Use '1' for active, '0' for inactive
    isdefault: str  # Use '1' for default, '0' for non-default
    createdby: str  # The user who created this entry
    updatedby:str

    class Config:
        orm_mode = True
