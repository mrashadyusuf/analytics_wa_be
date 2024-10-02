from pydantic import BaseModel
from typing import Optional
from datetime import datetime

# Schema for creating and updating a user
class UserCreateUpdate(BaseModel):
    ms_user_name: str
    ms_user_username: str
    ms_user_password: Optional[str] = None
    ms_user_email: str
    isactive: Optional[str] = ' Y'
    ms_user_token: Optional[str] = ''
    role: str
    phone_number: Optional[str] = None
    created_by: Optional[str] = None
    updated_by: Optional[str] = None
    ms_group_id: Optional[str] = None

# Schema for response (exclude sensitive fields like password)
class UserResponse(BaseModel):
    ms_user_id: str
    ms_user_name: str
    ms_user_username: str
    ms_user_email: str
    isactive: str
    role: Optional[str] = None
    phone_number: Optional[str] = None
    created_dt: Optional[datetime] = None
    created_by: Optional[str] = None
    updated_dt: Optional[datetime] = None
    updated_by: Optional[str] = None
    ms_group_id: Optional[str] = None

    

    class Config:
        orm_mode = True
