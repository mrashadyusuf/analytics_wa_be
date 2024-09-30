from pydantic import BaseModel
from typing import Optional
from datetime import datetime

# Schema for creating and updating a user
class UserCreateUpdate(BaseModel):
    ms_user_name: str
    ms_user_username: str
    ms_user_password: str
    ms_user_email: str
    isactive: Optional[str] = '1'
    ms_user_token: Optional[str] = ''
    created_by: Optional[str] = None
    updated_by: Optional[str] = None

# Schema for response (exclude sensitive fields like password)
class UserResponse(BaseModel):
    ms_user_id: str
    ms_user_name: str
    ms_user_username: str
    ms_user_email: str
    isactive: str
    created_dt: datetime
    created_by:str
    updated_dt: datetime
    updated_by: str

    class Config:
        orm_mode = True
