# Pydantic models for request validation
from pydantic import BaseModel
from datetime import datetime

class GroupBase(BaseModel):
    ms_group_name: str
    isactive: str = "Y"  # Default value for active status
    url_dashboard_sales: str | None
    url_dashboard_customer: str | None

class GroupCreate(GroupBase):
    createdby: str

class GroupUpdate(GroupBase):
    updatedby: str

class GroupResponse(GroupBase):
    ms_group_id: str
    created: datetime
    updated: datetime | None

    class Config:
        orm_mode = True
