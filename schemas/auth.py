# schemas/user.py
from datetime import date
from pydantic import BaseModel, Field
from typing import List


class LoginSchema(BaseModel):
    ms_user_username: str = Field(..., min_length=1, max_length=255)
    ms_user_password: str = Field(..., min_length=1, max_length=255)
    ms_user_type: str = Field(..., min_length=1, max_length=255)


class ChangePasswordSchema(BaseModel):
    ms_user_old_password: str = Field(..., min_length=1, max_length=255)
    ms_user_password: str = Field(..., min_length=1, max_length=255)
    ms_user_confirm_password: str = Field(..., min_length=1, max_length=255)


class ResetPasswordSchema(BaseModel):
    ms_user_id: str = Field()


class UpdatePasswordSchema(BaseModel):
    ms_user_email: str = Field(..., min_length=1, max_length=255)
    ms_user_token: str = Field(..., min_length=1, max_length=5)
    ms_user_password: str = Field(..., min_length=1, max_length=255)
    ms_user_confirm_password: str = Field(..., min_length=1, max_length=255)

class VetifyOTPSchema(BaseModel):
    ms_user_email: str = Field(..., min_length=1, max_length=255)
    ms_user_token: str = Field(..., min_length=1, max_length=5)

class ResetForgotPasswordSchema(BaseModel):
    ms_user_email: str = Field(..., min_length=1, max_length=255)
    ms_user_password: str = Field(..., min_length=1, max_length=255)
    ms_user_confirm_password: str = Field(..., min_length=1, max_length=255)


class ForgotPasswordSchema(BaseModel):
    ms_user_email: str = Field(..., min_length=1, max_length=255)


class Auth(LoginSchema):
    ms_user_id: int
