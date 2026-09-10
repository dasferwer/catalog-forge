from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import jwt
from fastapi import APIRouter, Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pwdlib import PasswordHash
from pydantic import BaseModel, EmailStr, Field
from sqlalchemy import text
from starlette.concurrency import run_in_threadpool

from .config import settings
from .db import engine

router = APIRouter(prefix="/auth", tags=["Authentication"])
bearer = HTTPBearer(auto_error=False)
hasher = PasswordHash.recommended()
DUMMY_HASH = hasher.hash("dummy-password-never-valid")


class Credentials(BaseModel):
    email: EmailStr
    password: str = Field(min_length=10, max_length=128)


class User(BaseModel):
    id: UUID
    email: str
    role: str


async def current_user(credentials: HTTPAuthorizationCredentials | None = Depends(bearer)) -> User:
    unauthorized = HTTPException(
        401, "Invalid or expired access token", headers={"WWW-Authenticate": "Bearer"}
    )
    if credentials is None:
        raise unauthorized
    try:
        claims = jwt.decode(
            credentials.credentials,
            settings.jwt_secret,
            algorithms=["HS256"],
            options={"require": ["sub", "exp", "iat", "iss", "aud"]},
            issuer=__package__,
            audience=__package__,
        )
        user_id = UUID(claims["sub"])
    except (jwt.InvalidTokenError, ValueError, KeyError):
        raise unauthorized from None
    async with engine.connect() as conn:
        row = (
            (
                await conn.execute(
                    text("SELECT id, email, role FROM users WHERE id=:id"), {"id": user_id}
                )
            )
            .mappings()
            .first()
        )
    if row is None:
        raise unauthorized
    return User(**row)


async def admin_user(user: User = Depends(current_user)) -> User:
    if user.role != "admin":
        raise HTTPException(403, "Administrator required")
    return user


@router.post("/register", status_code=201, response_model=User)
async def register(data: Credentials):
    hashed = await run_in_threadpool(hasher.hash, data.password)
    async with engine.begin() as conn:
        row = (
            (
                await conn.execute(
                    text("""
            INSERT INTO users(id, email, password_hash) VALUES (:id, :email, :hash)
            ON CONFLICT(email) DO NOTHING RETURNING id, email, role
        """),
                    {"id": uuid4(), "email": str(data.email).lower(), "hash": hashed},
                )
            )
            .mappings()
            .first()
        )
        if row is None:
            raise HTTPException(409, "Email already registered")
        return dict(row)


@router.post("/login")
async def login(data: Credentials):
    async with engine.connect() as conn:
        row = (
            (
                await conn.execute(
                    text("SELECT * FROM users WHERE email=:email"),
                    {"email": str(data.email).lower()},
                )
            )
            .mappings()
            .first()
        )
    valid = await run_in_threadpool(
        hasher.verify, data.password, row["password_hash"] if row else DUMMY_HASH
    )
    if row is None or not valid:
        raise HTTPException(401, "Invalid credentials")
    now = datetime.now(UTC)
    token = jwt.encode(
        {
            "sub": str(row["id"]),
            "iat": now,
            "exp": now + timedelta(minutes=settings.token_minutes),
            "iss": __package__,
            "aud": __package__,
        },
        settings.jwt_secret,
        algorithm="HS256",
    )
    return {
        "access_token": token,
        "token_type": "bearer",
        "expires_in": settings.token_minutes * 60,
    }
