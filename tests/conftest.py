from datetime import UTC, datetime, timedelta
from uuid import uuid4

import httpx
import jwt
import pytest
from sqlalchemy import text

from catalogforge.config import settings
from catalogforge.db import engine
from catalogforge.main import app

assert settings.testing and settings.database_url.endswith("_test"), (
    "Tests require an isolated *_test database"
)


@pytest.fixture(autouse=True)
async def clean():
    async with engine.begin() as conn:
        await conn.execute(
            text(
                "TRUNCATE users,catalogs,products,import_jobs,staged_products,row_errors,import_events,worker_heartbeats RESTART IDENTITY CASCADE"
            )
        )
    import shutil
    from pathlib import Path

    assert settings.upload_dir == Path("/tmp/catalogforge-tests")
    shutil.rmtree(settings.upload_dir, ignore_errors=True)
    settings.upload_dir.mkdir(parents=True, exist_ok=True)
    yield


@pytest.fixture
async def client():
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        yield client


@pytest.fixture
async def identities():
    users = {}
    async with engine.begin() as conn:
        for role in ("admin", "alice", "bob"):
            uid = uuid4()
            await conn.execute(
                text(
                    "INSERT INTO users(id,email,password_hash,role) VALUES (:id,:email,:hash,:role)"
                ),
                {
                    "id": uid,
                    "email": f"{role}@example.com",
                    "hash": "unused",
                    "role": "admin" if role == "admin" else "user",
                },
            )
            now = datetime.now(UTC)
            token = jwt.encode(
                {
                    "sub": str(uid),
                    "iat": now,
                    "exp": now + timedelta(hours=1),
                    "iss": "catalogforge",
                    "aud": "catalogforge",
                },
                settings.jwt_secret,
                algorithm="HS256",
            )
            users[role] = {"id": uid, "headers": {"Authorization": f"Bearer {token}"}}
    return users


@pytest.fixture(scope="session", autouse=True)
async def dispose_pool():
    yield
    await engine.dispose()


@pytest.fixture
async def catalog(client, identities):
    response = await client.post(
        "/catalogs", json={"name": "Test catalog"}, headers=identities["alice"]["headers"]
    )
    assert response.status_code == 201, response.text
    return response.json()
