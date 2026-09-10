import asyncio
from uuid import UUID

from sqlalchemy import text

from catalogforge.auth import hasher
from catalogforge.db import engine


async def seed():
    async with engine.begin() as conn:
        await conn.execute(
            text(
                "INSERT INTO users(id,email,password_hash) VALUES (:id,'demo@example.com',:hash) ON CONFLICT(email) DO NOTHING"
            ),
            {
                "id": UUID("13000000-0000-0000-0000-000000000001"),
                "hash": hasher.hash("CatalogForgeDemo123!"),
            },
        )
        user = await conn.scalar(text("SELECT id FROM users WHERE email='demo@example.com'"))
        await conn.execute(
            text(
                "INSERT INTO catalogs(id,user_id,name) VALUES (:id,:user,'Demo catalog') ON CONFLICT(user_id,name) DO NOTHING"
            ),
            {"id": UUID("13000000-0000-0000-0000-000000000010"), "user": user},
        )
    await engine.dispose()
    print("Seed ready: demo@example.com / CatalogForgeDemo123!")


if __name__ == "__main__":
    asyncio.run(seed())
