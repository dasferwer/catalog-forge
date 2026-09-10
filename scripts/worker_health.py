import asyncio
import sys

from sqlalchemy import text

from catalogforge.db import engine


async def main():
    async with engine.connect() as conn:
        age = await conn.scalar(
            text(
                "SELECT extract(epoch FROM clock_timestamp()-seen_at) FROM worker_heartbeats WHERE name=:name"
            ),
            {"name": sys.argv[1]},
        )
    await engine.dispose()
    if age is None or float(age) > 30:
        raise SystemExit(1)


if __name__ == "__main__":
    asyncio.run(main())
