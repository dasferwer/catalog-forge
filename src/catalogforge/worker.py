import asyncio
import hashlib
import itertools
import logging
import signal
from contextlib import suppress

from sqlalchemy import text

from .config import settings
from .db import engine
from .parsing import SourceError, rows
from .service import claim_job, fail_job, finalize, save_batch

logger = logging.getLogger("importer")


async def process_job(job):
    path = settings.upload_dir / job["source_path"]
    try:
        with path.open("rb") as source:
            digest = hashlib.file_digest(source, "sha256").hexdigest()
            if digest != job["source_sha256"]:
                raise SourceError("source_checksum_changed")
            source.seek(0)
            iterator = rows(source, job)
            while True:
                batch = list(itertools.islice(iterator, settings.batch_size))
                if not batch:
                    await finalize(job)
                    break
                if not await save_batch(job, batch):
                    break
    except FileNotFoundError:
        await fail_job(job, "source_missing")
    except SourceError as error:
        await fail_job(job, str(error))


async def pulse(stop):
    while not stop.is_set():
        try:
            async with engine.begin() as conn:
                await conn.execute(
                    text(
                        "INSERT INTO worker_heartbeats(name) VALUES ('importer') ON CONFLICT(name) DO UPDATE SET seen_at=clock_timestamp()"
                    )
                )
        except Exception:
            logger.exception("heartbeat failed")
        with suppress(TimeoutError):
            await asyncio.wait_for(stop.wait(), timeout=2)


async def main():
    logging.basicConfig(level=logging.INFO)
    stop = asyncio.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        asyncio.get_running_loop().add_signal_handler(sig, stop.set)
    pulser = asyncio.create_task(pulse(stop))
    try:
        while not stop.is_set():
            try:
                job = await claim_job()
                if job:
                    logger.info("job_id=%s resume_row=%s", job["id"], job["processed_rows"])
                    await process_job(job)
                    continue
            except Exception:
                logger.exception("import transaction failed; last checkpoint remains recoverable")
            with suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=settings.worker_interval)
    finally:
        stop.set()
        await pulser
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
