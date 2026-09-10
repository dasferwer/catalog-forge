import json
import resource
import sys
from datetime import timedelta
from uuid import uuid4

from sqlalchemy import text

from .config import settings
from .db import engine
from .parsing import normalize


async def event(conn, job_id, kind, details=None):
    await conn.execute(
        text(
            "INSERT INTO import_events(job_id,type,details) VALUES (:id,:type,CAST(:details AS jsonb))"
        ),
        {"id": job_id, "type": kind, "details": json.dumps(details or {})},
    )


def rss_bytes():
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return peak if sys.platform == "darwin" else peak * 1024


async def claim_job():
    async with engine.begin() as conn:
        row = (
            (
                await conn.execute(
                    text("""SELECT * FROM import_jobs WHERE status='ready'
            OR (status='running' AND lease_until<=clock_timestamp())
            ORDER BY created_at,id LIMIT 1 FOR UPDATE SKIP LOCKED""")
                )
            )
            .mappings()
            .first()
        )
        if row is None:
            return None
        if row["cancel_requested"] or row["pause_requested"]:
            status = "cancelled" if row["cancel_requested"] else "paused"
            await conn.execute(
                text(
                    "UPDATE import_jobs SET status=:status,lease_token=NULL,lease_until=NULL WHERE id=:id"
                ),
                {"id": row["id"], "status": status},
            )
            await event(conn, row["id"], status)
            return None
        now = await conn.scalar(text("SELECT clock_timestamp()"))
        token = uuid4()
        reclaimed = row["status"] == "running"
        updated = (
            (
                await conn.execute(
                    text("""UPDATE import_jobs SET status='running',lease_token=:token,lease_until=:lease,
            started_at=COALESCE(started_at,:now),recovery_count=recovery_count+:recovered WHERE id=:id RETURNING *"""),
                    {
                        "id": row["id"],
                        "token": token,
                        "lease": now + timedelta(seconds=settings.lease_seconds),
                        "now": now,
                        "recovered": int(reclaimed),
                    },
                )
            )
            .mappings()
            .one()
        )
        await event(
            conn,
            row["id"],
            "recovered" if reclaimed else "started",
            {"checkpoint_row": row["processed_rows"]},
        )
        return dict(updated)


async def locked_job(conn, job):
    current = (
        (
            await conn.execute(
                text("SELECT * FROM import_jobs WHERE id=:id FOR UPDATE"), {"id": job["id"]}
            )
        )
        .mappings()
        .one()
    )
    if current["status"] != "running" or current["lease_token"] != job["lease_token"]:
        return None
    if current["pause_requested"] or current["cancel_requested"]:
        status = "cancelled" if current["cancel_requested"] else "paused"
        await conn.execute(
            text(
                "UPDATE import_jobs SET status=:status,lease_token=NULL,lease_until=NULL WHERE id=:id"
            ),
            {"status": status, "id": job["id"]},
        )
        await event(conn, job["id"], status)
        return None
    return current


async def save_batch(job, batch):
    records = []
    errors = []
    for row in batch:
        normalized, error = (
            normalize(row.value, job["column_map"]) if row.error is None else (None, row.error)
        )
        if error:
            errors.append({"job": job["id"], "row": row.number, "code": error})
        else:
            records.append(
                (
                    normalized["sku"],
                    row.number,
                    normalized["name"],
                    normalized["price"],
                    normalized["stock"],
                    normalized["fingerprint"],
                )
            )
    async with engine.begin() as conn:
        current = await locked_job(conn, job)
        if current is None:
            return False
        if current["processed_rows"] != batch[0].number - 1:
            raise RuntimeError("Non-contiguous checkpoint")
        if records:
            await conn.execute(
                text(
                    "CREATE TEMP TABLE batch_rows(sku text,row_number bigint,name text,price numeric(12,2),stock integer,fingerprint text) ON COMMIT DROP"
                )
            )
            raw = await conn.get_raw_connection()
            await raw.driver_connection.copy_records_to_table(
                "batch_rows",
                records=records,
                columns=("sku", "row_number", "name", "price", "stock", "fingerprint"),
            )
            await conn.execute(
                text("""INSERT INTO staged_products(job_id,sku,row_number,name,price,stock,fingerprint)
                SELECT :job,sku,row_number,name,price,stock,fingerprint FROM (
                    SELECT DISTINCT ON (sku) * FROM batch_rows ORDER BY sku,row_number DESC
                ) batch ON CONFLICT(job_id,sku) DO UPDATE SET row_number=excluded.row_number,
                    name=excluded.name,price=excluded.price,stock=excluded.stock,fingerprint=excluded.fingerprint
                WHERE excluded.row_number>staged_products.row_number"""),
                {"job": job["id"]},
            )
        retained = await conn.scalar(
            text("SELECT count(*) FROM row_errors WHERE job_id=:id"), {"id": job["id"]}
        )
        sampled = errors[: max(0, settings.error_sample_limit - retained)]
        if sampled:
            await conn.execute(
                text("INSERT INTO row_errors(job_id,row_number,code) VALUES (:job,:row,:code)"),
                sampled,
            )
        now = await conn.scalar(text("SELECT clock_timestamp()"))
        await conn.execute(
            text("""UPDATE import_jobs SET processed_rows=processed_rows+:processed,
            valid_rows=valid_rows+:valid,invalid_rows=invalid_rows+:invalid,checkpoint_bytes=:offset,
            peak_rss_bytes=GREATEST(peak_rss_bytes,:rss),lease_until=:lease WHERE id=:id"""),
            {
                "processed": len(batch),
                "valid": len(records),
                "invalid": len(errors),
                "offset": batch[-1].offset,
                "rss": rss_bytes(),
                "lease": now + timedelta(seconds=settings.lease_seconds),
                "id": job["id"],
            },
        )
        return True


async def finalize(job):
    async with engine.begin() as conn:
        current = await locked_job(conn, job)
        if current is None:
            return False
        distinct = await conn.scalar(
            text("SELECT count(*) FROM staged_products WHERE job_id=:id"), {"id": job["id"]}
        )
        duplicate = current["valid_rows"] - distinct
        if current["error_policy"] == "reject" and current["invalid_rows"]:
            await conn.execute(
                text(
                    "UPDATE import_jobs SET status='failed',error='invalid_rows_rejected',duplicate_rows=:duplicates,finished_at=clock_timestamp(),lease_token=NULL,lease_until=NULL WHERE id=:id"
                ),
                {"duplicates": duplicate, "id": job["id"]},
            )
            await event(conn, job["id"], "failed", {"error": "invalid_rows_rejected"})
            return True
        inserted = updated = unchanged = 0
        if current["mode"] == "upsert":
            await conn.execute(
                text("SELECT id FROM catalogs WHERE id=:id FOR UPDATE"),
                {"id": current["catalog_id"]},
            )
            counts = (
                (
                    await conn.execute(
                        text("""SELECT
                count(*) FILTER(WHERE p.sku IS NULL) AS inserted,
                count(*) FILTER(WHERE p.sku IS NOT NULL AND p.fingerprint<>s.fingerprint) AS updated,
                count(*) FILTER(WHERE p.fingerprint=s.fingerprint) AS unchanged
                FROM staged_products s LEFT JOIN products p ON p.catalog_id=:catalog AND p.sku=s.sku
                WHERE s.job_id=:job"""),
                        {"catalog": current["catalog_id"], "job": job["id"]},
                    )
                )
                .mappings()
                .one()
            )
            inserted, updated, unchanged = (
                counts["inserted"],
                counts["updated"],
                counts["unchanged"],
            )
            await conn.execute(
                text("""INSERT INTO products(catalog_id,sku,name,price,stock,fingerprint)
                SELECT :catalog,sku,name,price,stock,fingerprint FROM staged_products WHERE job_id=:job
                ON CONFLICT(catalog_id,sku) DO UPDATE SET name=excluded.name,price=excluded.price,
                stock=excluded.stock,fingerprint=excluded.fingerprint,version=products.version+1,
                updated_at=clock_timestamp() WHERE products.fingerprint<>excluded.fingerprint"""),
                {"catalog": current["catalog_id"], "job": job["id"]},
            )
            if inserted or updated:
                await conn.execute(
                    text("UPDATE catalogs SET version=version+1 WHERE id=:id"),
                    {"id": current["catalog_id"]},
                )
        await conn.execute(
            text("""UPDATE import_jobs SET status='succeeded',duplicate_rows=:duplicates,
            inserted_rows=:inserted,updated_rows=:updated,unchanged_rows=:unchanged,finished_at=clock_timestamp(),
            lease_token=NULL,lease_until=NULL,peak_rss_bytes=GREATEST(peak_rss_bytes,:rss) WHERE id=:id"""),
            {
                "duplicates": duplicate,
                "inserted": inserted,
                "updated": updated,
                "unchanged": unchanged,
                "rss": rss_bytes(),
                "id": job["id"],
            },
        )
        await event(
            conn,
            job["id"],
            "succeeded",
            {"inserted": inserted, "updated": updated, "unchanged": unchanged},
        )
        return True


async def fail_job(job, code):
    async with engine.begin() as conn:
        row = await locked_job(conn, job)
        if row is not None:
            await conn.execute(
                text(
                    "UPDATE import_jobs SET status='failed',error=:error,lease_token=NULL,lease_until=NULL,finished_at=clock_timestamp() WHERE id=:id"
                ),
                {"id": job["id"], "error": code},
            )
            await event(conn, job["id"], "failed", {"error": code})
