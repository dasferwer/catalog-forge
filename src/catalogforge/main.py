import asyncio
import hashlib
import json
import os
from contextlib import asynccontextmanager, suppress
from datetime import timedelta
from typing import Annotated, Literal
from uuid import UUID, uuid4

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request, Response
from pydantic import BaseModel, Field
from sqlalchemy import text

from .auth import User, current_user
from .auth import router as auth_router
from .config import settings
from .db import engine
from .observability import instrument
from .service import event


@asynccontextmanager
async def lifespan(app):
    settings.upload_dir.mkdir(parents=True, exist_ok=True)
    yield
    await engine.dispose()


app = FastAPI(
    title="CatalogForge",
    version="0.1.0",
    lifespan=lifespan,
    description="Streaming CSV/JSON/JSONL import with validation, resumable checkpoints and atomic catalog updates.",
)
app.include_router(auth_router)
instrument(app)
Key = Annotated[
    str,
    Header(alias="Idempotency-Key", min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_.:-]+$"),
]


class CatalogInput(BaseModel):
    name: str = Field(min_length=1, max_length=120, pattern=r"\S")


class ColumnMap(BaseModel):
    sku: str = Field(default="sku", min_length=1, max_length=80)
    name: str = Field(default="name", min_length=1, max_length=80)
    price: str = Field(default="price", min_length=1, max_length=80)
    stock: str = Field(default="stock", min_length=1, max_length=80)


class ImportInput(BaseModel):
    catalog_id: UUID
    format: Literal["csv", "jsonl", "json"] = "csv"
    column_map: ColumnMap = Field(default_factory=ColumnMap)
    delimiter: Literal[",", ";", "\t"] = ","
    mode: Literal["upsert", "validate_only"] = "upsert"
    error_policy: Literal["skip", "reject"] = "skip"


async def owned_catalog(conn, catalog_id, user_id):
    row = (
        (
            await conn.execute(
                text("SELECT * FROM catalogs WHERE id=:id AND user_id=:user"),
                {"id": catalog_id, "user": user_id},
            )
        )
        .mappings()
        .first()
    )
    if row is None:
        raise HTTPException(404, "Catalog not found")
    return row


async def owned_job(conn, job_id, user_id, lock=False):
    suffix = " FOR UPDATE" if lock else ""
    row = (
        (
            await conn.execute(
                text("SELECT * FROM import_jobs WHERE id=:id AND user_id=:user" + suffix),
                {"id": job_id, "user": user_id},
            )
        )
        .mappings()
        .first()
    )
    if row is None:
        raise HTTPException(404, "Import not found")
    return row


def public_job(row):
    return {
        key: value
        for key, value in row.items()
        if key not in ("lease_token", "source_path", "request_hash", "idempotency_key")
    }


@app.get("/health", tags=["Operations"])
async def health():
    async with engine.connect() as conn:
        await conn.execute(text("SELECT 1"))
        age = await conn.scalar(
            text(
                "SELECT extract(epoch FROM clock_timestamp()-seen_at) FROM worker_heartbeats WHERE name='importer'"
            )
        )
    return {
        "status": "ok",
        "database": "ok",
        "importer_age_seconds": float(age) if age is not None else None,
    }


@app.post("/catalogs", status_code=201, tags=["Catalogs"])
async def create_catalog(data: CatalogInput, user: User = Depends(current_user)):
    async with engine.begin() as conn:
        row = (
            (
                await conn.execute(
                    text(
                        "INSERT INTO catalogs(id,user_id,name) VALUES (:id,:user,:name) ON CONFLICT(user_id,name) DO NOTHING RETURNING *"
                    ),
                    {"id": uuid4(), "user": user.id, "name": data.name},
                )
            )
            .mappings()
            .first()
        )
        if row is None:
            raise HTTPException(409, "Catalog name already exists")
        return dict(row)


@app.get("/catalogs", tags=["Catalogs"])
async def catalogs(
    user: User = Depends(current_user),
    limit: int = Query(default=50, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
):
    async with engine.connect() as conn:
        return [
            dict(r)
            for r in (
                await conn.execute(
                    text(
                        "SELECT id,name,version,created_at FROM catalogs WHERE user_id=:user ORDER BY created_at,id LIMIT :limit OFFSET :offset"
                    ),
                    {"user": user.id, "limit": limit, "offset": offset},
                )
            ).mappings()
        ]


@app.get("/catalogs/{catalog_id}", tags=["Catalogs"])
async def catalog(catalog_id: UUID, user: User = Depends(current_user)):
    async with engine.connect() as conn:
        row = await owned_catalog(conn, catalog_id, user.id)
        count = await conn.scalar(
            text("SELECT count(*) FROM products WHERE catalog_id=:id"), {"id": catalog_id}
        )
        return {**dict(row), "product_count": count}


@app.get("/catalogs/{catalog_id}/products", tags=["Catalogs"])
async def products(
    catalog_id: UUID,
    user: User = Depends(current_user),
    limit: int = Query(default=50, ge=1, le=1000),
    after_sku: str = Query(default="", max_length=80),
):
    async with engine.connect() as conn:
        await owned_catalog(conn, catalog_id, user.id)
        rows = (
            (
                await conn.execute(
                    text(
                        "SELECT sku,name,price,stock,version FROM products WHERE catalog_id=:id AND sku>:after ORDER BY sku LIMIT :limit"
                    ),
                    {"id": catalog_id, "after": after_sku, "limit": limit},
                )
            )
            .mappings()
            .all()
        )
        return [{**dict(r), "price": str(r["price"])} for r in rows]


@app.post("/imports", status_code=201, tags=["Imports"])
async def create_import(
    data: ImportInput, response: Response, idempotency_key: Key, user: User = Depends(current_user)
):
    fingerprint = hashlib.sha256(
        json.dumps(data.model_dump(mode="json"), sort_keys=True).encode()
    ).hexdigest()
    async with engine.begin() as conn:
        await conn.execute(
            text("SELECT pg_advisory_xact_lock(hashtextextended(:key,0))"),
            {"key": f"{user.id}:{idempotency_key}"},
        )
        await owned_catalog(conn, data.catalog_id, user.id)
        previous = (
            (
                await conn.execute(
                    text("SELECT * FROM import_jobs WHERE user_id=:user AND idempotency_key=:key"),
                    {"user": user.id, "key": idempotency_key},
                )
            )
            .mappings()
            .first()
        )
        if previous:
            if previous["request_hash"] != fingerprint:
                raise HTTPException(409, "Idempotency key was used with different parameters")
            response.status_code = 200
            return public_job(previous)
        row = (
            (
                await conn.execute(
                    text("""INSERT INTO import_jobs(id,catalog_id,user_id,format,column_map,delimiter,mode,error_policy,idempotency_key,request_hash)
            VALUES (:id,:catalog,:user,:format,CAST(:mapping AS jsonb),:delimiter,:mode,:policy,:key,:hash) RETURNING *"""),
                    {
                        "id": uuid4(),
                        "catalog": data.catalog_id,
                        "user": user.id,
                        "format": data.format,
                        "mapping": data.column_map.model_dump_json(),
                        "delimiter": data.delimiter,
                        "mode": data.mode,
                        "policy": data.error_policy,
                        "key": idempotency_key,
                        "hash": fingerprint,
                    },
                )
            )
            .mappings()
            .one()
        )
        await event(conn, row["id"], "created")
        return public_job(row)


@app.put(
    "/imports/{job_id}/file",
    status_code=202,
    tags=["Imports"],
    openapi_extra={
        "requestBody": {
            "required": True,
            "content": {
                "application/octet-stream": {"schema": {"type": "string", "format": "binary"}}
            },
        }
    },
)
async def upload(
    job_id: UUID,
    request: Request,
    x_content_sha256: Annotated[str, Header(pattern=r"^[A-Fa-f0-9]{64}$")],
    user: User = Depends(current_user),
):
    async with engine.begin() as conn:
        job = await owned_job(conn, job_id, user.id, True)
        now = await conn.scalar(text("SELECT clock_timestamp()"))
        expired = job["status"] == "uploading" and job["lease_until"] <= now
        if job["status"] != "awaiting_upload" and not expired:
            raise HTTPException(
                409, "Import already has an upload; read its status before retrying"
            )
        token = uuid4()
        await conn.execute(
            text(
                "UPDATE import_jobs SET status='uploading',lease_token=:token,lease_until=:lease,error=NULL WHERE id=:id"
            ),
            {
                "id": job_id,
                "token": token,
                "lease": now + timedelta(seconds=settings.upload_lease_seconds),
            },
        )
    directory = settings.upload_dir / str(job_id)
    directory.mkdir(parents=True, exist_ok=True)
    temporary = directory / f"{token}.part"
    complete = directory / f"{token}.source"
    total = 0
    digest = hashlib.sha256()
    try:
        with temporary.open("xb") as file:
            async for chunk in request.stream():
                total += len(chunk)
                if total > settings.max_upload_bytes:
                    raise HTTPException(413, "Source exceeds configured size limit")
                digest.update(chunk)
                await asyncio.to_thread(file.write, chunk)
            await asyncio.to_thread(file.flush)
            await asyncio.to_thread(os.fsync, file.fileno())
        if digest.hexdigest() != x_content_sha256.lower():
            raise HTTPException(422, "SHA-256 mismatch; retry the upload with the correct checksum")
        await asyncio.to_thread(os.replace, temporary, complete)
        # Make the rename durable on the local filesystem before making the job visible.
        directory_fd = os.open(directory, os.O_RDONLY)
        try:
            await asyncio.to_thread(os.fsync, directory_fd)
        finally:
            os.close(directory_fd)
        async with engine.begin() as conn:
            row = (
                (
                    await conn.execute(
                        text("""UPDATE import_jobs SET status='ready',source_path=:path,source_sha256=:sha,
                source_size_bytes=:size,lease_token=NULL,lease_until=NULL WHERE id=:id AND status='uploading'
                AND lease_token=:token RETURNING *"""),
                        {
                            "path": str(complete.relative_to(settings.upload_dir)),
                            "sha": digest.hexdigest(),
                            "size": total,
                            "id": job_id,
                            "token": token,
                        },
                    )
                )
                .mappings()
                .first()
            )
            if row is None:
                raise HTTPException(409, "Upload lease changed or import was cancelled")
            await event(conn, job_id, "uploaded", {"bytes": total})
            result = public_job(row)
        return result
    except Exception:
        with suppress(FileNotFoundError):
            temporary.unlink()
        with suppress(FileNotFoundError):
            complete.unlink()
        async with engine.begin() as conn:
            await conn.execute(
                text(
                    "UPDATE import_jobs SET status='awaiting_upload',lease_token=NULL,lease_until=NULL WHERE id=:id AND status='uploading' AND lease_token=:token"
                ),
                {"id": job_id, "token": token},
            )
        raise


@app.get("/imports", tags=["Imports"])
async def imports(
    user: User = Depends(current_user),
    limit: int = Query(default=50, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
):
    async with engine.connect() as conn:
        return [
            public_job(r)
            for r in (
                await conn.execute(
                    text(
                        "SELECT * FROM import_jobs WHERE user_id=:user ORDER BY created_at DESC,id LIMIT :limit OFFSET :offset"
                    ),
                    {"user": user.id, "limit": limit, "offset": offset},
                )
            ).mappings()
        ]


@app.get("/imports/{job_id}", tags=["Imports"])
async def import_status(job_id: UUID, user: User = Depends(current_user)):
    async with engine.connect() as conn:
        job = await owned_job(conn, job_id, user.id)
        samples = await conn.scalar(
            text("SELECT count(*) FROM row_errors WHERE job_id=:id"), {"id": job_id}
        )
        return {
            **public_job(job),
            "retained_errors": samples,
            "errors_truncated": samples < job["invalid_rows"],
        }


@app.post("/imports/{job_id}/{action}", tags=["Imports"])
async def control(
    job_id: UUID, action: Literal["pause", "resume", "cancel"], user: User = Depends(current_user)
):
    async with engine.begin() as conn:
        job = await owned_job(conn, job_id, user.id, True)
        if action == "resume":
            if job["status"] != "paused":
                raise HTTPException(409, "Only paused imports may be resumed")
            await conn.execute(
                text("UPDATE import_jobs SET status='ready',pause_requested=false WHERE id=:id"),
                {"id": job_id},
            )
        elif action == "pause":
            if job["status"] not in ("ready", "running", "paused"):
                raise HTTPException(409, "Import cannot be paused in its current state")
            if job["status"] == "ready":
                await conn.execute(
                    text(
                        "UPDATE import_jobs SET status='paused',pause_requested=true WHERE id=:id"
                    ),
                    {"id": job_id},
                )
            else:
                await conn.execute(
                    text("UPDATE import_jobs SET pause_requested=true WHERE id=:id"), {"id": job_id}
                )
        else:
            if job["status"] in ("succeeded", "failed"):
                raise HTTPException(409, "Completed import cannot be cancelled")
            if job["status"] == "running":
                await conn.execute(
                    text("UPDATE import_jobs SET cancel_requested=true WHERE id=:id"),
                    {"id": job_id},
                )
            else:
                await conn.execute(
                    text(
                        "UPDATE import_jobs SET status='cancelled',cancel_requested=true,lease_token=NULL,lease_until=NULL WHERE id=:id"
                    ),
                    {"id": job_id},
                )
        await event(conn, job_id, action + ".requested")
        current = (
            (await conn.execute(text("SELECT * FROM import_jobs WHERE id=:id"), {"id": job_id}))
            .mappings()
            .one()
        )
        return public_job(current)


@app.get("/imports/{job_id}/errors", tags=["Reports"])
async def errors(
    job_id: UUID,
    user: User = Depends(current_user),
    limit: int = Query(default=100, ge=1, le=1000),
    after_row: int = Query(default=0, ge=0),
):
    async with engine.connect() as conn:
        await owned_job(conn, job_id, user.id)
        return [
            dict(r)
            for r in (
                await conn.execute(
                    text(
                        "SELECT row_number,code FROM row_errors WHERE job_id=:id AND row_number>:after ORDER BY row_number LIMIT :limit"
                    ),
                    {"id": job_id, "after": after_row, "limit": limit},
                )
            ).mappings()
        ]


@app.get("/imports/{job_id}/preview", tags=["Reports"])
async def preview(
    job_id: UUID, user: User = Depends(current_user), limit: int = Query(default=50, ge=1, le=100)
):
    async with engine.connect() as conn:
        await owned_job(conn, job_id, user.id)
        return [
            {**dict(r), "price": str(r["price"])}
            for r in (
                await conn.execute(
                    text(
                        "SELECT sku,row_number,name,price,stock FROM staged_products WHERE job_id=:id ORDER BY sku LIMIT :limit"
                    ),
                    {"id": job_id, "limit": limit},
                )
            ).mappings()
        ]


@app.get("/imports/{job_id}/events", tags=["Reports"])
async def events(
    job_id: UUID,
    user: User = Depends(current_user),
    after_id: int = Query(default=0, ge=0),
    limit: int = Query(default=100, ge=1, le=1000),
):
    async with engine.connect() as conn:
        await owned_job(conn, job_id, user.id)
        return [
            dict(r)
            for r in (
                await conn.execute(
                    text(
                        "SELECT id,type,details,created_at FROM import_events WHERE job_id=:id AND id>:after ORDER BY id LIMIT :limit"
                    ),
                    {"id": job_id, "after": after_id, "limit": limit},
                )
            ).mappings()
        ]


@app.delete("/imports/{job_id}", tags=["Imports"])
async def delete_import(job_id: UUID, user: User = Depends(current_user)):
    async with engine.begin() as conn:
        job = await owned_job(conn, job_id, user.id, True)
        if job["status"] not in ("succeeded", "failed", "cancelled"):
            raise HTTPException(409, "Only terminal imports may be removed")
        path = settings.upload_dir / job["source_path"] if job["source_path"] else None
        await conn.execute(text("DELETE FROM import_jobs WHERE id=:id"), {"id": job_id})
    cleaned = True
    if path is not None:
        try:
            path.unlink(missing_ok=True)
            with suppress(OSError):
                path.parent.rmdir()
        except OSError:
            cleaned = False
    return {"deleted": True, "source_file_removed": cleaned, "catalog_products_preserved": True}
