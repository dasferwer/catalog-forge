import asyncio
import hashlib
import itertools
import json
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from catalogforge.config import settings
from catalogforge.db import engine
from catalogforge.parsing import rows
from catalogforge.service import claim_job, finalize, save_batch
from catalogforge.worker import process_job

CSV = b"sku,name,price,stock\na,First,10.00,2\nb,Second,20,3\n,Invalid,1,1\na,Updated,11,4\nc,Third,5,0\n"


def headers(identities, key=None, user="alice"):
    return {**identities[user]["headers"], "Idempotency-Key": key or uuid4().hex}


async def upload(client, identities, catalog, content=CSV, **options):
    response = await client.post(
        "/imports", json={"catalog_id": catalog["id"], **options}, headers=headers(identities)
    )
    assert response.status_code == 201, response.text
    job = response.json()
    uploaded = await client.put(
        f"/imports/{job['id']}/file",
        content=content,
        headers={
            **headers(identities),
            "Content-Type": "application/octet-stream",
            "X-Content-SHA256": hashlib.sha256(content).hexdigest(),
        },
    )
    assert uploaded.status_code == 202, uploaded.text
    return job


async def status(client, identities, job):
    return (await client.get(f"/imports/{job['id']}", headers=headers(identities))).json()


async def expire_lease():
    async with engine.begin() as conn:
        await conn.execute(
            text(
                "UPDATE import_jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE status='running'"
            )
        )


async def test_csv_normalization_duplicates_and_error_report(client, identities, catalog):
    job = await upload(client, identities, catalog)
    await process_job(await claim_job())
    result = await status(client, identities, job)
    assert result["status"] == "succeeded"
    assert (
        result["processed_rows"],
        result["valid_rows"],
        result["invalid_rows"],
        result["duplicate_rows"],
        result["inserted_rows"],
    ) == (5, 4, 1, 1, 3)
    products = (
        await client.get(f"/catalogs/{catalog['id']}/products", headers=headers(identities))
    ).json()
    assert [(p["sku"], p["price"], p["stock"]) for p in products] == [
        ("A", "11.00", 4),
        ("B", "20.00", 3),
        ("C", "5.00", 0),
    ]
    errors = (await client.get(f"/imports/{job['id']}/errors", headers=headers(identities))).json()
    assert errors == [{"row_number": 3, "code": "invalid_sku"}]


@pytest.mark.parametrize("format", ["csv", "jsonl", "json"])
async def test_resume_from_committed_checkpoint_across_formats(format, client, identities, catalog):
    values = [
        {"sku": f"item-{i}", "name": f"Name {i}", "price": "1.25", "stock": i} for i in range(5)
    ]
    if format == "csv":
        content = (
            "sku,name,price,stock\n"
            + "".join(f"{r['sku']},{r['name']},1.25,{r['stock']}\n" for r in values)
        ).encode()
    elif format == "jsonl":
        content = ("\n".join(json.dumps(r) for r in values) + "\n").encode()
    else:
        content = json.dumps(values).encode()
    job = await upload(client, identities, catalog, content, format=format)
    first = await claim_job()
    with (settings.upload_dir / first["source_path"]).open("rb") as file:
        assert await save_batch(first, list(itertools.islice(rows(file, first), 2)))
    async with engine.connect() as conn:
        assert await conn.scalar(text("SELECT count(*) FROM products")) == 0
    await expire_lease()
    resumed = await claim_job()
    assert resumed["processed_rows"] == 2 and resumed["recovery_count"] == 1
    await process_job(resumed)
    result = await status(client, identities, job)
    assert (
        result["status"] == "succeeded"
        and result["processed_rows"] == 5
        and result["inserted_rows"] == 5
    )


async def test_checkpoint_rollback_does_not_leave_staging_or_counters(
    client, identities, catalog, monkeypatch
):
    await upload(client, identities, catalog)
    job = await claim_job()
    with (settings.upload_dir / job["source_path"]).open("rb") as source:
        batch = list(itertools.islice(rows(source, job), 2))
    original = AsyncConnection.execute

    async def failing(self, statement, *args, **kwargs):
        if "SET processed_rows=processed_rows" in str(statement):
            raise RuntimeError("Injected failure after COPY")
        return await original(self, statement, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(AsyncConnection, "execute", failing)
        with pytest.raises(RuntimeError, match="after COPY"):
            await save_batch(job, batch)
    async with engine.connect() as conn:
        assert await conn.scalar(text("SELECT count(*) FROM staged_products")) == 0
        assert await conn.scalar(text("SELECT processed_rows FROM import_jobs")) == 0
    await process_job(job)
    async with engine.connect() as conn:
        assert await conn.scalar(text("SELECT status FROM import_jobs")) == "succeeded"


async def test_promotion_and_job_status_are_one_atomic_commit(
    client, identities, catalog, monkeypatch
):
    import catalogforge.service as service

    await upload(client, identities, catalog)
    job = await claim_job()
    original = service.event

    async def fail(conn, job_id, kind, details=None):
        if kind == "succeeded":
            raise RuntimeError("Injected failure during promotion")
        await original(conn, job_id, kind, details)

    with monkeypatch.context() as patch:
        patch.setattr(service, "event", fail)
        with pytest.raises(RuntimeError, match="promotion"):
            await process_job(job)
    async with engine.connect() as conn:
        assert await conn.scalar(text("SELECT count(*) FROM products")) == 0
        assert await conn.scalar(text("SELECT processed_rows FROM import_jobs")) == 5
        assert await conn.scalar(text("SELECT version FROM catalogs")) == 0
    assert await finalize(job)
    async with engine.connect() as conn:
        assert await conn.scalar(text("SELECT count(*) FROM products")) == 3
        assert await conn.scalar(text("SELECT version FROM catalogs")) == 1


async def test_stale_worker_cannot_commit_after_lease_recovery(client, identities, catalog):
    await upload(client, identities, catalog)
    old = await claim_job()
    with (settings.upload_dir / old["source_path"]).open("rb") as source:
        batch = list(itertools.islice(rows(source, old), 2))
    await expire_lease()
    new = await claim_job()
    assert old["lease_token"] != new["lease_token"]
    assert not await save_batch(old, batch)
    assert not await finalize(old)
    await process_job(new)
    async with engine.connect() as conn:
        assert await conn.scalar(text("SELECT processed_rows FROM import_jobs")) == 5


async def test_pause_resume_preserves_checkpoint(client, identities, catalog):
    public = await upload(client, identities, catalog)
    job = await claim_job()
    with (settings.upload_dir / job["source_path"]).open("rb") as source:
        iterator = rows(source, job)
        await save_batch(job, list(itertools.islice(iterator, 2)))
        assert (
            await client.post(f"/imports/{job['id']}/pause", headers=headers(identities))
        ).status_code == 200
        assert not await save_batch(job, list(itertools.islice(iterator, 2)))
    paused = await status(client, identities, public)
    assert paused["status"] == "paused" and paused["processed_rows"] == 2
    assert (
        await client.post(f"/imports/{job['id']}/resume", headers=headers(identities))
    ).status_code == 200
    await process_job(await claim_job())
    assert (await status(client, identities, public))["status"] == "succeeded"


async def test_cancel_never_publishes_partial_catalog(client, identities, catalog):
    public = await upload(client, identities, catalog)
    job = await claim_job()
    with (settings.upload_dir / job["source_path"]).open("rb") as source:
        iterator = rows(source, job)
        await save_batch(job, list(itertools.islice(iterator, 2)))
        await client.post(f"/imports/{job['id']}/cancel", headers=headers(identities))
        assert not await save_batch(job, list(itertools.islice(iterator, 2)))
    assert (await status(client, identities, public))["status"] == "cancelled"
    async with engine.connect() as conn:
        assert await conn.scalar(text("SELECT count(*) FROM products")) == 0


async def test_validate_only_and_reject_policy_do_not_change_catalog(client, identities, catalog):
    validate = await upload(client, identities, catalog, mode="validate_only")
    await process_job(await claim_job())
    assert (await status(client, identities, validate))["status"] == "succeeded"
    rejected = await upload(client, identities, catalog, error_policy="reject")
    await process_job(await claim_job())
    result = await status(client, identities, rejected)
    assert result["status"] == "failed" and result["error"] == "invalid_rows_rejected"
    async with engine.connect() as conn:
        assert await conn.scalar(text("SELECT count(*) FROM products")) == 0


async def test_reimport_identical_rows_is_unchanged_and_changed_rows_update(
    client, identities, catalog
):
    await upload(client, identities, catalog)
    await process_job(await claim_job())
    identical = await upload(client, identities, catalog)
    await process_job(await claim_job())
    same = await status(client, identities, identical)
    assert (same["inserted_rows"], same["updated_rows"], same["unchanged_rows"]) == (0, 0, 3)
    updated = await upload(
        client, identities, catalog, b"sku,name,price,stock\na,Changed,22.25,1\nd,New,1,1\n"
    )
    await process_job(await claim_job())
    changed = await status(client, identities, updated)
    assert (changed["inserted_rows"], changed["updated_rows"]) == (1, 1)
    async with engine.connect() as conn:
        assert await conn.scalar(text("SELECT count(*) FROM products")) == 4


async def test_error_samples_are_bounded_but_counts_are_complete(
    client, identities, catalog, monkeypatch
):
    monkeypatch.setattr(settings, "error_sample_limit", 3)
    data = b"sku,name,price,stock\n" + b",bad,-1,-1\n" * 12
    job = await upload(client, identities, catalog, data)
    await process_job(await claim_job())
    result = await status(client, identities, job)
    assert (
        result["invalid_rows"] == 12
        and result["retained_errors"] == 3
        and result["errors_truncated"]
    )


async def test_checksum_mismatch_can_be_retried_and_tampering_is_detected(
    client, identities, catalog
):
    response = await client.post(
        "/imports", json={"catalog_id": catalog["id"]}, headers=headers(identities)
    )
    job = response.json()
    mismatch = await client.put(
        f"/imports/{job['id']}/file",
        content=CSV,
        headers={**headers(identities), "X-Content-SHA256": "0" * 64},
    )
    assert mismatch.status_code == 422
    assert (await status(client, identities, job))["status"] == "awaiting_upload"
    retry = await client.put(
        f"/imports/{job['id']}/file",
        content=CSV,
        headers={**headers(identities), "X-Content-SHA256": hashlib.sha256(CSV).hexdigest()},
    )
    assert retry.status_code == 202
    claimed = await claim_job()
    (settings.upload_dir / claimed["source_path"]).write_bytes(b"tampered")
    await process_job(claimed)
    assert (await status(client, identities, job))["error"] == "source_checksum_changed"


async def test_import_idempotency_and_owner_isolation(client, identities, catalog):
    h = headers(identities, "same")
    results = await asyncio.gather(
        *[client.post("/imports", json={"catalog_id": catalog["id"]}, headers=h) for _ in range(10)]
    )
    assert sum(r.status_code == 201 for r in results) == 1
    assert len({r.json()["id"] for r in results}) == 1
    changed = await client.post(
        "/imports", json={"catalog_id": catalog["id"], "mode": "validate_only"}, headers=h
    )
    assert changed.status_code == 409
    job = results[0].json()
    for suffix in ("", "/errors", "/preview", "/events"):
        assert (
            await client.get(
                f"/imports/{job['id']}{suffix}", headers=headers(identities, user="bob")
            )
        ).status_code == 404
    assert (
        await client.get(
            f"/catalogs/{catalog['id']}/products", headers=headers(identities, user="bob")
        )
    ).status_code == 404
    assert (
        await client.put(
            f"/imports/{job['id']}/file",
            content=CSV,
            headers={
                **headers(identities, user="bob"),
                "X-Content-SHA256": hashlib.sha256(CSV).hexdigest(),
            },
        )
    ).status_code == 404


async def test_csv_multiline_unicode_mapping_and_checkpoint(client, identities, catalog):
    data = 'Артикул;Название;Цена;Остаток\nA;"Первое\nимя";12.50;2\nB;Второе;1;0\nC;Третье;3;4\n'.encode()
    job = await upload(
        client,
        identities,
        catalog,
        data,
        delimiter=";",
        column_map={"sku": "Артикул", "name": "Название", "price": "Цена", "stock": "Остаток"},
    )
    first = await claim_job()
    with (settings.upload_dir / first["source_path"]).open("rb") as source:
        await save_batch(first, list(itertools.islice(rows(source, first), 1)))
    await expire_lease()
    await process_job(await claim_job())
    assert (await status(client, identities, job))["processed_rows"] == 3
    items = (
        await client.get(f"/catalogs/{catalog['id']}/products", headers=headers(identities))
    ).json()
    assert items[0]["name"] == "Первое имя"


@pytest.mark.parametrize(
    "format,content",
    [("csv", b"sku,name\na,A\n"), ("json", b'{"not":"array"}'), ("json", b'[{"sku":"A"},broken]')],
)
async def test_malformed_source_is_failed_without_partial_products(
    format, content, client, identities, catalog
):
    job = await upload(client, identities, catalog, content, format=format)
    await process_job(await claim_job())
    assert (await status(client, identities, job))["status"] == "failed"
    async with engine.connect() as conn:
        assert await conn.scalar(text("SELECT count(*) FROM products")) == 0


@pytest.mark.parametrize("format", ["csv", "jsonl", "json"])
async def test_oversized_unused_field_never_publishes_products(format, client, identities, catalog):
    from catalogforge.parsing import MAX_RECORD_BYTES

    record = {
        "sku": "A",
        "name": "Name",
        "price": "1",
        "stock": 1,
        "unused": "x" * MAX_RECORD_BYTES,
    }
    if format == "csv":
        content = ("sku,name,price,stock,unused\nA,Name,1,1," + record["unused"] + "\n").encode()
    elif format == "jsonl":
        content = json.dumps(record).encode() + b"\n"
    else:
        content = json.dumps([record]).encode()
    job = await upload(client, identities, catalog, content, format=format)
    await process_job(await claim_job())
    assert (await status(client, identities, job))["status"] == "failed"
    async with engine.connect() as conn:
        assert await conn.scalar(text("SELECT count(*) FROM products")) == 0


async def test_json_cancel_and_resume_with_nested_unused_fields(
    client, identities, catalog, monkeypatch
):
    values = [
        {
            "sku": f"A{i}",
            "name": "Name",
            "price": "1.25",
            "stock": i,
            "unused": {"nested": ["x" * 1000]},
        }
        for i in range(7)
    ]
    public = await upload(client, identities, catalog, json.dumps(values).encode(), format="json")
    first = await claim_job()
    with (settings.upload_dir / first["source_path"]).open("rb") as source:
        assert await save_batch(first, list(itertools.islice(rows(source, first), 2)))
    await expire_lease()
    resumed = await claim_job()
    assert resumed["processed_rows"] == 2
    original = save_batch

    async def cancel_after_batch(job, batch):
        result = await original(job, batch)
        await client.post(f"/imports/{public['id']}/cancel", headers=headers(identities))
        return result

    monkeypatch.setattr("catalogforge.worker.save_batch", cancel_after_batch)
    await process_job(resumed)
    assert (await status(client, identities, public))["status"] == "cancelled"
    async with engine.connect() as conn:
        assert await conn.scalar(text("SELECT count(*) FROM products")) == 0


async def test_cleanup_removes_source_and_staging_but_preserves_products(
    client, identities, catalog
):
    public = await upload(client, identities, catalog)
    assert (
        await client.delete(f"/imports/{public['id']}", headers=headers(identities))
    ).status_code == 409
    job = await claim_job()
    path = settings.upload_dir / job["source_path"]
    await process_job(job)
    deleted = await client.delete(f"/imports/{public['id']}", headers=headers(identities))
    assert deleted.json() == {
        "deleted": True,
        "source_file_removed": True,
        "catalog_products_preserved": True,
    }
    assert not path.exists()
    async with engine.connect() as conn:
        assert await conn.scalar(text("SELECT count(*) FROM staged_products")) == 0
        assert await conn.scalar(text("SELECT count(*) FROM products")) == 3


async def test_upload_limit_and_concurrent_upload_claim(client, identities, catalog, monkeypatch):
    response = await client.post(
        "/imports", json={"catalog_id": catalog["id"]}, headers=headers(identities)
    )
    job = response.json()
    h = {**headers(identities), "X-Content-SHA256": hashlib.sha256(CSV).hexdigest()}
    with monkeypatch.context() as patch:
        patch.setattr(settings, "max_upload_bytes", 20)
        assert (
            await client.put(f"/imports/{job['id']}/file", content=CSV, headers=h)
        ).status_code == 413
    assert (await status(client, identities, job))["status"] == "awaiting_upload"
    results = await asyncio.gather(
        *[client.put(f"/imports/{job['id']}/file", content=CSV, headers=h) for _ in range(2)]
    )
    assert sorted(r.status_code for r in results) == [202, 409]


async def test_jsonl_rejects_non_finite_and_excess_precision_numbers(client, identities, catalog):
    payload = b'{"sku":"A","name":"Name","price":NaN,"stock":1}\n{"sku":"B","name":"Name","price":2.001,"stock":1}\n'
    job = await upload(client, identities, catalog, payload, format="jsonl")
    await process_job(await claim_job())
    result = await status(client, identities, job)
    assert result["invalid_rows"] == 2 and result["inserted_rows"] == 0
