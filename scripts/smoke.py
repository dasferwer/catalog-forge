"""Live import smoke with streaming file generation and HTTP upload."""

import argparse
import hashlib
import http.client
import json
import os
import tempfile
import time
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen
from uuid import uuid4

BASE = os.environ.get("BASE_URL", "http://localhost:8000")


def call(method, path, data=None, token=None, key=None):
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    if key:
        headers["Idempotency-Key"] = key
    request = Request(
        BASE + path,
        data=json.dumps(data).encode() if data is not None else None,
        headers=headers,
        method=method,
    )
    try:
        with urlopen(request, timeout=120) as response:
            return response.status, json.load(response)
    except HTTPError as error:
        return error.code, json.load(error)


def new_user():
    credentials = {"email": f"catalog-{uuid4().hex}@example.com", "password": "SmokePassword123!"}
    assert call("POST", "/auth/register", credentials)[0] == 201
    return call("POST", "/auth/login", credentials)[1]["access_token"]


def source_file(count):
    with tempfile.NamedTemporaryFile(prefix="catalogforge-", suffix=".csv", delete=False) as handle:
        path = Path(handle.name)
        handle.write(b"sku,name,price,stock\n")
        for i in range(count):
            handle.write(f"SKU-{i:08d},Product {i},10.00,{i % 100}\n".encode())
        handle.write(b"SKU-00000000,Updated first item,12.50,7\n,Invalid item,-1,0\n")
    return path


def upload_file(token, job, path):
    with path.open("rb") as source:
        digest = hashlib.file_digest(source, "sha256").hexdigest()
    base = urlsplit(BASE)
    connection = http.client.HTTPConnection(base.hostname, base.port, timeout=120)
    try:
        connection.putrequest("PUT", f"/imports/{job['id']}/file")
        connection.putheader("Authorization", f"Bearer {token}")
        connection.putheader("Content-Type", "application/octet-stream")
        connection.putheader("Content-Length", str(path.stat().st_size))
        connection.putheader("X-Content-SHA256", digest)
        connection.endheaders()
        with path.open("rb") as source:
            while chunk := source.read(1024 * 1024):
                connection.send(chunk)
        response = connection.getresponse()
        body = json.loads(response.read())
        assert response.status == 202, body
        assert body["source_sha256"] == digest
        return body
    finally:
        connection.close()


def create_job(token, catalog, **options):
    key = uuid4().hex
    data = {"catalog_id": catalog["id"], **options}
    code, job = call("POST", "/imports", data, token, key)
    assert code == 201, job
    code, replay = call("POST", "/imports", data, token, key)
    assert code == 200 and replay["id"] == job["id"]
    return job


def wait_job(token, job, timeout=300):
    deadline = time.monotonic() + timeout
    last = -1
    while time.monotonic() < deadline:
        code, current = call("GET", f"/imports/{job['id']}", token=token)
        assert code == 200, current
        if current["status"] in ("succeeded", "failed", "cancelled"):
            return current
        bucket = current["processed_rows"] // 100000
        if bucket > last and bucket > 0:
            print(
                json.dumps(
                    {
                        "job_id": job["id"],
                        "processed_rows": current["processed_rows"],
                        "status": current["status"],
                    }
                ),
                flush=True,
            )
            last = bucket
        time.sleep(0.15)
    raise AssertionError(current)


def verify_result(result, count):
    assert result["status"] == "succeeded", result
    assert (
        result["processed_rows"],
        result["valid_rows"],
        result["invalid_rows"],
        result["duplicate_rows"],
        result["inserted_rows"],
    ) == (count + 2, count + 1, 1, 1, count), result
    assert result["peak_rss_bytes"] < 256 * 1024 * 1024, result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=10000)
    parser.add_argument("--benchmark", action="store_true")
    args = parser.parse_args()
    if not 1 <= args.rows <= 2_000_000:
        parser.error("--rows must be between 1 and 2000000")
    token = new_user()
    catalog = call("POST", "/catalogs", {"name": f"Smoke {uuid4().hex}"}, token)[1]
    path = source_file(args.rows)
    start = time.monotonic()
    try:
        job = create_job(token, catalog)
        upload_file(token, job, path)
        result = wait_job(token, job)
        verify_result(result, args.rows)
        products = call("GET", f"/catalogs/{catalog['id']}/products?limit=1", token=token)[1]
        assert products[0]["sku"] == "SKU-00000000" and products[0]["price"] == "12.50"
        if not args.benchmark:
            repeated = create_job(token, catalog)
            upload_file(token, repeated, path)
            repeated = wait_job(token, repeated)
            assert (
                repeated["inserted_rows"],
                repeated["updated_rows"],
                repeated["unchanged_rows"],
            ) == (0, 0, args.rows), repeated
            for format in ("jsonl", "json"):
                value = {
                    "sku": "NEW-" + format.upper(),
                    "name": "JSON item",
                    "price": "3.25",
                    "stock": 4,
                }
                path.write_text(json.dumps([value] if format == "json" else value))
                extra = create_job(token, catalog, format=format, mode="validate_only")
                upload_file(token, extra, path)
                extra = wait_job(token, extra)
                assert (
                    extra["status"] == "succeeded"
                    and extra["valid_rows"] == 1
                    and extra["inserted_rows"] == 0
                ), extra
        count = call("GET", f"/catalogs/{catalog['id']}", token=token)[1]["product_count"]
        assert count == args.rows
        print(
            json.dumps(
                {
                    "ok": True,
                    "catalog_id": catalog["id"],
                    "job_id": job["id"],
                    "source_rows": args.rows + 2,
                    "products": count,
                    "duplicate_rows": result["duplicate_rows"],
                    "invalid_rows": result["invalid_rows"],
                    "peak_worker_rss_mib": round(result["peak_rss_bytes"] / 1024**2, 2),
                    "elapsed_seconds": round(time.monotonic() - start, 2),
                    "formats": ["csv"] if args.benchmark else ["csv", "jsonl", "json"],
                }
            )
        )
    finally:
        path.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
