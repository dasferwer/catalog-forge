"""Из папки проекта проверяем восстановление и память, перезапуская его воркер."""

import argparse
import json
import subprocess
import time

import smoke

smoke.BASE = "http://localhost:8130"


def compose(*args):
    subprocess.run(["docker", "compose", *args], check=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=200000)
    args = parser.parse_args()
    if not 10000 <= args.rows <= 2_000_000:
        parser.error("Choose between 10000 and 2000000 rows")
    token = smoke.new_user()
    catalog = smoke.call("POST", "/catalogs", {"name": "Recovery catalog"}, token)[1]
    path = smoke.source_file(args.rows)
    start = time.monotonic()
    try:
        compose("stop", "--timeout", "5", "worker")
        job = smoke.create_job(token, catalog)
        smoke.upload_file(token, job, path)
        assert smoke.call("GET", f"/imports/{job['id']}", token=token)[1]["status"] == "ready"
        compose("up", "-d", "--no-deps", "worker")
        deadline = time.monotonic() + 45
        while time.monotonic() < deadline:
            current = smoke.call("GET", f"/imports/{job['id']}", token=token)[1]
            if 0 < current["processed_rows"] < args.rows and current["status"] == "running":
                break
            time.sleep(0.03)
        assert 0 < current["processed_rows"] < args.rows, current
        compose("kill", "--signal", "SIGKILL", "worker")
        stopped = smoke.call("GET", f"/imports/{job['id']}", token=token)[1]
        assert stopped["status"] == "running" and stopped["processed_rows"] > 0, stopped
        assert smoke.call("GET", f"/catalogs/{catalog['id']}", token=token)[1]["product_count"] == 0
        compose("up", "-d", "--no-deps", "worker")
        result = smoke.wait_job(token, job, timeout=600)
        smoke.verify_result(result, args.rows)
        assert result["recovery_count"] >= 1, result
        assert (
            smoke.call("GET", f"/catalogs/{catalog['id']}", token=token)[1]["product_count"]
            == args.rows
        )
        events = smoke.call("GET", f"/imports/{job['id']}/events", token=token)[1]
        recovered = [row for row in events if row["type"] == "recovered"]
        assert (
            recovered and recovered[0]["details"]["checkpoint_row"] == stopped["processed_rows"]
        ), events
        print(
            json.dumps(
                {
                    "ok": True,
                    "input_rows": args.rows + 2,
                    "products": args.rows,
                    "committed_checkpoint_before_kill": stopped["processed_rows"],
                    "resumed_from_same_checkpoint": True,
                    "partial_catalog_visible": False,
                    "recovery_count": result["recovery_count"],
                    "duplicate_rows": result["duplicate_rows"],
                    "invalid_rows": result["invalid_rows"],
                    "peak_worker_rss_mib": round(result["peak_rss_bytes"] / 1024**2, 2),
                    "elapsed_seconds": round(time.monotonic() - start, 2),
                    "job_id": job["id"],
                }
            )
        )
    finally:
        path.unlink(missing_ok=True)
        compose("up", "-d", "--no-deps", "worker")


if __name__ == "__main__":
    main()
