"""Manual end-to-end test for oversize-field attachment spilling.

Both source and dest can be SaaS (api.braintrust.dev). SaaS won't accept a
>20MB span on ingest, so instead we lower the migrator's spill threshold to a
small value (default 1MB) at RUNTIME (no package changes), seed a span whose
`input` is larger than that (but well under 20MB so SaaS accepts it on the
source), migrate just that project's logs, and verify the destination span's
`input` came across as a `braintrust_attachment` reference.

Side effects (writes to the orgs your .env points at):
  - creates/uses a source project and a dest project (names below),
  - logs one span to the source project,
  - migrates that project's logs to the dest project.

Run:
  uv run python scripts/manual_spill_e2e.py
Optional env overrides:
  SPILL_E2E_THRESHOLD_BYTES (default 1048576)
  SPILL_E2E_BLOB_BYTES      (default 1572864 ~1.5MB)
  SPILL_E2E_SOURCE_PROJECT / SPILL_E2E_DEST_PROJECT
"""

from __future__ import annotations

import asyncio
import json
import os
import tempfile
import uuid
from datetime import UTC, datetime
from pathlib import Path

import httpx
from dotenv import load_dotenv

from braintrust_migrate.attachments import OversizeFieldSpiller
from braintrust_migrate.btql import btql_quote
from braintrust_migrate.client import create_client_pair
from braintrust_migrate.config import Config
from braintrust_migrate.resources.logs import LogsMigrator
from braintrust_migrate.sdk_logs import SDKProjectLogsWriter

THRESHOLD = int(os.getenv("SPILL_E2E_THRESHOLD_BYTES", str(1 * 1024 * 1024)))
BLOB_BYTES = int(os.getenv("SPILL_E2E_BLOB_BYTES", str(1_572_864)))  # ~1.5MB
SRC_PROJECT = os.getenv("SPILL_E2E_SOURCE_PROJECT", "spill-e2e-src")
DST_PROJECT = os.getenv("SPILL_E2E_DEST_PROJECT", "spill-e2e-dst")
POLL_TIMEOUT_S = 90.0
POLL_INTERVAL_S = 3.0


async def _btql_find_span(client, project_id: str, span_id: str) -> dict | None:
    query = (
        "select: *\n"
        f"from: project_logs('{btql_quote(project_id)}') spans\n"
        f"filter: span_id = '{btql_quote(span_id)}'\n"
        "limit: 1"
    )
    resp = await client.with_retry(
        "btql_find_span",
        lambda: client.raw_request("POST", "/btql", json={"query": query}),
    )
    data = resp.get("data") if isinstance(resp, dict) else None
    if isinstance(data, list) and data:
        return data[0]
    return None


async def _poll_for_span(client, project_id: str, span_id: str, label: str) -> dict:
    deadline = asyncio.get_event_loop().time() + POLL_TIMEOUT_S
    while True:
        span = await _btql_find_span(client, project_id, span_id)
        if span is not None:
            print(f"  [{label}] span {span_id} is queryable")
            return span
        if asyncio.get_event_loop().time() > deadline:
            raise TimeoutError(f"[{label}] span {span_id} did not appear within {POLL_TIMEOUT_S}s")
        print(f"  [{label}] waiting for span to be queryable…")
        await asyncio.sleep(POLL_INTERVAL_S)


async def _download_attachment(client, ref: dict) -> bytes:
    org_id = await client.get_org_id()
    meta = await client.with_retry(
        "attachment_download_meta",
        lambda: client.raw_request(
            "GET",
            "/attachment",
            params={
                "key": ref["key"],
                "filename": ref["filename"],
                "content_type": ref["content_type"],
                "org_id": org_id,
            },
        ),
    )
    url = meta.get("downloadUrl")
    async with httpx.AsyncClient() as h:
        r = await h.get(url, follow_redirects=True)
        r.raise_for_status()
        return r.content


async def main() -> None:
    load_dotenv()
    config = Config.from_env()
    print(f"source url: {config.source.url}  dest url: {config.destination.url}")
    print(f"threshold={THRESHOLD} bytes, blob={BLOB_BYTES} bytes")

    async with create_client_pair(
        config.source, config.destination, config.migration
    ) as (src, dst):
        # 1) Ensure projects exist.
        src_proj = await src.create_project(name=SRC_PROJECT)
        dst_proj = await dst.create_project(name=DST_PROJECT)
        src_pid, dst_pid = src_proj["id"], dst_proj["id"]
        print(f"source project {SRC_PROJECT}={src_pid}  dest project {DST_PROJECT}={dst_pid}")

        # 2) Seed a >threshold span on the source.
        span_id = str(uuid.uuid4())
        blob = "x" * BLOB_BYTES
        row = {
            "id": str(uuid.uuid4()),
            "span_id": span_id,
            "root_span_id": span_id,
            "input": {"blob": blob},
            "created": datetime.now(UTC).isoformat(),
        }
        writer = SDKProjectLogsWriter(src, src_pid)
        await writer.write_rows([row])
        print(f"seeded source span span_id={span_id} (input ~{BLOB_BYTES} bytes)")

        await _poll_for_span(src, src_pid, span_id, "source")

        # 3) Migrate just this project's logs, with the spill threshold lowered.
        with tempfile.TemporaryDirectory() as tmp:
            migrator = LogsMigrator(
                src, dst, Path(tmp), page_limit=50, use_seen_db=True
            )
            migrator.set_destination_project_id(dst_pid)
            migrator._max_event_bytes = THRESHOLD
            migrator._spiller = OversizeFieldSpiller(
                dest_client=dst, max_event_bytes=THRESHOLD
            )

            result = await migrator.migrate_all(src_pid)
            print(
                "migrate_all:",
                {k: result.get(k) for k in ("migrated", "spilled_fields", "failed", "errors")},
            )

        # 4) Verify the dest span's input is now an attachment reference.
        dst_span = await _poll_for_span(dst, dst_pid, span_id, "dest")
        dst_input = dst_span.get("input")
        print("dest input:", json.dumps(dst_input)[:200])

        ok = (
            isinstance(dst_input, dict)
            and dst_input.get("type") == "braintrust_attachment"
            and dst_input.get("content_type") == "application/json"
        )
        if not ok:
            print("❌ FAIL: dest input is not a braintrust_attachment reference")
            return

        # 5) Download the attachment and confirm it round-tripped the original value.
        data = await _download_attachment(dst, dst_input)
        recovered = json.loads(data.decode("utf-8"))
        match = recovered == {"blob": blob}
        print(f"attachment bytes={len(data)} content_matches_original={match}")
        print("✅ PASS" if match else "❌ FAIL: attachment content mismatch")


if __name__ == "__main__":
    asyncio.run(main())
