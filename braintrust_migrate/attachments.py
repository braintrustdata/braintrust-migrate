"""Attachment migration utilities.

Currently supports copying Braintrust-managed attachments:
  {"type": "braintrust_attachment", "key": "...", "filename": "...", "content_type": "..."}
"""

from __future__ import annotations

import json as _json
import uuid
from dataclasses import dataclass, field
from typing import Any

import structlog

from braintrust_migrate.batching import approx_json_bytes
from braintrust_migrate.client import BraintrustClient

logger = structlog.get_logger(__name__)


AttachmentRef = dict[str, Any]

# Braintrust enforces a ~20MB per-span limit on individual logging upload
# requests (this also bounds the logs3 overflow path). We spill below it with
# headroom so the small inline attachment reference plus the rest of the row
# stays comfortably under the cap.
DEFAULT_MAX_EVENT_BYTES = 18 * 1024 * 1024

# Fields eligible to be spilled into a JSON attachment when a row is too large,
# ordered loosely by how likely they are to hold the bulk of the payload. We
# never spill small/indexable fields (scores, metrics, tags, ids).
SPILLABLE_FIELDS: tuple[str, ...] = (
    "input",
    "output",
    "metadata",
    "expected",
    "error",
    "span_attributes",
)

# Don't convert a field to an attachment unless it is itself meaningfully large;
# otherwise we'd spill several small fields without getting the row under the cap.
MIN_SPILL_FIELD_BYTES = 64 * 1024


def _is_braintrust_attachment_ref(v: Any) -> bool:
    return (
        isinstance(v, dict)
        and v.get("type") == "braintrust_attachment"
        and isinstance(v.get("key"), str)
        and isinstance(v.get("filename"), str)
        and isinstance(v.get("content_type"), str)
    )


def _walk_and_collect_attachment_refs(obj: Any, out: list[AttachmentRef]) -> None:
    if isinstance(obj, dict):
        if _is_braintrust_attachment_ref(obj):
            out.append(obj)  # modified in-place later
            return
        for v in obj.values():
            _walk_and_collect_attachment_refs(v, out)
        return
    if isinstance(obj, list):
        for v in obj:
            _walk_and_collect_attachment_refs(v, out)


@dataclass(slots=True)
class AttachmentCopier:
    source_client: BraintrustClient
    dest_client: BraintrustClient
    max_bytes: int
    _logger: Any = field(init=False, repr=False)
    _cache: dict[str, AttachmentRef] = field(
        default_factory=dict, init=False, repr=False
    )

    def __post_init__(self) -> None:
        self._logger = logger.bind(component="AttachmentCopier")

    async def rewrite_event_in_place(self, event: dict[str, Any]) -> int:
        """Find and rewrite any braintrust_attachment references in-place.

        Returns:
            Number of attachments rewritten.
        """
        refs: list[AttachmentRef] = []
        _walk_and_collect_attachment_refs(event, refs)
        if not refs:
            return 0

        rewritten = 0
        for ref in refs:
            src_key = ref["key"]
            cached = self._cache.get(src_key)
            if cached is not None:
                ref.clear()
                ref.update(cached)
                rewritten += 1
                continue

            dest_ref = await self._copy_one(ref)
            self._cache[src_key] = dest_ref
            ref.clear()
            ref.update(dest_ref)
            rewritten += 1

        return rewritten

    async def _copy_one(self, src_ref: AttachmentRef) -> AttachmentRef:
        filename = str(src_ref["filename"])
        content_type = str(src_ref["content_type"])
        src_key = str(src_ref["key"])

        src_org_id = await self.source_client.get_org_id()
        dst_org_id = await self.dest_client.get_org_id()

        # 1) Get download URL from source org
        meta = await self.source_client.with_retry(
            "attachment_metadata_source",
            lambda: self.source_client.raw_request(
                "GET",
                "/attachment",
                params={
                    "key": src_key,
                    "filename": filename,
                    "content_type": content_type,
                    "org_id": src_org_id,
                },
                timeout=120.0,
            ),
        )
        if not isinstance(meta, dict) or "downloadUrl" not in meta:
            raise RuntimeError(f"Unexpected attachment metadata response: {meta}")
        download_url = meta.get("downloadUrl")
        status = meta.get("status")
        if isinstance(status, dict) and status.get("upload_status") not in (
            None,
            "done",
        ):
            raise RuntimeError(f"Attachment not ready for download (status={status})")
        if not isinstance(download_url, str) or not download_url:
            raise RuntimeError(f"Invalid downloadUrl: {download_url}")

        # 2) Download bytes (no auth header)
        data = await self._download_bytes(download_url)

        # 3-5) Upload to destination object store and return the new reference.
        return await upload_bytes_as_attachment(
            self.dest_client,
            data=data,
            filename=filename,
            content_type=content_type,
            dst_org_id=dst_org_id,
        )

    async def _download_bytes(self, url: str) -> bytes:
        http_client = self.source_client._http_client
        if http_client is None:
            raise RuntimeError("Source http client not initialized")

        async with http_client.stream(
            "GET", url, follow_redirects=True, headers={}
        ) as resp:
            resp.raise_for_status()
            chunks: list[bytes] = []
            total = 0
            async for chunk in resp.aiter_bytes():
                total += len(chunk)
                if total > self.max_bytes:
                    raise RuntimeError(
                        f"Attachment exceeds MIGRATION_ATTACHMENT_MAX_BYTES ({self.max_bytes} bytes)"
                    )
                chunks.append(chunk)
            return b"".join(chunks)


async def _upload_bytes_to_signed_url(
    dest_client: BraintrustClient,
    url: str,
    headers: dict[str, Any],
    data: bytes,
) -> None:
    http_client = dest_client._http_client
    if http_client is None:
        raise RuntimeError("Destination http client not initialized")

    out_headers: dict[str, str] = {str(k): str(v) for k, v in headers.items()}
    # Azure Blob requires this header for PUTs to SAS URLs.
    if "blob.core.windows.net" in url and "x-ms-blob-type" not in {
        k.lower() for k in out_headers.keys()
    }:
        out_headers["x-ms-blob-type"] = "BlockBlob"

    resp = await http_client.request(
        "PUT", url, content=data, headers=out_headers, follow_redirects=True
    )
    resp.raise_for_status()


async def upload_bytes_as_attachment(
    dest_client: BraintrustClient,
    *,
    data: bytes,
    filename: str,
    content_type: str,
    dst_org_id: str | None = None,
) -> AttachmentRef:
    """Upload raw bytes to the destination org's object store.

    Mirrors the SDK's attachment upload flow (POST /attachment -> PUT to signed
    URL -> POST /attachment/status) and returns a `braintrust_attachment`
    reference that can be embedded inline in an event.
    """
    if dst_org_id is None:
        dst_org_id = await dest_client.get_org_id()

    dest_key = str(uuid.uuid4())
    upload_meta = await dest_client.with_retry(
        "attachment_metadata_dest",
        lambda: dest_client.raw_request(
            "POST",
            "/attachment",
            json={
                "key": dest_key,
                "filename": filename,
                "content_type": content_type,
                "org_id": dst_org_id,
            },
            timeout=120.0,
        ),
    )
    if not isinstance(upload_meta, dict):
        raise RuntimeError(
            f"Unexpected attachment upload metadata response: {upload_meta}"
        )
    signed_url = upload_meta.get("signedUrl")
    headers = upload_meta.get("headers")
    if not isinstance(signed_url, str) or not isinstance(headers, dict):
        raise RuntimeError(f"Invalid signed upload response: {upload_meta}")

    await _upload_bytes_to_signed_url(dest_client, signed_url, headers, data)

    await dest_client.with_retry(
        "attachment_status_dest",
        lambda: dest_client.raw_request(
            "POST",
            "/attachment/status",
            json={
                "key": dest_key,
                "org_id": dst_org_id,
                "status": {"upload_status": "done"},
            },
            timeout=120.0,
        ),
    )

    return {
        "type": "braintrust_attachment",
        "key": dest_key,
        "filename": filename,
        "content_type": content_type,
    }


@dataclass(slots=True)
class OversizeFieldSpiller:
    """Spill oversized event fields into JSON attachments on the destination.

    Braintrust enforces a ~20MB per-span limit on individual logging upload
    requests (this also bounds the logs3 overflow path). Some migrated spans
    exceed it because a single field — typically ``input``/``output``/
    ``metadata`` — holds a very large JSON blob. This rewrites such fields into
    ``braintrust_attachment`` references uploaded to the destination object
    store, shrinking the inline row below the cap while keeping the data
    accessible via the attachment viewer in the UI.
    """

    dest_client: BraintrustClient
    max_event_bytes: int = DEFAULT_MAX_EVENT_BYTES
    spill_fields: tuple[str, ...] = SPILLABLE_FIELDS
    min_field_bytes: int = MIN_SPILL_FIELD_BYTES
    _logger: Any = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self._logger = logger.bind(component="OversizeFieldSpiller")

    async def spill_event_in_place(
        self, event: dict[str, Any], event_bytes: int
    ) -> tuple[int, int]:
        """Spill the largest eligible fields until ``event`` is under the cap.

        Args:
            event: The shaped insert row. Modified in place.
            event_bytes: Precomputed approximate serialized size of ``event`` so
                callers that already measured it don't pay for it twice.

        Returns:
            ``(fields_spilled, new_event_bytes)``. When nothing is spilled,
            ``new_event_bytes`` equals the input ``event_bytes``.
        """
        if event_bytes <= self.max_event_bytes:
            return 0, event_bytes

        # Rank eligible fields largest-first. Skip values already turned into
        # attachment references and fields too small to be worth spilling.
        candidates: list[tuple[str, int]] = []
        for name in self.spill_fields:
            if name not in event:
                continue
            value = event[name]
            if _is_braintrust_attachment_ref(value):
                continue
            field_bytes = approx_json_bytes(value)
            if field_bytes < self.min_field_bytes:
                continue
            candidates.append((name, field_bytes))
        candidates.sort(key=lambda kv: kv[1], reverse=True)

        spilled = 0
        # Running estimate used only to decide when to stop spilling; the exact
        # size is recomputed once at the end for accurate byte accounting.
        estimated = event_bytes
        for name, field_bytes in candidates:
            if estimated <= self.max_event_bytes:
                break
            value = event[name]
            ref = await upload_bytes_as_attachment(
                self.dest_client,
                data=_json.dumps(value, ensure_ascii=False).encode("utf-8"),
                filename=f"{name}.json",
                content_type="application/json",
            )
            event[name] = ref
            estimated = estimated - field_bytes + approx_json_bytes(ref)
            spilled += 1
            self._logger.info(
                "Spilled oversize field to attachment",
                field=name,
                field_bytes=field_bytes,
                attachment_key=ref["key"],
                event_id=event.get("id"),
            )

        if spilled == 0:
            # Nothing eligible was large enough; leave the row alone (the caller's
            # oversize-isolation path will report it if it still fails to insert).
            return 0, event_bytes

        return spilled, approx_json_bytes(event)
