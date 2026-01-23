"""Attachment migration utilities.

Currently supports copying Braintrust-managed attachments:
  {"type": "braintrust_attachment", "key": "...", "filename": "...", "content_type": "..."}
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from typing import Any

import structlog

from braintrust_migrate.client import BraintrustClient

logger = structlog.get_logger(__name__)


AttachmentRef = dict[str, Any]


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

        # 3) Request signed upload URL from destination org
        dest_key = str(uuid.uuid4())
        upload_meta = await self.dest_client.with_retry(
            "attachment_metadata_dest",
            lambda: self.dest_client.raw_request(
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

        # 4) Upload to object store (no auth header)
        await self._upload_bytes(signed_url, headers, data)

        # 5) Mark status "done"
        await self.dest_client.with_retry(
            "attachment_status_dest",
            lambda: self.dest_client.raw_request(
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

    async def _upload_bytes(
        self, url: str, headers: dict[str, Any], data: bytes
    ) -> None:
        http_client = self.dest_client._http_client
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
