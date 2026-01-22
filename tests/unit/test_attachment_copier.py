import httpx
import pytest
from pydantic import HttpUrl

from braintrust_migrate.attachments import AttachmentCopier
from braintrust_migrate.client import BraintrustClient
from braintrust_migrate.config import BraintrustOrgConfig, MigrationConfig


@pytest.mark.asyncio
async def test_attachment_copier_rewrites_braintrust_attachment_reference_and_does_not_leak_auth():
    src_base = "https://api.source.dev"
    dst_base = "https://api.dest.dev"
    download_url = "https://download.example/file.bin"
    upload_url = "https://upload.example/put"

    src_org_id = "org_source"
    dst_org_id = "org_dest"
    src_key = "src-key-1"
    payload_bytes = b"hello-world"

    src_attachment_meta_calls = 0
    src_download_calls = 0
    dst_upload_meta_calls = 0
    dst_upload_put_calls = 0
    dst_status_calls = 0

    def source_handler(request: httpx.Request) -> httpx.Response:
        nonlocal src_attachment_meta_calls, src_download_calls
        if str(request.url) == f"{src_base}/ping":
            return httpx.Response(200, json={"org_id": src_org_id})
        if str(request.url).startswith(f"{src_base}/attachment"):
            src_attachment_meta_calls += 1
            assert request.method == "GET"
            assert request.url.params.get("key") == src_key
            return httpx.Response(
                200,
                json={"downloadUrl": download_url, "status": {"upload_status": "done"}},
            )
        if str(request.url) == download_url:
            src_download_calls += 1
            # must not forward Braintrust auth header to download domain
            assert request.headers.get("Authorization") is None
            return httpx.Response(200, content=payload_bytes)
        raise AssertionError(
            f"Unexpected source request: {request.method} {request.url}"
        )

    def dest_handler(request: httpx.Request) -> httpx.Response:
        nonlocal dst_upload_meta_calls, dst_upload_put_calls, dst_status_calls
        if str(request.url) == f"{dst_base}/ping":
            return httpx.Response(200, json={"org_id": dst_org_id})
        if str(request.url) == f"{dst_base}/attachment":
            dst_upload_meta_calls += 1
            assert request.method == "POST"
            return httpx.Response(
                200, json={"signedUrl": upload_url, "headers": {"X-Test": "1"}}
            )
        if str(request.url) == upload_url:
            dst_upload_put_calls += 1
            assert request.method == "PUT"
            assert request.headers.get("Authorization") is None
            assert request.headers.get("X-Test") == "1"
            assert request.content == payload_bytes
            return httpx.Response(200)
        if str(request.url) == f"{dst_base}/attachment/status":
            dst_status_calls += 1
            assert request.method == "POST"
            return httpx.Response(200, json={})
        raise AssertionError(f"Unexpected dest request: {request.method} {request.url}")

    src_cfg = BraintrustOrgConfig(api_key="src", url=HttpUrl(src_base))
    dst_cfg = BraintrustOrgConfig(api_key="dst", url=HttpUrl(dst_base))
    mig_cfg = MigrationConfig(copy_attachments=True, attachment_max_bytes=10_000)

    src_client = BraintrustClient(src_cfg, mig_cfg, org_name="source")
    dst_client = BraintrustClient(dst_cfg, mig_cfg, org_name="dest")
    src_client._http_client = httpx.AsyncClient(
        transport=httpx.MockTransport(source_handler)
    )
    dst_client._http_client = httpx.AsyncClient(
        transport=httpx.MockTransport(dest_handler)
    )

    try:
        copier = AttachmentCopier(
            src_client, dst_client, max_bytes=mig_cfg.attachment_max_bytes
        )

        event = {
            "input": {
                "img": {
                    "type": "braintrust_attachment",
                    "key": src_key,
                    "filename": "a.png",
                    "content_type": "image/png",
                }
            }
        }
        rewritten = await copier.rewrite_event_in_place(event)
        assert rewritten == 1
        new_ref = event["input"]["img"]
        assert new_ref["type"] == "braintrust_attachment"
        assert new_ref["key"] != src_key
        assert new_ref["filename"] == "a.png"
        assert new_ref["content_type"] == "image/png"

        # Second rewrite should hit cache (no more download/upload)
        event2 = {
            "input": {
                "img": {
                    "type": "braintrust_attachment",
                    "key": src_key,
                    "filename": "a.png",
                    "content_type": "image/png",
                }
            }
        }
        rewritten2 = await copier.rewrite_event_in_place(event2)
        assert rewritten2 == 1
        assert event2["input"]["img"]["key"] == new_ref["key"]

        assert src_attachment_meta_calls == 1
        assert src_download_calls == 1
        assert dst_upload_meta_calls == 1
        assert dst_upload_put_calls == 1
        assert dst_status_calls == 1
    finally:
        await src_client._http_client.aclose()
        await dst_client._http_client.aclose()
