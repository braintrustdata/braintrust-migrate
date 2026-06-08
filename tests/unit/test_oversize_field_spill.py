import json

import httpx
import pytest
from pydantic import HttpUrl

from braintrust_migrate.attachments import OversizeFieldSpiller
from braintrust_migrate.batching import approx_json_bytes
from braintrust_migrate.client import BraintrustClient
from braintrust_migrate.config import BraintrustOrgConfig, MigrationConfig


def _make_dest_client():
    """Destination client backed by a mock transport recording attachment ops."""
    dst_base = "https://api.dest.dev"
    upload_url = "https://upload.example/put"
    dst_org_id = "org_dest"

    calls = {"meta": 0, "put": 0, "status": 0, "uploaded": []}

    def dest_handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if url == f"{dst_base}/ping":
            return httpx.Response(200, json={"org_id": dst_org_id})
        if url == f"{dst_base}/attachment":
            calls["meta"] += 1
            assert request.method == "POST"
            body = json.loads(request.content)
            assert body["content_type"] == "application/json"
            return httpx.Response(
                200, json={"signedUrl": upload_url, "headers": {"X-Test": "1"}}
            )
        if url == upload_url:
            calls["put"] += 1
            assert request.method == "PUT"
            assert request.headers.get("Authorization") is None
            calls["uploaded"].append(request.content)
            return httpx.Response(200)
        if url == f"{dst_base}/attachment/status":
            calls["status"] += 1
            assert request.method == "POST"
            return httpx.Response(200, json={})
        raise AssertionError(f"Unexpected dest request: {request.method} {url}")

    cfg = BraintrustOrgConfig(api_key="dst", url=HttpUrl(dst_base))
    client = BraintrustClient(cfg, MigrationConfig(), org_name="dest")
    client._http_client = httpx.AsyncClient(
        transport=httpx.MockTransport(dest_handler)
    )
    return client, calls


@pytest.mark.asyncio
async def test_spills_largest_field_until_under_cap():
    client, calls = _make_dest_client()
    try:
        spiller = OversizeFieldSpiller(
            dest_client=client, max_event_bytes=1000, min_field_bytes=100
        )
        big_input = {"transcript": "x" * 5000}
        event = {
            "id": "evt-1",
            "input": big_input,
            "metadata": {"model": "gpt-4"},  # small: stays inline
            "scores": {"accuracy": 1},
        }
        size_before = approx_json_bytes(event)
        assert size_before > 1000

        spilled, new_size = await spiller.spill_event_in_place(event, size_before)

        assert spilled == 1
        assert new_size <= 1000
        assert new_size == approx_json_bytes(event)  # exact, not estimated
        ref = event["input"]
        assert ref["type"] == "braintrust_attachment"
        assert ref["filename"] == "input.json"
        assert ref["content_type"] == "application/json"
        # Small / indexable fields are left untouched.
        assert event["metadata"] == {"model": "gpt-4"}
        assert event["scores"] == {"accuracy": 1}
        # The uploaded bytes are the original field value as JSON.
        assert calls["meta"] == 1 and calls["put"] == 1 and calls["status"] == 1
        assert json.loads(calls["uploaded"][0].decode("utf-8")) == big_input
    finally:
        await client._http_client.aclose()


@pytest.mark.asyncio
async def test_event_under_cap_is_untouched_and_makes_no_requests():
    client, calls = _make_dest_client()
    try:
        spiller = OversizeFieldSpiller(dest_client=client, max_event_bytes=10**9)
        event = {"id": "evt-2", "input": {"a": 1}}
        spilled, new_size = await spiller.spill_event_in_place(
            event, approx_json_bytes(event)
        )
        assert spilled == 0
        assert event["input"] == {"a": 1}
        assert calls["meta"] == 0 and calls["put"] == 0 and calls["status"] == 0
    finally:
        await client._http_client.aclose()


@pytest.mark.asyncio
async def test_spills_multiple_fields_when_one_is_not_enough():
    client, calls = _make_dest_client()
    try:
        spiller = OversizeFieldSpiller(
            dest_client=client, max_event_bytes=1000, min_field_bytes=100
        )
        event = {
            "id": "evt-3",
            "input": {"a": "x" * 3000},
            "output": {"b": "y" * 3000},
        }
        spilled, new_size = await spiller.spill_event_in_place(
            event, approx_json_bytes(event)
        )
        # Either field alone leaves the row over the cap, so both spill.
        assert spilled == 2
        assert new_size <= 1000
        assert event["input"]["type"] == "braintrust_attachment"
        assert event["output"]["type"] == "braintrust_attachment"
        assert calls["meta"] == 2 and calls["put"] == 2 and calls["status"] == 2
    finally:
        await client._http_client.aclose()
