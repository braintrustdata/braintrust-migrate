import gzip
import json

import httpx
import pytest
from pydantic import HttpUrl

from braintrust_migrate.client import BraintrustClient
from braintrust_migrate.config import BraintrustOrgConfig, MigrationConfig


@pytest.mark.asyncio
async def test_raw_request_follows_303_redirect_without_leaking_auth_and_parses_gz():
    project_id = "7052b097-7051-42d9-b7d6-c97ece46b744"
    api_url = "https://api.braintrust.dev"
    fetch_url = f"{api_url}/v1/project_logs/{project_id}/fetch"
    s3_url = "https://example-bucket.s3.us-east-1.amazonaws.com/responses/abc.json.gz"

    payload = {"events": [{"id": "e1"}], "cursor": None}
    gz = gzip.compress(json.dumps(payload).encode("utf-8"))

    def handler(request: httpx.Request) -> httpx.Response:
        if str(request.url) == fetch_url:
            return httpx.Response(303, headers={"Location": s3_url})
        if str(request.url) == s3_url:
            # Ensure we do NOT forward the Braintrust auth header to S3.
            assert request.headers.get("Authorization") is None
            return httpx.Response(
                200, content=gz, headers={"Content-Type": "application/json"}
            )
        raise AssertionError(f"Unexpected request url: {request.url}")

    org_config = BraintrustOrgConfig(api_key="test", url=HttpUrl(api_url))
    mig_config = MigrationConfig()
    client = BraintrustClient(
        org_config=org_config, migration_config=mig_config, org_name="source"
    )
    client._http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))

    try:
        out = await client.raw_request(
            "POST", f"/v1/project_logs/{project_id}/fetch", json={"limit": 1}
        )
        assert out == payload
    finally:
        await client._http_client.aclose()
