"""SDK-backed logs3 writers for pre-shaped migration rows."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping, Sequence
from typing import Any

from braintrust_migrate.batching import iter_ordered_batches_by_count_and_bytes
from braintrust_migrate.client import BraintrustClient
from braintrust_migrate.streaming_utils import StreamingConfig

PROJECT_LOGS_LOG_ID = "g"


class SDKRowWriter:
    """Use the Braintrust Python SDK's logs3 transport for pre-shaped rows."""

    def __init__(
        self, dest_client: BraintrustClient, object_id_fields: Mapping[str, Any]
    ) -> None:
        self._dest_client = dest_client
        self._object_id_fields = dict(object_id_fields)
        self._background_logger: Any | None = None
        self._lazy_value_cls: Any | None = None

    def _ensure_logger(self) -> None:
        if self._background_logger is not None and self._lazy_value_cls is not None:
            return

        try:
            from braintrust.logger import HTTPConnection, _HTTPBackgroundLogger
            from braintrust.util import LazyValue
        except ImportError as exc:
            raise ImportError(
                "braintrust package is required for SDK-backed migration writes"
            ) from exc

        conn = HTTPConnection(str(self._dest_client.org_config.url).rstrip("/"))
        conn.set_token(self._dest_client.org_config.api_key)
        conn.make_long_lived()

        logger = _HTTPBackgroundLogger(LazyValue(lambda: conn, use_mutex=True))
        logger.sync_flush = True
        self._background_logger = logger
        self._lazy_value_cls = LazyValue

    def _prepare_row(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            **dict(row),
            **self._object_id_fields,
        }

    def _max_request_bytes(self) -> int:
        stream_config = StreamingConfig.resolve(self._dest_client, self._dest_client)
        migration_config = getattr(self._dest_client, "migration_config", None)
        insert_max_request_bytes = int(
            getattr(migration_config, "insert_max_request_bytes", 6 * 1024 * 1024)
        )
        insert_request_headroom_ratio = float(
            getattr(migration_config, "insert_request_headroom_ratio", 0.75)
        )
        return min(
            stream_config.max_event_bytes,
            int(insert_max_request_bytes * insert_request_headroom_ratio),
        )

    def write_rows_sync(self, rows: Sequence[dict[str, Any]]) -> None:
        self._ensure_logger()
        assert self._background_logger is not None
        assert self._lazy_value_cls is not None

        prepared_rows = [self._prepare_row(row) for row in rows]
        for batch in iter_ordered_batches_by_count_and_bytes(
            prepared_rows,
            max_items=max(1, len(prepared_rows)),
            max_bytes=self._max_request_bytes(),
            exact_wrapper_bytes=True,
        ):
            events = [
                self._lazy_value_cls(
                    lambda prepared=prepared: prepared,
                    use_mutex=False,
                )
                for prepared in batch
            ]
            self._background_logger.log(*events)
            self._background_logger.flush()

    async def write_rows(self, rows: Sequence[dict[str, Any]]) -> None:
        await asyncio.to_thread(self.write_rows_sync, rows)


class SDKProjectLogsWriter(SDKRowWriter):
    def __init__(self, dest_client: BraintrustClient, project_id: str) -> None:
        super().__init__(
            dest_client,
            {
                "project_id": project_id,
                "log_id": PROJECT_LOGS_LOG_ID,
            },
        )


class SDKExperimentWriter(SDKRowWriter):
    def __init__(self, dest_client: BraintrustClient, experiment_id: str) -> None:
        super().__init__(dest_client, {"experiment_id": experiment_id})


class SDKDatasetWriter(SDKRowWriter):
    def __init__(self, dest_client: BraintrustClient, dataset_id: str) -> None:
        super().__init__(dest_client, {"dataset_id": dataset_id})
