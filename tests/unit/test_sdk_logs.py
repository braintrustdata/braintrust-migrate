from __future__ import annotations

import sys
from types import ModuleType, SimpleNamespace
from typing import Any

from braintrust_migrate.sdk_logs import (
    PROJECT_LOGS_LOG_ID,
    SDKDatasetWriter,
    SDKExperimentWriter,
    SDKProjectLogsWriter,
)

EXPECTED_SPLIT_FLUSHES = 2


class _FakeLazyValue:
    def __init__(self, fn, use_mutex: bool = False) -> None:
        _ = use_mutex
        self._fn = fn

    def get(self) -> Any:
        return self._fn()


class _FakeHTTPConnection:
    def __init__(self, base_url: str) -> None:
        self.base_url = base_url
        self.token: str | None = None
        self.long_lived = False

    def set_token(self, token: str) -> None:
        self.token = token

    def make_long_lived(self) -> None:
        self.long_lived = True


class _FakeBackgroundLogger:
    def __init__(self, api_conn: _FakeLazyValue) -> None:
        self.api_conn = api_conn
        self.sync_flush = False
        self.logged_rows: list[dict[str, Any]] = []
        self.log_calls: list[list[dict[str, Any]]] = []
        self.flush_count = 0

    def log(self, *args: _FakeLazyValue) -> None:
        rows = [arg.get() for arg in args]
        self.log_calls.append(rows)
        self.logged_rows.extend(rows)

    def flush(self) -> None:
        self.flush_count += 1


def test_sdk_project_logs_writer_adds_logs3_object_ids(monkeypatch) -> None:
    fake_logger_module = ModuleType("braintrust.logger")
    fake_logger_module.HTTPConnection = _FakeHTTPConnection
    fake_logger_module._HTTPBackgroundLogger = _FakeBackgroundLogger
    fake_util_module = ModuleType("braintrust.util")
    fake_util_module.LazyValue = _FakeLazyValue

    monkeypatch.setitem(sys.modules, "braintrust.logger", fake_logger_module)
    monkeypatch.setitem(sys.modules, "braintrust.util", fake_util_module)

    dest_client = SimpleNamespace(
        org_config=SimpleNamespace(
            url="https://api.example.com",
            api_key="secret-token",
        )
    )

    writer = SDKProjectLogsWriter(dest_client, "dest-project-id")
    writer.write_rows_sync([{"id": "row1", "input": "hello"}])

    logger = writer._background_logger
    assert logger is not None
    assert logger.flush_count == 1
    assert logger.logged_rows == [
        {
            "id": "row1",
            "input": "hello",
            "project_id": "dest-project-id",
            "log_id": PROJECT_LOGS_LOG_ID,
        }
    ]
    conn = logger.api_conn.get()
    assert conn.base_url == "https://api.example.com"
    assert conn.token == "secret-token"
    assert conn.long_lived is True


def test_sdk_project_logs_writer_splits_aggregate_payloads(monkeypatch) -> None:
    fake_logger_module = ModuleType("braintrust.logger")
    fake_logger_module.HTTPConnection = _FakeHTTPConnection
    fake_logger_module._HTTPBackgroundLogger = _FakeBackgroundLogger
    fake_util_module = ModuleType("braintrust.util")
    fake_util_module.LazyValue = _FakeLazyValue

    monkeypatch.setitem(sys.modules, "braintrust.logger", fake_logger_module)
    monkeypatch.setitem(sys.modules, "braintrust.util", fake_util_module)

    dest_client = SimpleNamespace(
        org_config=SimpleNamespace(
            url="https://api.example.com",
            api_key="secret-token",
        ),
        migration_config=SimpleNamespace(
            insert_max_request_bytes=1_000,
            insert_request_headroom_ratio=1.0,
        ),
    )

    writer = SDKProjectLogsWriter(dest_client, "dest-project-id")
    writer.write_rows_sync(
        [
            {"id": "row1", "input": "x" * 700},
            {"id": "row2", "input": "y" * 700},
        ]
    )

    logger = writer._background_logger
    assert logger is not None
    assert logger.flush_count == EXPECTED_SPLIT_FLUSHES
    assert [[row["id"] for row in call] for call in logger.log_calls] == [
        ["row1"],
        ["row2"],
    ]


def test_sdk_experiment_writer_adds_experiment_id(monkeypatch) -> None:
    fake_logger_module = ModuleType("braintrust.logger")
    fake_logger_module.HTTPConnection = _FakeHTTPConnection
    fake_logger_module._HTTPBackgroundLogger = _FakeBackgroundLogger
    fake_util_module = ModuleType("braintrust.util")
    fake_util_module.LazyValue = _FakeLazyValue

    monkeypatch.setitem(sys.modules, "braintrust.logger", fake_logger_module)
    monkeypatch.setitem(sys.modules, "braintrust.util", fake_util_module)

    dest_client = SimpleNamespace(
        org_config=SimpleNamespace(
            url="https://api.example.com",
            api_key="secret-token",
        )
    )

    writer = SDKExperimentWriter(dest_client, "dest-experiment-id")
    writer.write_rows_sync([{"id": "row1", "input": "hello"}])

    logger = writer._background_logger
    assert logger is not None
    assert logger.logged_rows == [
        {
            "id": "row1",
            "input": "hello",
            "experiment_id": "dest-experiment-id",
        }
    ]


def test_sdk_dataset_writer_adds_dataset_id(monkeypatch) -> None:
    fake_logger_module = ModuleType("braintrust.logger")
    fake_logger_module.HTTPConnection = _FakeHTTPConnection
    fake_logger_module._HTTPBackgroundLogger = _FakeBackgroundLogger
    fake_util_module = ModuleType("braintrust.util")
    fake_util_module.LazyValue = _FakeLazyValue

    monkeypatch.setitem(sys.modules, "braintrust.logger", fake_logger_module)
    monkeypatch.setitem(sys.modules, "braintrust.util", fake_util_module)

    dest_client = SimpleNamespace(
        org_config=SimpleNamespace(
            url="https://api.example.com",
            api_key="secret-token",
        )
    )

    writer = SDKDatasetWriter(dest_client, "dest-dataset-id")
    writer.write_rows_sync([{"id": "row1", "input": "hello"}])

    logger = writer._background_logger
    assert logger is not None
    assert logger.logged_rows == [
        {
            "id": "row1",
            "input": "hello",
            "dataset_id": "dest-dataset-id",
        }
    ]
