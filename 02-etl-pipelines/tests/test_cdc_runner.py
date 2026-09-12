from types import SimpleNamespace
from unittest.mock import Mock

import pytest

import cdc_load
from cdc_load import run_cdc_batch

class FakeStream:
    def __init__(
        self,
        log_file,
        log_pos,
    ):
        self.log_file = log_file
        self.log_pos = log_pos
        self.closed = False

    def close(self):
        self.closed = True


def configure_runner_dependencies(
    monkeypatch,
    checkpoint,
    transactions,
):
    bootstrap_coordinate = (
        "binlog.000030",
        157,
    )
    calls = {
        "current_coordinate": 0,
        "initialize_checkpoint": [],
        "start_batch": [],
        "create_stream": [],
        "persist": [],
        "complete_batch": [],
        "complete_etl": [],
        "fail_etl": [],
    }

    if transactions:
        final_transaction = transactions[-1]

        stream_coordinate = (
            final_transaction["binlog_file"],
            final_transaction[
                "commit_position"
            ],
        )

    elif checkpoint is not None:
        stream_coordinate = checkpoint

    else:
        stream_coordinate = (
            bootstrap_coordinate
        )


    stream = FakeStream(
        log_file=stream_coordinate[0],
        log_pos=stream_coordinate[1],
    )

    monkeypatch.setattr(
        cdc_load,
        "start_etl_run",
        lambda *args, **kwargs: 101,
    )

    monkeypatch.setattr(
        cdc_load,
        "get_mysql_cdc_settings",
        lambda: {
            "host": "mysql",
            "port": 3306,
            "user": "cdc_reader",
            "password": "test",
        },
    )

    monkeypatch.setattr(
        cdc_load,
        "get_cdc_checkpoint",
        lambda *args, **kwargs: checkpoint,
    )

    monkeypatch.setattr(
        cdc_load,
        "update_cdc_checkpoint",
        lambda *args, **kwargs: None,
    )

    def get_current_coordinate(settings):
        calls["current_coordinate"] += 1

        return (
            "binlog.000030",
            157,
        )

    def get_current_coordinate(
        *args,
        **kwargs,
    ):
        calls["current_coordinate"] += 1

        return bootstrap_coordinate


    monkeypatch.setattr(
        cdc_load,
        "get_current_binlog_coordinate",
        get_current_coordinate,
    )

    def initialize_checkpoint(
        pipeline_name,
        checkpoint_name,
        binlog_file,
        binlog_position,
    ):
        calls["initialize_checkpoint"].append(
            (
                pipeline_name,
                checkpoint_name,
                binlog_file,
                binlog_position,
            )
        )

        return (
            binlog_file,
            binlog_position,
        )

    monkeypatch.setattr(
        cdc_load,
        "initialize_cdc_checkpoint",
        initialize_checkpoint,
    )

    def start_batch(**kwargs):
        calls["start_batch"].append(
            kwargs
        )

        return 202

    monkeypatch.setattr(
        cdc_load,
        "start_cdc_batch",
        start_batch,
    )

    monkeypatch.setattr(
        cdc_load,
        "build_primary_key_columns_by_table",
        lambda source_schema: {
            (
                source_schema,
                "categories",
            ): ("category_id",),
            (
                source_schema,
                "countries",
            ): ("country_id",),
            (
                source_schema,
                "orders",
            ): ("order_id",),
        },
    )

    def create_stream(**kwargs):
        calls["create_stream"].append(
            kwargs
        )

        return stream

    monkeypatch.setattr(
        cdc_load,
        "create_cdc_stream",
        create_stream,
    )

    monkeypatch.setattr(
        cdc_load,
        "iter_committed_transactions",
        lambda stream, metadata: iter(
            transactions
        ),
    )

    def persist_transaction(**kwargs):
        calls["persist"].append(
            kwargs
        )

        return len(
            kwargs["transaction"][
                "change_events"
            ]
        )

    monkeypatch.setattr(
        cdc_load,
        "persist_cdc_transaction",
        persist_transaction,
    )

    monkeypatch.setattr(
        cdc_load,
        "complete_cdc_batch",
        lambda **kwargs: calls[
            "complete_batch"
        ].append(kwargs),
    )

    def complete_etl(*args, **kwargs):
        calls["complete_etl"].append(
            (
                args,
                kwargs,
            )
        )

    monkeypatch.setattr(
        cdc_load,
        "complete_etl_run",
        complete_etl,
    )

    def fail_etl(*args, **kwargs):
        calls["fail_etl"].append(
            (
                args,
                kwargs,
            )
        )

    monkeypatch.setattr(
        cdc_load,
        "fail_etl_run",
        fail_etl,
    )

    return calls, stream

def test_run_cdc_batch_with_no_transactions(
    monkeypatch,
):
    calls, stream = (
        configure_runner_dependencies(
            monkeypatch,
            checkpoint=(
                "binlog.000029",
                8697,
            ),
            transactions=[],
        )
    )

    result = run_cdc_batch()

    assert (
        result["transactions_processed"]
        == 0
    )
    assert result["events_processed"] == 0
    assert result["events_inserted"] == 0

    assert (
        result["end_binlog_file"]
        == "binlog.000029"
    )
    assert (
        result["end_binlog_position"]
        == 8697
    )

    assert calls["current_coordinate"] == 0
    assert calls[
        "initialize_checkpoint"
    ] == []

    assert len(calls["persist"]) == 0

    assert len(
        calls["complete_batch"]
    ) == 1

    completed_batch = calls[
        "complete_batch"
    ][0]

    assert (
        completed_batch[
            "start_binlog_file"
        ]
        if "start_binlog_file"
        in completed_batch
        else "binlog.000029"
    ) == "binlog.000029"

    assert (
        completed_batch[
            "end_binlog_file"
        ]
        == "binlog.000029"
    )
    assert (
        completed_batch[
            "end_binlog_position"
        ]
        == 8697
    )

    assert len(
        calls["complete_etl"]
    ) == 1
    assert calls["fail_etl"] == []

    assert stream.closed is True

    assert calls["start_batch"][0][
        "start_binlog_file"
    ] == "binlog.000029"

    assert calls["start_batch"][0][
        "start_binlog_position"
    ] == 8697

def test_advances_checkpoint_to_safe_read_cursor_after_empty_scan(
    monkeypatch,
):
    stream = SimpleNamespace(
        log_file="binlog.000031",
        log_pos=474,
        close=Mock(),
    )

    checkpoint_updates = []
    completed_batches = []

    monkeypatch.setattr(
        cdc_load,
        "start_etl_run",
        lambda *args, **kwargs: 1001,
    )
    monkeypatch.setattr(
        cdc_load,
        "get_mysql_cdc_settings",
        lambda: {},
    )
    monkeypatch.setattr(
        cdc_load,
        "get_cdc_checkpoint",
        lambda *args, **kwargs: (
            "binlog.000030",
            2511,
        ),
    )
    monkeypatch.setattr(
        cdc_load,
        "start_cdc_batch",
        lambda *args, **kwargs: 2001,
    )
    monkeypatch.setattr(
        cdc_load,
        "build_primary_key_columns_by_table",
        lambda *args, **kwargs: {},
    )
    monkeypatch.setattr(
        cdc_load,
        "create_cdc_stream",
        lambda *args, **kwargs: stream,
    )
    monkeypatch.setattr(
        cdc_load,
        "iter_committed_transactions",
        lambda *args, **kwargs: iter(()),
    )

    def fake_update_cdc_checkpoint(
        pipeline_name,
        checkpoint_name,
        binlog_file,
        binlog_position,
    ):
        checkpoint_updates.append(
            (
                pipeline_name,
                checkpoint_name,
                binlog_file,
                binlog_position,
            )
        )

    monkeypatch.setattr(
        cdc_load,
        "update_cdc_checkpoint",
        fake_update_cdc_checkpoint,
    )

    def fake_complete_cdc_batch(
        *args,
        **kwargs,
    ):
        completed_batches.append(
            (args, kwargs)
        )

    monkeypatch.setattr(
        cdc_load,
        "complete_cdc_batch",
        fake_complete_cdc_batch,
    )
    monkeypatch.setattr(
        cdc_load,
        "complete_etl_run",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        cdc_load,
        "fail_etl_run",
        lambda *args, **kwargs: None,
    )

    result = cdc_load.run_cdc_batch()

    assert checkpoint_updates == [
        (
            cdc_load.PIPELINE_NAME,
            cdc_load.CHECKPOINT_NAME,
            "binlog.000031",
            474,
        )
    ]

    assert result[
        "transactions_processed"
    ] == 0

    assert result[
        "events_processed"
    ] == 0

    assert result[
        "events_inserted"
    ] == 0

    assert result[
        "end_binlog_file"
    ] == "binlog.000031"

    assert result[
        "end_binlog_position"
    ] == 474

    assert len(completed_batches) == 1

    stream.close.assert_called_once()


def test_run_cdc_batch_bootstraps_checkpoint(
    monkeypatch,
):
    calls, stream = (
        configure_runner_dependencies(
            monkeypatch,
            checkpoint=None,
            transactions=[],
        )
    )

    result = run_cdc_batch()

    assert calls["current_coordinate"] == 1

    assert calls[
        "initialize_checkpoint"
    ] == [
        (
            cdc_load.PIPELINE_NAME,
            cdc_load.CHECKPOINT_NAME,
            "binlog.000030",
            157,
        )
    ]

    assert calls["start_batch"][0][
        "start_binlog_file"
    ] == "binlog.000030"

    assert calls["start_batch"][0][
        "start_binlog_position"
    ] == 157

    assert (
        result["end_binlog_file"]
        == "binlog.000030"
    )
    assert (
        result["end_binlog_position"]
        == 157
    )

    assert stream.closed is True

def test_advances_from_last_transaction_to_later_safe_cursor(
    monkeypatch,
):
    stream = SimpleNamespace(
        log_file="binlog.000031",
        log_pos=900,
        close=Mock(),
    )

    transaction = {
        "transaction_id": 5001,
        "binlog_file": "binlog.000031",
        "commit_position": 700,
        "change_events": [
            {
                "operation": "INSERT",
            }
        ],
    }

    persisted_transactions = []
    checkpoint_updates = []

    monkeypatch.setattr(
        cdc_load,
        "start_etl_run",
        lambda *args, **kwargs: 1002,
    )
    monkeypatch.setattr(
        cdc_load,
        "get_mysql_cdc_settings",
        lambda: {},
    )
    monkeypatch.setattr(
        cdc_load,
        "get_cdc_checkpoint",
        lambda *args, **kwargs: (
            "binlog.000031",
            500,
        ),
    )
    monkeypatch.setattr(
        cdc_load,
        "start_cdc_batch",
        lambda *args, **kwargs: 2002,
    )
    monkeypatch.setattr(
        cdc_load,
        "build_primary_key_columns_by_table",
        lambda *args, **kwargs: {},
    )
    monkeypatch.setattr(
        cdc_load,
        "create_cdc_stream",
        lambda *args, **kwargs: stream,
    )
    monkeypatch.setattr(
        cdc_load,
        "iter_committed_transactions",
        lambda *args, **kwargs: iter(
            [transaction]
        ),
    )

    def fake_persist_cdc_transaction(
        *args,
        **kwargs,
    ):
        persisted_transactions.append(
            (args, kwargs)
        )
        return 1

    monkeypatch.setattr(
        cdc_load,
        "persist_cdc_transaction",
        fake_persist_cdc_transaction,
    )

    def fake_update_cdc_checkpoint(
        pipeline_name,
        checkpoint_name,
        binlog_file,
        binlog_position,
    ):
        checkpoint_updates.append(
            (
                pipeline_name,
                checkpoint_name,
                binlog_file,
                binlog_position,
            )
        )

    monkeypatch.setattr(
        cdc_load,
        "update_cdc_checkpoint",
        fake_update_cdc_checkpoint,
    )
    monkeypatch.setattr(
        cdc_load,
        "complete_cdc_batch",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        cdc_load,
        "complete_etl_run",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        cdc_load,
        "fail_etl_run",
        lambda *args, **kwargs: None,
    )

    result = cdc_load.run_cdc_batch()

    assert len(
        persisted_transactions
    ) == 1

    assert checkpoint_updates == [
        (
            cdc_load.PIPELINE_NAME,
            cdc_load.CHECKPOINT_NAME,
            "binlog.000031",
            900,
        )
    ]

    assert result[
        "transactions_processed"
    ] == 1

    assert result[
        "events_processed"
    ] == 1

    assert result[
        "events_inserted"
    ] == 1

    assert result[
        "insert_events"
    ] == 1

    assert result[
        "update_events"
    ] == 0

    assert result[
        "delete_events"
    ] == 0

    assert result[
        "end_binlog_file"
    ] == "binlog.000031"

    assert result[
        "end_binlog_position"
    ] == 900

    stream.close.assert_called_once()

def test_does_not_advance_safe_cursor_when_stream_fails(
    monkeypatch,
):
    stream = SimpleNamespace(
        log_file="binlog.000031",
        log_pos=900,
        close=Mock(),
    )

    checkpoint_updates = []
    failed_runs = []

    monkeypatch.setattr(
        cdc_load,
        "start_etl_run",
        lambda *args, **kwargs: 1003,
    )
    monkeypatch.setattr(
        cdc_load,
        "get_mysql_cdc_settings",
        lambda: {},
    )
    monkeypatch.setattr(
        cdc_load,
        "get_cdc_checkpoint",
        lambda *args, **kwargs: (
            "binlog.000030",
            2511,
        ),
    )
    monkeypatch.setattr(
        cdc_load,
        "start_cdc_batch",
        lambda *args, **kwargs: 2003,
    )
    monkeypatch.setattr(
        cdc_load,
        "build_primary_key_columns_by_table",
        lambda *args, **kwargs: {},
    )
    monkeypatch.setattr(
        cdc_load,
        "create_cdc_stream",
        lambda *args, **kwargs: stream,
    )

    def failing_transactions(
        *args,
        **kwargs,
    ):
        raise RuntimeError(
            "simulated CDC stream failure"
        )
        yield

    monkeypatch.setattr(
        cdc_load,
        "iter_committed_transactions",
        failing_transactions,
    )

    def fake_update_cdc_checkpoint(
        *args,
        **kwargs,
    ):
        checkpoint_updates.append(
            (args, kwargs)
        )

    monkeypatch.setattr(
        cdc_load,
        "update_cdc_checkpoint",
        fake_update_cdc_checkpoint,
    )

    def fake_fail_etl_run(
        *args,
        **kwargs,
    ):
        failed_runs.append(
            (args, kwargs)
        )

    monkeypatch.setattr(
        cdc_load,
        "fail_etl_run",
        fake_fail_etl_run,
    )
    monkeypatch.setattr(
        cdc_load,
        "complete_cdc_batch",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        cdc_load,
        "complete_etl_run",
        lambda *args, **kwargs: None,
    )

    with pytest.raises(
        RuntimeError,
        match="simulated CDC stream failure",
    ):
        cdc_load.run_cdc_batch()

    assert checkpoint_updates == []

    assert len(
        failed_runs
    ) == 1

    stream.close.assert_called_once()


def test_run_cdc_batch_processes_transactions_across_binlog_rotation(
    monkeypatch,
):
    transactions = [
        {
            "transaction_id": 5001,
            "binlog_file": "binlog.000029",
            "commit_position": 9200,
            "change_events": [
                {
                    "operation": "INSERT",
                },
                {
                    "operation": "UPDATE",
                },
            ],
        },
        {
            "transaction_id": 5002,
            "binlog_file": "binlog.000030",
            "commit_position": 157,
            "change_events": [
                {
                    "operation": "DELETE",
                },
            ],
        },
    ]

    calls, stream = (
        configure_runner_dependencies(
            monkeypatch,
            checkpoint=(
                "binlog.000029",
                8697,
            ),
            transactions=transactions,
        )
    )

    result = run_cdc_batch()

    assert (
        result["transactions_processed"]
        == 2
    )

    assert result["events_processed"] == 3
    assert result["events_inserted"] == 3

    assert result["insert_events"] == 1
    assert result["update_events"] == 1
    assert result["delete_events"] == 1

    assert (
        result["end_binlog_file"]
        == "binlog.000030"
    )
    assert (
        result["end_binlog_position"]
        == 157
    )

    assert len(calls["persist"]) == 2

    completed_batch = calls[
        "complete_batch"
    ][0]

    assert (
        completed_batch[
            "transactions_processed"
        ]
        == 2
    )
    assert (
        completed_batch[
            "events_processed"
        ]
        == 3
    )

    assert (
        completed_batch[
            "end_binlog_file"
        ]
        == "binlog.000030"
    )
    assert (
        completed_batch[
            "end_binlog_position"
        ]
        == 157
    )

    assert stream.closed is True

def test_run_cdc_batch_fails_on_transaction_error(
    monkeypatch,
):
    transactions = [
        {
            "transaction_id": 6001,
            "binlog_file": "binlog.000029",
            "commit_position": 9200,
            "change_events": [
                {
                    "operation": "INSERT",
                },
            ],
        },
        {
            "transaction_id": 6002,
            "binlog_file": "binlog.000029",
            "commit_position": 9500,
            "change_events": [
                {
                    "operation": "UPDATE",
                },
            ],
        },
    ]

    calls, stream = (
        configure_runner_dependencies(
            monkeypatch,
            checkpoint=(
                "binlog.000029",
                8697,
            ),
            transactions=transactions,
        )
    )

    persisted_transaction_ids = []

    def persist_with_failure(**kwargs):
        transaction = kwargs[
            "transaction"
        ]

        persisted_transaction_ids.append(
            transaction["transaction_id"]
        )

        if (
            transaction["transaction_id"]
            == 6002
        ):
            raise RuntimeError(
                "Forced transaction failure"
            )

        return len(
            transaction["change_events"]
        )

    monkeypatch.setattr(
        cdc_load,
        "persist_cdc_transaction",
        persist_with_failure,
    )

    with pytest.raises(
        RuntimeError,
        match="Forced transaction failure",
    ):
        run_cdc_batch()

    assert persisted_transaction_ids == [
        6001,
        6002,
    ]

    assert calls["complete_batch"] == []
    assert calls["complete_etl"] == []

    assert len(calls["fail_etl"]) == 1

    fail_args, fail_kwargs = (
        calls["fail_etl"][0]
    )

    assert fail_args[0] == 101
    assert (
        fail_args[1]
        == "Forced transaction failure"
    )
    assert fail_kwargs == {}

    assert stream.closed is True