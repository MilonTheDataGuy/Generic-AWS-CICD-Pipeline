from unittest.mock import MagicMock, patch
import pytest
from structlog import get_logger
from structlog.testing import capture_logs
from scripts.Snowflake_Incremental_MRG_Load import Table

logger = get_logger()


@pytest.fixture
def table() -> Table:
    return Table(
        batch_id="my_batch_id",
        name="my_table_name",
        raw_table_flag=True,
        raw_schema_name="my_raw_schema_name",
        stg_table_flag=True,
        stg_schema_name="my_stg_schema_name",
        mrg_table_flag=True,
        mrg_schema_name="my_mrg_schema_name",
    )


class TestTable:
    def test_populate_collection(self):
        cursor = MagicMock()
        batch_id = "1234"
        cursor.execute.return_value.fetchall.return_value = [
            (
                batch_id,
                "table_name_a",
                True,
                "raw_schema",
                True,
                "stg_schema",
                True,
                "mrg_schema",
            ),
            (
                batch_id,
                "table_name_b",
                True,
                "raw_schema",
                True,
                "stg_schema",
                True,
                "mrg_schema",
            ),
        ]
        with capture_logs() as cap_logs:
            Table.populate_collection(
                batch_id=batch_id, cursor=cursor, logger=logger, core_centers=""
            )

        assert any(
            log["event"] == "fetching tables" and "query" in log.keys()
            for log in cap_logs
        )
        assert cursor.execute.call_count == 1
        assert len(Table.collection) == 2

    @pytest.mark.parametrize(
        "load_fn,procedure_name",
        [
            ("raw_load", "SP_INCREMENTAL_RAW_TABLE"),
            ("stg_load", "SP_INCREMENTAL_STG_TABLE"),
            ("mrg_load", "SP_INCREMENTAL_MRG_TABLE"),
        ],
    )
    def test_load_fn(self, table, load_fn: str, procedure_name: str):
        cursor = MagicMock()
        with capture_logs() as cap_logs:
            getattr(table, load_fn)(cursor=cursor, logger=logger)
        assert any(
            log["event"] == f"Running" and "query" in log.keys() for log in cap_logs
        )
        assert any(log["event"] == f"Successful {procedure_name}" for log in cap_logs)
        assert cursor.execute.call_count == 1
        assert procedure_name in cursor.execute.call_args[0][0]

    @pytest.mark.parametrize(
        "raw_table_flag, raw_load_call_count, stg_table_flag, stg_load_call_count, mrg_table_flag, mrg_load_call_count",
        [
            (False, 0, False, 0, False, 0),
            (False, 0, False, 0, True, 1),
            (False, 0, True, 1, True, 1),
            (False, 0, True, 1, False, 0),
            (True, 1, False, 0, False, 0),
            (True, 1, False, 0, True, 1),
            (True, 1, True, 1, True, 1),
            (True, 1, True, 1, False, 0),
        ],
    )
    def test_load_to_mrg_happy_path(
        self,
        raw_table_flag: bool,
        raw_load_call_count: int,
        stg_table_flag: bool,
        stg_load_call_count: int,
        mrg_table_flag: bool,
        mrg_load_call_count: int,
    ):
        tbl = Table(
            batch_id="my_batch_id",
            name="my_table_name",
            raw_table_flag=raw_table_flag,
            raw_schema_name="my_raw_schema_name",
            stg_table_flag=stg_table_flag,
            stg_schema_name="my_stg_schema_name",
            mrg_table_flag=mrg_table_flag,
            mrg_schema_name="my_mrg_schema_name",
        )
        connection = MagicMock()
        with patch.object(tbl, "raw_load") as mock_raw_load:
            with patch.object(tbl, "stg_load") as mock_stg_load:
                with patch.object(tbl, "mrg_load") as mock_mrg_load:
                    with capture_logs() as cap_logs:
                        tbl.load_to_mrg(connection=connection, logger=logger)

        assert all(
            "table_name" in log.keys() and log["table_name"] == "my_table_name"
            for log in cap_logs
        )
        assert mock_raw_load.call_count == raw_load_call_count
        assert mock_stg_load.call_count == stg_load_call_count
        assert mock_mrg_load.call_count == mrg_load_call_count

    def test_run_concurrent_processes_happy_path(self):
        schema_dict: dict[str, str | bool] = dict(
            batch_id="my_batch_id",
            raw_table_flag=True,
            raw_schema_name="my_raw_schema_name",
            stg_table_flag=True,
            stg_schema_name="my_stg_schema_name",
            mrg_table_flag=True,
            mrg_schema_name="my_mrg_schema_name",
        )
        Table.collection = [
            Table(name="my_table_name_a", **schema_dict),
            Table(name="my_table_name_b", **schema_dict),
        ]

        connection = MagicMock()
        connection.cursor().execute.side_effect = [
            "good!",
            "good!",
            "good!",
            "good!",
            "good!",
            Exception("whoa there"),
        ]

        Table.run_concurrent_processes(connection=connection, logger=logger)
