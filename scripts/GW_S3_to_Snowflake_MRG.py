from __future__ import annotations

import dataclasses
import json
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from textwrap import dedent
from time import sleep
from typing import ClassVar, Any, Generator

from boto3.session import Session
from botocore.client import BaseClient
from botocore.config import Config
from botocore.exceptions import ClientError
from botocore.paginate import Paginator
from snowflake.connector import SnowflakeConnection, connect
from snowflake.connector.cursor import SnowflakeCursor

try:
    IS_GLUE_JOB = True
    # noinspection PyUnresolvedReferences
    from awsglue.transforms import *

    # noinspection PyUnresolvedReferences
    from awsglue.utils import getResolvedOptions

    # noinspection PyUnresolvedReferences
    from pyspark.context import SparkContext

    # noinspection PyUnresolvedReferences
    from awsglue.context import GlueContext

    # noinspection PyUnresolvedReferences
    from awsglue.job import Job

    # noinspection PyUnresolvedReferences
    from awsglue import DynamicFrame
except ModuleNotFoundError as e:
    IS_GLUE_JOB = False

logging.basicConfig(
    format="%(asctime)s : %(filename)s : %(levelname)s : %(message)s",
    datefmt="%Y-%m-%d %I:%M:%S %p",
)

for noisy_logger in [
    "snowflake",
    "boto3",
    "botocore",
    "urllib3",
    "concurrent.futures",
]:
    logging.getLogger(noisy_logger).setLevel(logging.WARNING)


def get_snowflake_connection(
    secretsmanager_client, secret_name: str, database_name: str
) -> SnowflakeConnection:
    get_secret_value_response = secretsmanager_client.get_secret_value(
        SecretId=secret_name
    )
    secrets_value = json.loads(get_secret_value_response["SecretString"])

    return connect(
        user=secrets_value["SNOWFLAKE_USER"],
        password=secrets_value["SNOWFLAKE_PASSWORD"],
        account=secrets_value["SNOWFLAKE_ACCOUNT"],
        warehouse=secrets_value["SNOWFLAKE_WAREHOUSE"],
        database=database_name,
        schema="ETL_CTRL",
        role=secrets_value["SNOWFLAKE_ROLE"],
    )


@dataclasses.dataclass
class SNSClient:
    _client: BaseClient
    _topic_arn: str
    _logger: logging.Logger

    @staticmethod
    def get_topic_arn(
        client: BaseClient,
        topic_name: str,
        logger: logging.Logger,
    ):
        response = client.list_topics()
        topics = response["Topics"]
        topic_arns = [
            topic["TopicArn"]
            for topic in topics
            if topic["TopicArn"].split(":")[-1] == topic_name
        ]

        if len(topic_arns) == 0:
            exception = f"SNS Topic Not Found: {topic_name}"
            logger.error(exception)
            raise ValueError(exception)

        if len(topic_arns) > 1:
            exception = f"Multiple SNS Topic Matches Found: {topic_name}\n{', '.join(topic_arns)}"
            logger.error(exception)
            raise ValueError(exception)

        return topic_arns[0]

    @classmethod
    def factory(
        cls,
        session: Session,
        topic_name: str,
        logger,
    ) -> SNSClient:
        client = session.client("sns")
        topic_arn = cls.get_topic_arn(
            client=client, topic_name=topic_name, logger=logger
        )
        logger.info(f"returning SNSClient for topic_arn: {topic_arn}")
        return cls(_client=client, _topic_arn=topic_arn, _logger=logger)

    def send_notification(self, subject: str, message: str) -> None:
        self._logger.info(f"publishing SNS message:\n{subject}\n\n{message}")
        if not IS_GLUE_JOB:
            return
        self._client.publish(
            TopicArn=self._topic_arn,
            Message=dedent(str(message)),
            Subject=str(subject),
        )


class LoadException(Exception):
    table: Layer
    procedure: str
    cause: Exception | None

    def __init__(self, message, table: Layer, procedure: str, cause: Exception | None):
        self.message = message
        self.table = table
        self.procedure = procedure
        self.cause = cause
        super().__init__(message, *args)


def insert_into_jc_process_log(
    cursor: SnowflakeCursor,
    row_count: int,
    schema_name: str,
    table_name: str,
    batch_id: str,
    process_name: str,
):
    cursor.execute(
        dedent(
            f"""
                INSERT INTO ETL_CTRL.JC_{schema_name}_PROCESS_LOG (
                    BATCHID,
                    TABLE_NAME,
                    JOB_NAME,
                    PROCESS_NAME,
                    ROW_COUNT
                )
                VALUES (
                    {batch_id},
                    '{table_name}',
                    'GW_S3_to_Snowflake_MRG',
                    $${process_name}$$,
                    {row_count}
                );
            """
        )
    ).fetchone()


def set_jc_load_status_post_s3_load(
    schema_name: str,
    table_name: str,
    batch_id: str,
    cursor: SnowflakeCursor,
    logger: logging.Logger,
):
    logger.info(
        f"{schema_name}.{table_name}: merging into ETL_CTRL.JC_{schema_name}_LOAD_STATUS"
    )
    (rows_inserted, rows_updated) = cursor.execute(
        dedent(
            f"""
                MERGE INTO ETL_CTRL.JC_{schema_name}_LOAD_STATUS AS TGT
                USING (
                    SELECT *
                    FROM (
                        VALUES (
                            '{table_name}'
                        )
                    )
                        AS SUB (TABLE_NAME)
                ) AS SRC
                    ON LOWER(TGT.TABLE_NAME) = LOWER(SRC.TABLE_NAME)
                WHEN MATCHED AND LOWER(TGT.STATUS) = 'completed' THEN
                    UPDATE SET
                        TGT.BATCH_ID = {batch_id},
                        TGT.STATUS = 'pending',
                        TGT.ROW_UPDATE_TMS = CURRENT_TIMESTAMP,
                        TGT.ROW_INSERT_TMS = CURRENT_TIMESTAMP
                WHEN NOT MATCHED THEN INSERT (
                    TABLE_NAME,
                    STATUS
                )
                VALUES (
                    SRC.TABLE_NAME,
                    'initial'
                );
            """
        )
    ).fetchone()
    insert_into_jc_process_log(
        cursor=cursor,
        row_count=rows_inserted + rows_updated,
        schema_name=schema_name,
        table_name=table_name,
        batch_id=batch_id,
        process_name=f"merge into load status",
    )


def set_jc_manifest_post_s3_load(
    core_center: str,
    schema_name: str,
    table_name: str,
    cda_schema_id: str,
    cda_folder: int,
    cda_folder_tms: datetime,
    total_records_count: int,
    batch_id: str,
    cursor: SnowflakeCursor,
    logger: logging.Logger,
):
    logger.info(
        f"{schema_name}.{table_name}: merging into ETL_CTRL.JC_{core_center.upper()}_MANIFEST_DETAILS"
    )

    (rows_inserted, rows_updated) = cursor.execute(
        dedent(
            f"""
                MERGE INTO ETL_CTRL.JC_{core_center.upper()}_MANIFEST_DETAILS AS TGT
                    USING (
                        SELECT *
                        FROM (
                            VALUES (
                                '{table_name}',
                                '{cda_schema_id}',
                                '{cda_folder}',
                                '{cda_folder_tms.strftime('%Y-%m-%d %H:%M:%S.%f')}'::TIMESTAMP_NTZ,
                                {total_records_count}
                            )
                        )
                            AS SUB (
                                TABLE_NAME, 
                                CDA_SCHEMA_ID, 
                                CDA_FOLDER, 
                                CDA_FOLDER_TMS, 
                                TOTAL_RECORDS_COUNT
                            )
                    ) AS SRC
                    ON TGT.TABLE_NAME = SRC.TABLE_NAME
                    WHEN MATCHED AND TGT.CDA_FOLDER_TMS < SRC.CDA_FOLDER_TMS THEN
                        UPDATE SET
                            TGT.CDA_SCHEMA_ID = SRC.CDA_SCHEMA_ID,
                            TGT.CDA_FOLDER = SRC.CDA_FOLDER ,
                            TGT.CDA_FOLDER_TMS = SRC.CDA_FOLDER_TMS ,
                            TGT.TOTAL_RECORDS_COUNT = SRC.TOTAL_RECORDS_COUNT,
                            TGT.ROW_UPDATE_TMS = CURRENT_TIMESTAMP
                    WHEN NOT MATCHED THEN
                        INSERT (
                                TABLE_NAME,
                                CDA_SCHEMA_ID,
                                CDA_FOLDER,
                                CDA_FOLDER_TMS,
                                TOTAL_RECORDS_COUNT,
                                ROW_INSERT_TMS,
                                ROW_UPDATE_TMS
                            )
                            VALUES (SRC.TABLE_NAME,
                                    SRC.CDA_SCHEMA_ID,
                                    SRC.CDA_FOLDER,
                                    SRC.CDA_FOLDER_TMS,
                                    TOTAL_RECORDS_COUNT,
                                    CURRENT_TIMESTAMP,
                                    CURRENT_TIMESTAMP);
            """
        ),
    ).fetchone()
    insert_into_jc_process_log(
        cursor=cursor,
        row_count=rows_inserted + rows_updated,
        schema_name=schema_name,
        table_name=table_name,
        batch_id=batch_id,
        process_name=f"merge into manifest details",
    )


@dataclasses.dataclass
class Layer:
    logger: Any
    batch_id: str
    core_center: str
    table_name: str
    schema_name: str
    status: str
    load: bool
    proc_name: ClassVar[str]

    def update_jc_load_status(self, status: str, cursor: SnowflakeCursor):
        cursor.execute(
            dedent(
                f"""
                    UPDATE ETL_CTRL.JC_{self.schema_name}_LOAD_STATUS
                    SET BATCH_ID = {self.batch_id},
                        ROW_UPDATE_TMS = CURRENT_TIMESTAMP,
                        STATUS = $${status}$$
                    WHERE TABLE_NAME = '{self.table_name}';
                """
            )
        )

    def insert_into_jc_error_log(
        self, error_message: str, error_sql: str, cursor: SnowflakeCursor
    ):
        query = dedent(
            f"""
                INSERT INTO ETL_CTRL.JC_ERROR_LOG(
                    BATCHID,
                    DAY_ID,
                    ERROR_MESSAGE,
                    ERROR_SQL,
                    JOB_NAME,
                    ROW_INSERT_TMS,
                    SCHEMA_NAME,
                    TABLE_NAME
                )
                VALUES (
                    {self.batch_id},
                    CURRENT_DATE,
                    $${error_message}$$,
                    $${error_sql}$$,
                    $${self.proc_name}$$,
                    current_timestamp,
                    $${self.schema_name}$$,
                    $${self.table_name}$$
                )
            """
        )
        cursor.execute(query)

    def call_stored_procedure(
        self,
        query: str,
        cursor: SnowflakeCursor,
    ) -> list[tuple] | list[dict]:
        try:
            self.logger.info(
                f"{self.schema_name}.{self.table_name}: Running query: {query}"
            )
            self.update_jc_load_status(status="started", cursor=cursor)
            response = cursor.execute(dedent(query)).fetchall()
            self.update_jc_load_status(status="completed", cursor=cursor)
            self.logger.info(
                f"{self.schema_name}.{self.table_name}: Successful {self.proc_name}"
            )
            return response
        except Exception as exc:
            error_sql = getattr(exc, "query", None)
            if error_sql is not None:
                error_sql = error_sql.replace("$$", r"\$\$")
            error_message = str(getattr(exc, "msg", None))

            self.insert_into_jc_error_log(
                error_message=error_message, error_sql=error_sql, cursor=cursor
            )

            self.update_jc_load_status(status="failed", cursor=cursor)

            self.logger.error(
                f"{self.schema_name}.{self.table_name}: exception occurred while running query: {query}\n{str(exc)}"
            )
            raise LoadException(
                message=f"{self.schema_name}.{self.table_name}: exception occurred while running",
                table=self,
                procedure=self.proc_name,
                cause=exc,
            ) from exc


@dataclasses.dataclass
class RawLayer(Layer):
    proc_name = "SP_PROCESS_RAW_TABLE"

    def execute(self, cursor: SnowflakeCursor) -> None:
        if not self.load:
            self.logger.info(
                f"{self.schema_name}.{self.table_name}: skipping execution"
            )
            return
        self.call_stored_procedure(
            query=f"""
                    CALL ETL_CTRL.{self.proc_name}(
                        {self.batch_id},
                        {self.status.lower() == 'initial'},
                        '{self.core_center}',
                        '{self.table_name}',
                        '{self.schema_name}'
                    );
                """,
            cursor=cursor,
        )

        (
            manifest_total_records_count,
            raw_count,
            total_records_count_diff,
            manifest_cda_folder,
            raw_cda_folder,
            cda_folder_diff,
        ) = cursor.execute(
            dedent(
                f"""
                    SELECT
                        MANIFEST_TOTAL_RECORDS_COUNT,
                        RAW_COUNT,
                        TOTAL_RECORDS_COUNT_DIFF,
                        MANIFEST_CDA_FOLDER,
                        RAW_CDA_FOLDER,
                        CDA_FOLDER_DIFF
                    FROM ETL_CTRL.JC_{self.core_center.upper()}_AUDIT_COUNT_VALIDATION
                    WHERE TABLE_NAME = LOWER('{self.table_name.lower()}')
                        AND BATCH_ID = {self.batch_id};
                """
            )
        ).fetchone()

        if total_records_count_diff != 0 or cda_folder_diff:
            message = dedent(
                f"{self.schema_name}.{self.table_name} | "
                f"TOTAL_RECORDS_COUNT: table {raw_count}, "
                f"manifest: {manifest_total_records_count} | "
                f"CDA_FOLDER: table {raw_cda_folder}, "
                f"manifest: {manifest_cda_folder}"
            )
            self.logger.error(message)
            self.insert_into_jc_error_log(
                error_message=message, error_sql="N/A", cursor=cursor
            )
            raise LoadException(
                message=message,
                table=self,
                procedure=self.proc_name,
                cause=ValueError(message),
            )


@dataclasses.dataclass
class StageLayer(Layer):
    proc_name = "SP_PROCESS_STG_TABLE"

    def execute(
        self,
        cursor: SnowflakeCursor,
        raw_schema_name: str,
        cda_schema_id: str,
        cda_folder: int,
    ) -> None:
        if not self.load:
            self.logger.info(
                f"{self.schema_name}.{self.table_name}: skipping execution"
            )
            return

        self.call_stored_procedure(
            query=f"""
                        CALL ETL_CTRL.{self.proc_name}(
                            {self.batch_id},
                            {self.status.lower() == 'initial'},
                            '{self.core_center}',
                            '{self.table_name}',
                            '{self.schema_name}',
                            '{raw_schema_name}',
                            '{cda_schema_id}',
                            '{cda_folder}'
                        );
                    """,
            cursor=cursor,
        )

        (raw_count, stg_count, stg_count_diff) = cursor.execute(
            dedent(
                f"""
                    SELECT
                        RAW_COUNT,
                        STG_COUNT,
                        STG_COUNT_DIFF
                    FROM ETL_CTRL.JC_{self.core_center.upper()}_AUDIT_COUNT_VALIDATION
                    WHERE TABLE_NAME = LOWER('{self.table_name.lower()}')
                        AND BATCH_ID = {self.batch_id};
                """
            )
        ).fetchone()

        if stg_count_diff != 0:
            message = dedent(
                f"{self.schema_name}.{self.table_name} | "
                f"RAW count: {raw_count}, "
                f"STG count: {stg_count}"
            )
            self.logger.error(message)
            self.insert_into_jc_error_log(
                error_message=message, error_sql="N/A", cursor=cursor
            )
            raise LoadException(
                message=message,
                table=self,
                procedure=self.proc_name,
                cause=ValueError(message),
            )


@dataclasses.dataclass
class MergeLayer(Layer):
    proc_name = "SP_PROCESS_MRG_TABLE"

    def execute(self, cursor: SnowflakeCursor, stg_schema_name: str) -> None:
        if not self.load:
            self.logger.info(
                f"{self.schema_name}.{self.table_name}: skipping execution"
            )
            return

        self.call_stored_procedure(
            query=f"""
                    CALL ETL_CTRL.{self.proc_name}(
                        {self.batch_id},
                        {self.status.lower() == 'initial'},
                        '{self.core_center}',
                        '{self.table_name}',
                        '{self.schema_name}',
                        '{stg_schema_name}'
                     );
                    """,
            cursor=cursor,
        )

        (mrg_count, expected_mrg_count, expected_mrg_count_diff) = cursor.execute(
            dedent(
                f"""
                    SELECT
                        MRG_COUNT,
                        EXPECTED_MRG_COUNT,
                        EXPECTED_MRG_COUNT_DIFF
                    FROM ETL_CTRL.JC_{self.core_center.upper()}_AUDIT_COUNT_VALIDATION
                    WHERE TABLE_NAME = LOWER('{self.table_name.lower()}')
                        AND BATCH_ID = {self.batch_id};
                """
            )
        ).fetchone()

        if expected_mrg_count_diff != 0:
            message = dedent(
                f"{self.schema_name}.{self.table_name} | "
                f"MRG count: {mrg_count}, "
                f"Expected MRG count: {expected_mrg_count}"
            )
            self.logger.error(message)
            self.insert_into_jc_error_log(
                error_message=message, error_sql="N/A", cursor=cursor
            )
            raise LoadException(
                message=message,
                table=self,
                procedure=self.proc_name,
                cause=ValueError(message),
            )


@dataclasses.dataclass
class Table:
    logger: logging.Logger
    table_name: str
    core_center: str
    batch_id: str
    raw_layer: RawLayer
    stg_layer: StageLayer
    mrg_layer: MergeLayer

    load_s3: bool
    src_cda_schema_id: str
    src_cda_folder: int
    src_cda_folder_tms: datetime
    src_total_records_count: int
    src_bucket_name: str
    src_prefix: str

    dest_cda_folder: int | None
    dest_cda_folder_tms: datetime
    dest_bucket_name: str
    dest_prefix: str

    def get_table_path(self, prefix: str) -> str:
        if prefix == "":
            return self.table_name
        return f"{prefix}/{self.table_name}"

    @property
    def src_table_path(self) -> str:
        return self.get_table_path(prefix=self.src_prefix)

    @property
    def dest_table_path(self) -> str:
        return self.get_table_path(prefix=self.dest_prefix)

    @property
    def src_full_path(self) -> str:
        return f"{self.src_bucket_name}/{self.src_table_path}"

    @property
    def dest_full_path(self) -> str:
        return f"{self.dest_bucket_name}/{self.dest_table_path}"

    def get_src_cda_timestamp_prefixes(
        self, paginator: Paginator
    ) -> Generator[str, Any, None]:
        schema_paginator = paginator.paginate(
            Bucket=self.src_bucket_name,
            Prefix=f"{self.src_table_path}/",
            Delimiter="/",
        )

        for schema in schema_paginator.search("CommonPrefixes"):
            cda_schema_prefix = schema.get("Prefix")
            cda_timestamp_paginator = paginator.paginate(
                Bucket=self.src_bucket_name,
                Prefix=cda_schema_prefix,
                Delimiter="/",
            )
            for cda_timestamp in cda_timestamp_paginator.search("CommonPrefixes"):
                yield cda_timestamp.get("Prefix")

    def get_loadable_cda_timestamp_prefixes(
        self, paginator: Paginator, logger
    ) -> Generator[str, Any, None]:
        logger.info(f"{self.table_name}: getting CDA timestamp prefixes")
        cda_timestamp_prefixes = self.get_src_cda_timestamp_prefixes(
            paginator=paginator
        )
        low_bound_cda_timestamp = (
            0 if self.dest_cda_folder is None else self.dest_cda_folder
        )
        for cda_timestamp_prefix in cda_timestamp_prefixes:
            cda_timestamp = int(cda_timestamp_prefix.split("/")[-2])
            if low_bound_cda_timestamp < cda_timestamp <= self.src_cda_folder:
                yield cda_timestamp_prefix

    def load_to_s3(
        self, executor: ThreadPoolExecutor, handler: Handler, cursor: SnowflakeCursor
    ) -> None:
        if not self.load_s3:
            self.logger.info(f"{self.table_name}: skipping S3 load")
            return

        paginator = handler.s3_client.get_paginator("list_objects_v2")

        cda_timestamp_prefixes = self.get_loadable_cda_timestamp_prefixes(
            paginator=paginator, logger=handler.logger
        )

        exceptions: list[Exception] = []

        futures_cda_timestamp_prefix = {
            executor.submit(
                handler.copy_files_for_folder,
                cda_timestamp_prefix=cda_timestamp_prefix,
            ): cda_timestamp_prefix
            for cda_timestamp_prefix in cda_timestamp_prefixes
        }

        # Wait for all tasks to complete
        for future in as_completed(futures_cda_timestamp_prefix):
            try:
                future.result()
            except Exception as exc:
                exceptions.append(exc)

        if exceptions:
            raise exceptions[0]

        handler.logger.info(
            f"{'incremental' if handler.is_incremental else 'initial'} "
            f"file transfer for all {self.core_center} {self.table_name} completed"
        )

        self.raw_layer.load = True
        set_jc_load_status_post_s3_load(
            schema_name=self.raw_layer.schema_name,
            table_name=self.table_name,
            batch_id=self.batch_id,
            cursor=cursor,
            logger=self.logger,
        )

        self.stg_layer.load = True
        set_jc_load_status_post_s3_load(
            schema_name=self.stg_layer.schema_name,
            table_name=self.table_name,
            batch_id=self.batch_id,
            cursor=cursor,
            logger=self.logger,
        )

        self.mrg_layer.load = True
        set_jc_load_status_post_s3_load(
            schema_name=self.mrg_layer.schema_name,
            table_name=self.table_name,
            batch_id=self.batch_id,
            cursor=cursor,
            logger=self.logger,
        )

        set_jc_manifest_post_s3_load(
            core_center=self.core_center,
            schema_name=self.raw_layer.schema_name,
            table_name=self.table_name,
            cda_schema_id=self.src_cda_schema_id,
            cda_folder=self.src_cda_folder,
            cda_folder_tms=self.src_cda_folder_tms,
            total_records_count=self.src_total_records_count,
            batch_id=self.batch_id,
            cursor=cursor,
            logger=self.logger,
        )

    def load_to_mrg(
        self,
        handler: Handler,
        s3_executor: ThreadPoolExecutor,
        connection: SnowflakeConnection,
    ):
        with connection.cursor() as cursor:
            if handler.s3_load_enabled:
                self.load_to_s3(executor=s3_executor, handler=handler, cursor=cursor)

            if not handler.mrg_load_enabled:
                return

            self.raw_layer.execute(cursor=cursor)

            self.stg_layer.execute(
                cursor=cursor,
                raw_schema_name=self.raw_layer.schema_name,
                cda_schema_id=self.src_cda_schema_id,
                cda_folder=self.src_cda_folder,
            )

            self.mrg_layer.execute(
                cursor=cursor,
                stg_schema_name=self.stg_layer.schema_name,
            )


def get_core_center_details(
    is_incremental: bool,
    core_center: str,
    logger: logging.Logger,
    connection: SnowflakeConnection,
) -> tuple[bool, bool, str, str, str]:
    logger.info(f"fetching {core_center} core center details")
    with connection.cursor() as cursor:
        (
            s3_load_enabled,
            mrg_load_enabled,
            raw_schema_name,
            stg_schema_name,
            mrg_schema_name,
        ) = cursor.execute(
            dedent(
                f"""
                    SELECT
                        IFF(
                            {is_incremental},
                            S3_INCREMENTAL_LOAD_FLAG = TRUE,
                            S3_INITIAL_LOAD_FLAG = TRUE
                        )                        AS S3_LOAD_ENABLED,
                        IFF(
                            {is_incremental},
                            MRG_INCREMENTAL_LOAD_FLAG = TRUE,
                            MRG_INITIAL_LOAD_FLAG = TRUE
                        )                        AS MRG_LOAD_ENABLED,
                        UPPER(RAW_SCHEMA_NAME)   AS RAW_SCHEMA_NAME,
                        UPPER(STG_SCHEMA_NAME)   AS STG_SCHEMA_NAME,
                        UPPER(MERGE_SCHEMA_NAME) AS MRG_SCHEMA_NAME
                    FROM ETL_CTRL.JC_CORE_CENTER_DETAILS
                    WHERE UPPER(CORE_CENTER) = UPPER('{core_center}')       
                """
            )
        ).fetchone()
        return (
            s3_load_enabled,
            mrg_load_enabled,
            raw_schema_name,
            stg_schema_name,
            mrg_schema_name,
        )


@dataclasses.dataclass
class Handler:
    logger: logging.Logger
    sns_client: SNSClient
    s3_client: BaseClient
    connection: SnowflakeConnection
    batch_id: str
    job_name: str
    env_name: str
    is_incremental: bool
    core_center: str
    src_bucket: str
    src_prefix: str
    dest_bucket: str
    raw_schema_name: str
    stg_schema_name: str
    mrg_schema_name: str
    s3_load_enabled: bool
    mrg_load_enabled: bool
    dest_prefix: ClassVar[str] = "__copy_into__"

    def copy_manifest_file(self):
        """
        Copy manifest file from source to temporary path in dest bucket
        """
        if self.src_prefix == "":
            src_path = "manifest.json"
        else:
            src_path = f"{self.src_prefix}/manifest.json"

        if self.dest_prefix == "":
            dest_path = "manifest.json"
        else:
            dest_path = f"{self.dest_prefix}/manifest.json"

        self.copy_file(
            src_bucket=self.src_bucket,
            src_path=src_path,
            dest_bucket=self.dest_bucket,
            dest_path=dest_path,
        )
        self.logger.info(
            f"manifest file copied from {self.src_bucket}/{src_path} to {self.dest_bucket}/{dest_path}"
        )

    def truncate_jc_manifest_details(self):
        self.logger.info(
            f"truncating ETL_CTRL.JC_{self.core_center.upper()}_MANIFEST_DETAILS"
        )
        with self.connection.cursor() as cursor:
            cursor.execute(
                dedent(
                    f"""
                        TRUNCATE TABLE ETL_CTRL.JC_{self.core_center.upper()}_MANIFEST_DETAILS;
                    """
                )
            ).fetchone()

    def truncate_jc_load_status(self, schema_name: str):
        self.logger.info(f"truncating ETL_CTRL.JC_{schema_name}_LOAD_STATUS")
        with self.connection.cursor() as cursor:
            cursor.execute(
                dedent(
                    f"""
                        TRUNCATE TABLE ETL_CTRL.JC_{schema_name}_LOAD_STATUS;
                    """
                )
            ).fetchone()

    def truncate_manifest_and_load_status(self):
        self.truncate_jc_manifest_details()
        self.truncate_jc_load_status(schema_name=self.raw_schema_name)
        self.truncate_jc_load_status(schema_name=self.stg_schema_name)
        self.truncate_jc_load_status(schema_name=self.mrg_schema_name)

    def get_altered_tables(self) -> list[Table]:
        """
        Compare the current manifest (from KFB) to the new manifest (from GW) and return tables
        where on of the following is true:
        - The lastSuccessfulWriteTimestamp has changed
        - The table is missing from either the current or new manifest

        :return:
        """
        self.logger.info(f"fetching altered tables")
        query = dedent(
            f"""
                    SELECT
                        LOWER(SRC_MFST.TABLE_NAME)                            AS TABLE_NAME,
                        SRC_MFST.CDA_SCHEMA_ID                                AS SRC_CDA_SCHEMA_ID,
                        SRC_MFST.CDA_FOLDER::NUMBER                           AS SRC_CDA_FOLDER,
                        SRC_MFST.CDA_FOLDER_TMS                               AS SRC_CDA_FOLDER_TMS,
                        SRC_MFST.TOTAL_RECORDS_COUNT                          AS SRC_TOTAL_RECORDS_COUNT,
                        DEST_MFST.CDA_FOLDER::NUMBER                          AS DEST_CDA_FOLDER,
                        DEST_MFST.CDA_FOLDER_TMS                              AS DEST_CDA_FOLDER_TMS,
                        DEST_MFST.CDA_FOLDER_TMS IS NULL
                        OR SRC_MFST.CDA_FOLDER_TMS > DEST_MFST.CDA_FOLDER_TMS AS LOAD_S3,
                        COALESCE(RAW.STATUS, 'initial')                       AS RAW_STATUS,
                        LOAD_S3 OR NOT EQUAL_NULL(RAW.STATUS, 'completed')    AS LOAD_RAW,
                        COALESCE(STG.STATUS, 'initial')                       AS STG_STATUS,
                        LOAD_RAW OR NOT EQUAL_NULL(STG.STATUS, 'completed')   AS LOAD_STG,
                        COALESCE(MRG.STATUS, 'initial')                       AS MRG_STATUS,
                        LOAD_STG OR NOT EQUAL_NULL(MRG.STATUS, 'completed')   AS LOAD_MRG
                    FROM ETL_CTRL.VW_{self.core_center.upper()}_MANIFEST_DETAILS AS SRC_MFST
                        LEFT JOIN ETL_CTRL.JC_{self.core_center.upper()}_MANIFEST_DETAILS AS DEST_MFST
                            ON SRC_MFST.TABLE_NAME = DEST_MFST.TABLE_NAME
                        LEFT JOIN ETL_CTRL.JC_{self.raw_schema_name.upper()}_LOAD_STATUS AS RAW
                            ON LOWER(SRC_MFST.TABLE_NAME) = LOWER(RAW.TABLE_NAME)
                        LEFT JOIN ETL_CTRL.JC_{self.stg_schema_name.upper()}_LOAD_STATUS AS STG
                            ON LOWER(SRC_MFST.TABLE_NAME) = LOWER(STG.TABLE_NAME)
                        LEFT JOIN ETL_CTRL.JC_{self.mrg_schema_name.upper()}_LOAD_STATUS AS MRG
                            ON LOWER(SRC_MFST.TABLE_NAME) = LOWER(MRG.TABLE_NAME)
                    WHERE LOWER(SRC_MFST.TABLE_NAME) != 'heartbeat' 
                        AND (
                            ({self.s3_load_enabled} AND LOAD_S3) OR
                            ({self.mrg_load_enabled} AND (LOAD_RAW OR LOAD_STG OR LOAD_MRG))
                        )
                """
        )
        tables: list[Table] = []

        with self.connection.cursor() as cursor:
            for (
                table_name,
                src_cda_schema_id,
                src_cda_folder,
                src_cda_folder_tms,
                src_total_records_count,
                dest_cda_folder,
                dest_cda_folder_tms,
                load_s3,
                raw_status,
                load_raw,
                stg_status,
                load_stg,
                mrg_status,
                load_mrg,
            ) in cursor.execute(query).fetchall():
                self.logger.info(
                    f"{table_name}: load_s3: {load_s3} load_raw: {load_raw} load_stg: {load_stg} load_mrg: {load_mrg}"
                )
                tables.append(
                    Table(
                        logger=self.logger,
                        table_name=table_name,
                        core_center=self.core_center,
                        batch_id=self.batch_id,
                        raw_layer=RawLayer(
                            logger=self.logger,
                            batch_id=self.batch_id,
                            core_center=self.core_center,
                            table_name=table_name,
                            schema_name=self.raw_schema_name,
                            status=raw_status,
                            load=load_raw,
                        ),
                        stg_layer=StageLayer(
                            logger=self.logger,
                            batch_id=self.batch_id,
                            core_center=self.core_center,
                            table_name=table_name,
                            schema_name=self.stg_schema_name,
                            status=stg_status,
                            load=load_stg,
                        ),
                        mrg_layer=MergeLayer(
                            logger=self.logger,
                            batch_id=self.batch_id,
                            core_center=self.core_center,
                            table_name=table_name,
                            schema_name=self.mrg_schema_name,
                            status=mrg_status,
                            load=load_mrg,
                        ),
                        load_s3=load_s3,
                        # src
                        src_cda_schema_id=src_cda_schema_id,
                        src_cda_folder=src_cda_folder,
                        src_cda_folder_tms=src_cda_folder_tms,
                        src_total_records_count=src_total_records_count,
                        src_bucket_name=self.src_bucket,
                        src_prefix=self.src_prefix,
                        # dest
                        dest_cda_folder=dest_cda_folder,
                        dest_cda_folder_tms=dest_cda_folder_tms,
                        dest_bucket_name=self.dest_bucket,
                        dest_prefix=self.dest_prefix,
                    )
                )

        return tables

    def copy_file(
        self, src_bucket: str, src_path: str, dest_bucket: str, dest_path: str
    ):
        copy_source = {"Bucket": src_bucket, "Key": src_path}
        max_retries = 10
        delay = 2  # initial delay
        delay_incr = 2  # additional delay in each loop

        for retry_count in range(max_retries + 1):
            try:
                self.s3_client.copy(copy_source, dest_bucket, dest_path)

                if not IS_GLUE_JOB:
                    self.logger.debug(
                        f"file copied from {src_bucket}/{src_path} to {dest_bucket}/{dest_path}"
                    )

                return

            except ClientError as err:
                if retry_count == max_retries:
                    raise err

                error = err.response.get("Error")
                if not error:
                    raise err

                code = error.get("Code", "")
                if code != "SlowDown":
                    raise err

                sleep(delay)
                delay += delay_incr

    def copy_files_for_folder(self, cda_timestamp_prefix: str) -> None:
        paginator = self.s3_client.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(
            Bucket=self.src_bucket,
            Prefix=cda_timestamp_prefix,
        )

        # Filter results for parquet files only.
        parquet_objects = page_iterator.search(
            "Contents[?ends_with(Key, '.snappy.parquet')][]"
        )

        [
            self.copy_file(
                src_bucket=self.src_bucket,
                src_path=key_data["Key"],
                dest_bucket=self.dest_bucket,
                dest_path=f'{self.dest_prefix}{key_data["Key"].replace(self.src_prefix, "")}',
            )
            for key_data in parquet_objects
        ]
        self.logger.info(
            f"cda timestamp folder {cda_timestamp_prefix} copied from {self.src_bucket} to {self.dest_bucket}"
        )

    def send_no_files_or_tables_notification(self) -> None:
        if not self.s3_load_enabled and not self.mrg_load_enabled:
            self.sns_client.send_notification(
                subject=(
                    f"{self.env_name} Ingestion - {self.core_center} - S3 and Merge load are disabled"
                ),
                message=(
                    "Hi team,\n\n"
                    f"S3 and Merge load are disabled for the '{self.core_center}' core center\n\n"
                    "Thanks"
                ),
            )
        else:
            self.sns_client.send_notification(
                subject=(
                    f"{self.env_name} Ingestion - {self.core_center} - No "
                    f"{'incremental' if self.is_incremental else 'initial'} "
                    f"files to copy or tables to load through the MRG layer"
                ),
                message=(
                    "Hi team,\n\n"
                    f"The {self.core_center} Glue job {self.job_name} processed 0 "
                    f"{'incremental' if self.is_incremental else 'initial'} files in its latest run. "
                    "This is because there were no new files available for ingestion and no Snowflake "
                    "tables to load through the MRG layer\n\n"
                    "Thanks"
                ),
            )

    def load_tables_to_mrg(
        self,
        tables: list[Table],
        max_workers: int,
        s3_executor: ThreadPoolExecutor,
    ) -> list[LoadException]:
        exceptions: list[LoadException] = []

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures_table_dict = {
                executor.submit(
                    table.load_to_mrg,
                    handler=self,
                    s3_executor=s3_executor,
                    connection=self.connection,
                ): table
                for table in tables
            }

            # Wait for all tasks to complete
            for future in as_completed(futures_table_dict):
                table = futures_table_dict[future]
                try:
                    future.result()
                    self.logger.info(f"{table.table_name}: Successful load to merge")
                except LoadException as exc:
                    exceptions.append(exc)
        return exceptions

    def send_failure_notification(
        self, exc: Exception, exceptions: list[LoadException]
    ) -> None:
        exception_message = (
            str(exc)
            if not exceptions
            else "\n\n\n".join(
                [
                    (
                        f"Table Name: {exc.table.table_name}\n"
                        f"Procedure Name: {exc.procedure}\n"
                        f"Exception: {str(exc.cause).splitlines()[-1]}\n\n"
                        f"********************************************************"
                    )
                    for exc in exceptions
                ]
            )
        )
        if len(exception_message) > 1500:
            exception_message = exception_message[:1500]

        message = dedent(
            "Hi team,\n\n"
            f"FAILURE: The {'incremental' if self.is_incremental else 'initial'} glue job {self.job_name} "
            f"for the '{self.core_center}' core center failed to copy the files from GW CDA to Snowflake Mrg layer. "
            f"The errors are listed below: \n\n{exception_message}\n\n"
            "For more information please check Cloudwatch latest log streams.\n\n"
            "Thanks"
        )

        self.sns_client.send_notification(
            subject=f"FAILURE: Ingestion - GW CDA to Snowflake Mrg layer Failed for '{self.core_center}' core center",
            message=message,
        )

    def send_success_notification(self) -> None:
        self.sns_client.send_notification(
            subject=f"SUCCESS: Ingestion - GW CDA to Snowflake Mrg layer "
            f"Succeeded for '{self.core_center}' core center",
            message=dedent(
                "Hi team,\n\n"
                f"SUCCESS: The glue job {self.job_name} has succeeded for the '{self.core_center}' core center."
            ),
        )


def main(
    logger,
    job_name: str,
    env_name: str,
    secret_name: str,
    sns_topic_name: str,
    batch_id: str,
    core_center: str,
    gw_cda_bucket: str,
    gw_s3_dir: str,
    kfb_s3_bucket: str,
    is_incremental: bool,
    database_name: str,
    aws_profile: str | None = None,
):
    logger.info(
        f"The {'incremental' if is_incremental else 'initial'} job has started for {core_center}"
    )
    exceptions: list[LoadException] = []
    aws_session = Session(profile_name=aws_profile, region_name="us-east-1")
    sf_connection = get_snowflake_connection(
        secretsmanager_client=aws_session.client("secretsmanager"),
        secret_name=secret_name,
        database_name=database_name,
    )
    sns_client = SNSClient.factory(
        session=aws_session, topic_name=sns_topic_name, logger=logger
    )
    max_s3_workers = 60
    max_sf_workers = 20
    boto_config = Config(max_pool_connections=max_s3_workers)
    s3_client = aws_session.client("s3", config=boto_config)
    (
        s3_load_enabled,
        mrg_load_enabled,
        raw_schema_name,
        stg_schema_name,
        mrg_schema_name,
    ) = get_core_center_details(
        is_incremental=is_incremental,
        core_center=core_center,
        connection=sf_connection,
        logger=logger,
    )
    handler = Handler(
        logger=logger,
        sns_client=sns_client,
        s3_client=s3_client,
        connection=sf_connection,
        batch_id=batch_id,
        job_name=job_name,
        env_name=env_name,
        is_incremental=is_incremental,
        core_center=core_center,
        src_bucket=gw_cda_bucket,
        src_prefix=gw_s3_dir,
        dest_bucket=kfb_s3_bucket,
        s3_load_enabled=s3_load_enabled,
        mrg_load_enabled=mrg_load_enabled,
        raw_schema_name=raw_schema_name,
        stg_schema_name=stg_schema_name,
        mrg_schema_name=mrg_schema_name,
    )

    try:
        if handler.s3_load_enabled and not handler.is_incremental:
            handler.truncate_manifest_and_load_status()

        if handler.s3_load_enabled:
            handler.copy_manifest_file()

        tables = handler.get_altered_tables()

        if len(tables) == 0:
            handler.send_no_files_or_tables_notification()

        with ThreadPoolExecutor(max_workers=max_s3_workers) as s3_executor:
            exceptions = handler.load_tables_to_mrg(
                tables=tables, max_workers=max_sf_workers, s3_executor=s3_executor
            )

        if exceptions:
            raise exceptions[0]

        handler.send_success_notification()
        handler.logger.info(
            f"{'incremental' if is_incremental else 'initial'} file transfer for all {core_center} tables completed"
        )
    except Exception as exc:
        handler.send_failure_notification(exc=exc, exceptions=exceptions)
        raise exc


def get_core_center_config(in_args: dict[str, str]) -> dict[str, str]:
    core_center_configs = {
        "pc": {
            "gw_cda_bucket": in_args["gw_pc_s3_bucket"],
            "gw_s3_dir": in_args["gw_pc_dir"],
            "kfb_s3_bucket": in_args["kfb_s3_pc_bucket"],
        },
        "bc": {
            "gw_cda_bucket": in_args["gw_bc_s3_bucket"],
            "gw_s3_dir": in_args["gw_bc_dir"],
            "kfb_s3_bucket": in_args["kfb_s3_bc_bucket"],
        },
        "cc": {
            "gw_cda_bucket": in_args["gw_cc_s3_bucket"],
            "gw_s3_dir": in_args["gw_cc_dir"],
            "kfb_s3_bucket": in_args["kfb_s3_cc_bucket"],
        },
        "cm": {
            "gw_cda_bucket": in_args["gw_cm_s3_bucket"],
            "gw_s3_dir": in_args["gw_cm_dir"],
            "kfb_s3_bucket": in_args["kfb_s3_cm_bucket"],
        },
    }

    _core_center = in_args["core_center"]

    if _core_center not in core_center_configs:
        raise ValueError(
            f"Invalid arguments. Must provide one of {', '.join([key for key in core_center_configs.keys()])}."
        )

    _core_center_config = core_center_configs[_core_center]
    if _core_center_config["gw_s3_dir"] == "NOT AVAILABLE":
        _core_center_config["gw_s3_dir"] = ""

    _core_center_config["gw_s3_dir"] = _core_center_config["gw_s3_dir"].rstrip("/")

    return _core_center_config


if __name__ == "__main__" and IS_GLUE_JOB:
    ## @params: [JOB_NAME]
    args = getResolvedOptions(
        sys.argv,
        [
            # Hard-coded parameters
            "JOB_NAME",
            "gw_bc_dir",
            "gw_bc_s3_bucket",
            "gw_cc_dir",
            "gw_cc_s3_bucket",
            "gw_cm_dir",
            "gw_cm_s3_bucket",
            "gw_pc_dir",
            "gw_pc_s3_bucket",
            "kfb_s3_bc_bucket",
            "kfb_s3_cc_bucket",
            "kfb_s3_cm_bucket",
            "kfb_s3_pc_bucket",
            "sns_topic_name",
            "secret_name",
            # Supplied by Matillion
            "batchid",
            "core_center",
            "env_name",
            "is_incremental",
            "database_name",
        ],
    )
    sc = SparkContext()
    glue_context = GlueContext(sc)
    glue_logger = glue_context.get_logger()
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args["JOB_NAME"] + args["core_center"], args)

    core_center_config = get_core_center_config(args)

    main(
        logger=glue_logger,
        job_name=args["JOB_NAME"],
        env_name=args["env_name"],
        secret_name=args["secret_name"],
        sns_topic_name=args["sns_topic_name"],
        batch_id=args["batchid"],
        core_center=args["core_center"],
        gw_cda_bucket=core_center_config["gw_cda_bucket"],
        gw_s3_dir=core_center_config["gw_s3_dir"],
        kfb_s3_bucket=core_center_config["kfb_s3_bucket"],
        is_incremental=True if args["is_incremental"].upper() == "TRUE" else False,
        database_name=args["database_name"],
    )

    job.commit()
elif __name__ == "__main__" and not IS_GLUE_JOB:
    if "REQUESTS_CA_BUNDLE" in os.environ:
        os.environ.pop("REQUESTS_CA_BUNDLE")

    _logger = logging.getLogger(__name__)
    _logger.setLevel(logging.DEBUG)

    args = {
        "JOB_NAME": "GW_S3_to_Snowflake_MRG",
        "core_center": "cc",
        "sns_topic_name": "kfbmic-edr-np-dev-snstopic",
        "gw_pc_s3_bucket": "kfbmic-edr-np-gw-dev-landing-zone-pc-01-bucket",
        "gw_bc_s3_bucket": "kfbmic-edr-np-gw-dev-landing-zone-bc-01-bucket",
        "gw_cc_s3_bucket": "kfbmic-edr-np-gw-dev-landing-zone-cc-01-bucket",
        "gw_cm_s3_bucket": "kfbmic-edr-np-gw-dev-landing-zone-cm-01-bucket",
        "gw_pc_dir": "NOT AVAILABLE",
        "gw_bc_dir": "NOT AVAILABLE",
        "gw_cc_dir": "NOT AVAILABLE",
        "gw_cm_dir": "NOT AVAILABLE",
        "kfb_s3_pc_bucket": "kfbmic-edr-np-gw-dev-landing-zone-pc-01-bucket",
        "kfb_s3_bc_bucket": "kfbmic-edr-np-gw-dev-landing-zone-bc-01-bucket",
        "kfb_s3_cc_bucket": "kfbmic-edr-np-gw-dev-landing-zone-cc-01-bucket",
        "kfb_s3_cm_bucket": "kfbmic-edr-np-gw-dev-landing-zone-cm-01-bucket",
        "env_name": "kfbmic-edr-np-gw-dev-landing-zone-cm-01-bucket",
        "is_incremental": "false",
        "secret_name": "kfbmic-edr-np-dev-glue-snowflake-service-secret",
        "database_name": "KFB_ECDP_DEV",
        "batchid": "-4567",
    }

    core_center_config = get_core_center_config(args)

    main(
        logger=_logger,
        job_name=args["JOB_NAME"],
        env_name=args["env_name"],
        secret_name=args["secret_name"],
        sns_topic_name=args["sns_topic_name"],
        batch_id=args["batchid"],
        core_center=args["core_center"],
        gw_cda_bucket=core_center_config["gw_cda_bucket"],
        gw_s3_dir=core_center_config["gw_s3_dir"],
        kfb_s3_bucket=core_center_config["kfb_s3_bucket"],
        aws_profile="nonprod",
        is_incremental=True if args["is_incremental"].upper() == "TRUE" else False,
        database_name=args["database_name"],
    )
