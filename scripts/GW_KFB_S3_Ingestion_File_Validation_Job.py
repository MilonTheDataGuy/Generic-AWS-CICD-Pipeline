from __future__ import annotations

import dataclasses
import os
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from textwrap import dedent

from boto3 import Session as AWSSession
from botocore.client import BaseClient
import json
import logging

from botocore.config import Config
from snowflake.snowpark import Session as SFSession, DataFrame, MergeResult
from snowflake.snowpark.functions import when_matched, when_not_matched
from snowflake.snowpark.types import (
    StringType,
    IntegerType,
    StructType,
    StructField,
    TimestampType,
    TimestampTimeZone,
)

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
    "concurrent.futures",
]:
    logging.getLogger(noisy_logger).setLevel(logging.WARNING)


def get_snowpark_session(
    secretsmanager_client, secret_name: str, database_name: str
) -> SFSession:
    get_secret_value_response = secretsmanager_client.get_secret_value(
        SecretId=secret_name
    )
    secrets_value = json.loads(get_secret_value_response["SecretString"])
    return SFSession.builder.configs(
        {
            "user": secrets_value["SNOWFLAKE_USER"],
            "password": secrets_value["SNOWFLAKE_PASSWORD"],
            "account": secrets_value["SNOWFLAKE_ACCOUNT"],
            "warehouse": secrets_value["SNOWFLAKE_WAREHOUSE"],
            "database": database_name,
            "schema": "ETL_CTRL",
            "role": secrets_value["SNOWFLAKE_ROLE"],
        }
    ).create()


@dataclasses.dataclass(frozen=True)
class CDAPath:
    table_name: str
    cda_schema_id: str
    cda_timestamp: str


@dataclasses.dataclass(frozen=False)
class CDAPathContents:
    bytes: int
    file_count: int
    max_last_modified: datetime | None


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
        session: AWSSession,
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


@dataclasses.dataclass
class Handler:
    s3_client: BaseClient
    session: SFSession

    def get_paths(self, bucket: str, prefix: str) -> dict[CDAPath, CDAPathContents]:
        paths: dict[CDAPath, CDAPathContents] = defaultdict(
            lambda: CDAPathContents(bytes=0, file_count=0, max_last_modified=None)
        )
        paginator = self.s3_client.get_paginator("list_objects_v2")

        page_iterator = paginator.paginate(
            Bucket=bucket,
            Prefix=prefix,
        )
        # Filter results for parquet files only.
        parquet_objects = page_iterator.search(
            "Contents[?ends_with(Key, '.snappy.parquet')][]"
        )
        for key_data in parquet_objects:
            parquet_path = Path(key_data["Key"])
            path = CDAPath(
                cda_timestamp=parquet_path.parents[0].stem,
                cda_schema_id=parquet_path.parents[1].stem,
                table_name=parquet_path.parents[2].stem,
            )
            content = paths[path]
            content.bytes += key_data["Size"]
            content.file_count += 1
            if (
                content.max_last_modified is None
                or content.max_last_modified < key_data["LastModified"]
            ):
                content.max_last_modified = key_data["LastModified"]

        return paths

    def get_df(
        self,
        target_system: str,
        core_center: str,
        paths: dict[CDAPath, CDAPathContents],
    ) -> DataFrame:
        schema = StructType(
            [
                StructField("SYSTEM", StringType(), nullable=False),
                StructField("CORE_CENTER", StringType(), nullable=False),
                StructField("TABLE_NAME", StringType(), nullable=False),
                StructField("CDA_SCHEMA_ID", StringType(), nullable=False),
                StructField("CDA_TIMESTAMP", StringType(), nullable=False),
                StructField("TOTAL_BYTES", IntegerType(), nullable=False),
                StructField("FILE_COUNT", IntegerType(), nullable=False),
                StructField(
                    "MAX_LAST_MODIFIED",
                    TimestampType(timezone=TimestampTimeZone.TZ),
                    nullable=False,
                ),
                StructField(
                    "ROW_INSERT_TMS",
                    TimestampType(timezone=TimestampTimeZone.NTZ),
                    nullable=False,
                ),
                StructField(
                    "ROW_UPDATE_TMS",
                    TimestampType(timezone=TimestampTimeZone.NTZ),
                    nullable=False,
                ),
            ]
        )

        return self.session.create_dataframe(
            [
                [
                    target_system,
                    core_center,
                    path.table_name,
                    path.cda_schema_id,
                    path.cda_timestamp,
                    contents.bytes,
                    contents.file_count,
                    contents.max_last_modified,
                    datetime.now(),
                    datetime.now(),
                ]
                for path, contents in paths.items()
            ],
            schema=schema,
        )

    def merge_df(self, database_name: str, source: DataFrame) -> MergeResult:
        self.session.use_database(database_name)
        self.session.use_schema("ETL_CTRL")
        target = self.session.table("FILE_COUNTS")

        # noinspection PyTypeChecker
        return target.merge(
            source,
            (target["SYSTEM"] == source["SYSTEM"])
            & (target["CORE_CENTER"] == source["CORE_CENTER"])
            & (target["TABLE_NAME"] == source["TABLE_NAME"])
            & (target["CDA_SCHEMA_ID"] == source["CDA_SCHEMA_ID"])
            & (target["CDA_TIMESTAMP"] == source["CDA_TIMESTAMP"]),
            [
                when_matched().update(
                    {
                        "TOTAL_BYTES": source["TOTAL_BYTES"],
                        "FILE_COUNT": source["FILE_COUNT"],
                        "MAX_LAST_MODIFIED": source["MAX_LAST_MODIFIED"],
                        "ROW_UPDATE_TMS": source["ROW_UPDATE_TMS"],
                    }
                ),
                when_not_matched().insert(
                    {
                        "SYSTEM": source["SYSTEM"],
                        "CORE_CENTER": source["CORE_CENTER"],
                        "TABLE_NAME": source["TABLE_NAME"],
                        "CDA_SCHEMA_ID": source["CDA_SCHEMA_ID"],
                        "CDA_TIMESTAMP": source["CDA_TIMESTAMP"],
                        "TOTAL_BYTES": source["TOTAL_BYTES"],
                        "FILE_COUNT": source["FILE_COUNT"],
                        "MAX_LAST_MODIFIED": source["MAX_LAST_MODIFIED"],
                        "ROW_INSERT_TMS": source["ROW_INSERT_TMS"],
                        "ROW_UPDATE_TMS": source["ROW_UPDATE_TMS"],
                    }
                ),
            ],
        )


def main(
    logger,
    job_name: str,
    database_name: str,
    core_center: str,
    secret_name: str,
    gw_cda_bucket: str,
    gw_s3_dir: str,
    kfb_s3_bucket: str,
    target_system: str,
    sns_topic_name: str,
    aws_profile: str | None = None,
):
    logger.info(f"The persist file count / bytes job has started for {core_center}")
    aws_session = AWSSession(profile_name=aws_profile, region_name="us-east-1")
    sns_client = SNSClient.factory(
        session=aws_session, topic_name=sns_topic_name, logger=logger
    )
    boto_config = Config(max_pool_connections=1)
    s3_client = aws_session.client("s3", config=boto_config)
    sf_session = get_snowpark_session(
        secretsmanager_client=aws_session.client("secretsmanager"),
        secret_name=secret_name,
        database_name=database_name,
    )

    # Get bucket prefixes based on core_center
    if target_system == "gw":
        bucket = gw_cda_bucket
        prefix = gw_s3_dir
    elif target_system == "kfb":
        bucket = kfb_s3_bucket
        prefix = "__copy_into__/__archive__"
    else:
        raise Exception(
            f"target_system must be one of kfb or gw. received {target_system}"
        )

    handler = Handler(
        session=sf_session,
        s3_client=s3_client,
    )

    try:
        paths = handler.get_paths(bucket=bucket, prefix=prefix)
        df = handler.get_df(
            paths=paths, target_system=target_system, core_center=core_center
        )
        result = handler.merge_df(source=df, database_name=database_name)
    except Exception as exc:
        # Send SNS error notification
        logger.exception(f"Unhandled exception: {str(exc)}")
        sns_client.send_notification(
            subject=f"S3 validation - GW KFB S3 File Validation for {args['core_center']}",
            message="Hi team,"
            f"The GW - KFB Glue job {job_name} for {core_center} failed to validate the files count & size."
            f"The errors are as follows: {str(exc)}"
            "For more information, please check the CloudWatch latest log streams"
            "Thanks",
        )
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
            "JOB_NAME",
            "core_center",
            "database_name",
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
            "secret_name",
            "sns_topic_name",
            "target_system",
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
        secret_name=args["secret_name"],
        sns_topic_name=args["sns_topic_name"],
        target_system=args["target_system"],
        core_center=args["core_center"],
        gw_cda_bucket=core_center_config["gw_cda_bucket"],
        gw_s3_dir=core_center_config["gw_s3_dir"],
        kfb_s3_bucket=core_center_config["kfb_s3_bucket"],
        database_name=args["database_name"],
    )

    job.commit()
elif __name__ == "__main__" and not IS_GLUE_JOB:
    if "REQUESTS_CA_BUNDLE" in os.environ:
        os.environ.pop("REQUESTS_CA_BUNDLE")

    _logger = logging.getLogger(__name__)
    _logger.setLevel(logging.INFO)

    args = {
        "JOB_NAME": "GW_KFB_S3_Ingestion_File_Validation_Job",
        "core_center": "pc",
        "database_name": "KFB_ECDP_DEV",
        "gw_bc_dir": "NOT AVAILABLE",
        "gw_bc_s3_bucket": "kfbmic-edr-np-gw-dev-landing-zone-bc-01-bucket",
        "gw_cc_dir": "NOT AVAILABLE",
        "gw_cc_s3_bucket": "kfbmic-edr-np-gw-dev-landing-zone-cc-01-bucket",
        "gw_cm_dir": "NOT AVAILABLE",
        "gw_cm_s3_bucket": "kfbmic-edr-np-gw-dev-landing-zone-cm-01-bucket",
        "gw_pc_dir": "NOT AVAILABLE",
        "gw_pc_s3_bucket": "kfbmic-edr-np-gw-dev-landing-zone-pc-01-bucket",
        "kfb_s3_bc_bucket": "kfbmic-edr-np-gw-dev-landing-zone-bc-01-bucket",
        "kfb_s3_cc_bucket": "kfbmic-edr-np-gw-dev-landing-zone-cc-01-bucket",
        "kfb_s3_cm_bucket": "kfbmic-edr-np-gw-dev-landing-zone-cm-01-bucket",
        "kfb_s3_pc_bucket": "kfbmic-edr-np-gw-dev-landing-zone-pc-01-bucket",
        "secret_name": "kfbmic-edr-np-dev-glue-snowflake-service-secret",
        "sns_topic_name": "kfbmic-edr-np-dev-snstopic",
        "target_system": "kfb",
    }

    core_center_config = get_core_center_config(args)

    main(
        logger=_logger,
        job_name=args["JOB_NAME"],
        secret_name=args["secret_name"],
        sns_topic_name=args["sns_topic_name"],
        target_system=args["target_system"],
        core_center=args["core_center"],
        gw_cda_bucket=core_center_config["gw_cda_bucket"],
        gw_s3_dir=core_center_config["gw_s3_dir"],
        kfb_s3_bucket=core_center_config["kfb_s3_bucket"],
        database_name=args["database_name"],
        aws_profile="nonprod",
    )
