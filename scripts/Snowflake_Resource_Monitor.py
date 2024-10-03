from __future__ import annotations

import dataclasses
import json
import logging
import sys
from datetime import datetime, timedelta
from decimal import Decimal
from textwrap import dedent
from typing import ClassVar

import structlog
from boto3.session import Session as AWSSession
from botocore.client import BaseClient
from snowflake.snowpark import Session as SFSession

try:
    GLUE_JOB = True
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
    GLUE_JOB = False

for noisy_logger in [
    "snowflake",
    "boto3",
    "botocore",
    "urllib3",
]:
    logging.getLogger(noisy_logger).setLevel(logging.WARNING)


def get_glue_logger(context: GlueContext) -> structlog.BoundLogger:
    # noinspection PyTypeChecker
    return structlog.wrap_logger(
        context.get_logger(),
        processors=[
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.contextvars.merge_contextvars,
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.dict_tracebacks,
            structlog.dev.ConsoleRenderer(colors=False),
        ],
        wrapper_class=structlog.BoundLogger,
    )


def get_snowpark_session(secretsmanager_client, secret_name: str) -> SFSession:
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
            "schema": "ETL_CTRL",
            "role": secrets_value["SNOWFLAKE_ROLE"],
        }
    ).create()


@dataclasses.dataclass
class SNSClient:
    _client: BaseClient
    _topic_arn: str
    _logger: structlog.BoundLogger

    @staticmethod
    def get_topic_arn(
        client: BaseClient,
        topic_name: str,
        logger: structlog.BoundLogger,
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
            logger.exception(exception)
            raise ValueError(exception)

        if len(topic_arns) > 1:
            exception = f"Multiple SNS Topic Matches Found: {topic_name}\n{', '.join(topic_arns)}"
            logger.exception(exception)
            raise ValueError(exception)

        return topic_arns[0]

    @classmethod
    def factory(
        cls,
        session: AWSSession,
        topic_name: str,
        logger: structlog.BoundLogger,
    ) -> SNSClient:
        client = session.client("sns")
        topic_arn = cls.get_topic_arn(
            client=client, topic_name=topic_name, logger=logger
        )
        logger = logger.bind(topic_arn=topic_arn)
        logger.info("returning SNSClient")
        return cls(_client=client, _topic_arn=topic_arn, _logger=logger)

    def send_notification(self, subject: str, message: str):
        self._logger.info(
            "publishing SNS message", subject=subject, sns_message=message
        )
        self._client.publish(
            TopicArn=self._topic_arn,
            Message=dedent(str(message)),
            Subject=str(subject),
        )


TWOPLACES = Decimal(10) ** -2  # same as Decimal('0.01')


def get_percentage_decimal(pct: str) -> Decimal | None:
    if pct is None or pct == "":
        return None
    try:
        suspend_at_int = Decimal(pct.strip().replace("%", ""))
        return suspend_at_int / 100
    except:
        raise Exception(f"Failed to convert percentage to int: {pct}")


@dataclasses.dataclass
class ResourceMonitor:
    width: ClassVar[int] = 50
    fill_char: ClassVar[str] = "▓"
    empty_char: ClassVar[str] = "░"
    monitor_name: str
    credit_quota: Decimal
    used_credits: Decimal
    remaining_credits: Decimal
    level: str
    frequency: str
    start_time: datetime
    end_time: datetime
    notify_at: str
    suspend_at_pct: Decimal | None
    suspend_immediately_at_pct: Decimal | None
    created_on: datetime
    owner: str
    comment: str
    notify_users: str

    @property
    def frequency_duration(self) -> timedelta:
        if self.frequency == "WEEKLY":
            return timedelta(weeks=1)
        elif self.frequency == "DAILY":
            return timedelta(days=1)
        else:
            raise Exception(f"unhandled frequency: {self.frequency}")

    @property
    def time_from_start(self) -> timedelta:
        total_time_from_start = (
            datetime.now(tz=self.start_time.tzinfo) - self.start_time
        )
        frequency_multiple = total_time_from_start // self.frequency_duration
        result = total_time_from_start - (frequency_multiple * self.frequency_duration)
        return result

    @property
    def percent_from_start(self) -> Decimal:
        return Decimal(self.time_from_start / self.frequency_duration)

    @property
    def used_percent(self) -> Decimal:
        return self.used_credits / self.credit_quota

    @property
    def suspend_at_credits(self) -> Decimal | None:
        if self.suspend_at_pct is None:
            return None
        return self.credit_quota * self.suspend_at_pct

    @property
    def suspend_immediately_at_credits(self) -> Decimal | None:
        if self.suspend_immediately_at_pct is None:
            return None
        return self.credit_quota * self.suspend_immediately_at_pct

    def get_progress_bar(self, fill_percentage: Decimal) -> str:
        fill_count = round(fill_percentage * self.width)
        bar = list((self.fill_char * fill_count).ljust(self.width, self.empty_char))

        if self.suspend_at_pct is not None and self.suspend_at_pct != 0:
            suspend_at_place = int(self.suspend_at_pct * self.width)
            if suspend_at_place < len(bar):
                bar[suspend_at_place] = "⚠️"
            elif suspend_at_place == len(bar):
                bar.append("⚠️")

        if (
            self.suspend_immediately_at_pct is not None
            and self.suspend_immediately_at_pct != 0
        ):
            suspend_immediately_at_place = int(
                self.suspend_immediately_at_pct * self.width
            )
            if suspend_immediately_at_place < len(bar):
                bar[suspend_immediately_at_place] = "🛑"
            elif suspend_immediately_at_place == len(bar):
                bar.append("🛑")

        return "".join(bar)

    @property
    def time_progress_bar(self) -> str:
        return self.get_progress_bar(fill_percentage=self.percent_from_start)

    @property
    def credit_progress_bar(self) -> str:
        return self.get_progress_bar(fill_percentage=self.used_percent)

    @property
    def status(self) -> str:
        status_contents = {
            "Resource Monitor Name": self.monitor_name,
            "Monitor Frequency": self.frequency,
            "Frequency Start Time": self.start_time,
            "Credit Limit": self.credit_quota.quantize(TWOPLACES),
            "Credits Consumed": self.used_credits.quantize(TWOPLACES),
        }

        if self.suspend_at_credits is not None:
            status_contents["Suspend At"] = self.suspend_at_credits.quantize(TWOPLACES)

        if self.suspend_immediately_at_credits is not None:
            status_contents["Suspend Immediately At"] = (
                self.suspend_immediately_at_credits.quantize(TWOPLACES)
            )

        status_list = [
            f"{header}: {value}" for header, value in status_contents.items()
        ]
        status_list.extend(["Time:", self.time_progress_bar])
        status_list.extend(["Credits:", self.credit_progress_bar])
        status_list.append("-" * self.width)
        return "\n".join(status_list)

    @classmethod
    def factory(cls, **kwargs) -> "ResourceMonitor":
        suspend_at_pct = get_percentage_decimal(pct=kwargs.pop("suspend_at"))
        suspend_immediately_at_pct = get_percentage_decimal(
            pct=kwargs.pop("suspend_immediately_at")
        )
        credit_quota = Decimal(kwargs.pop("credit_quota"))
        used_credits = Decimal(kwargs.pop("used_credits"))
        remaining_credits = Decimal(kwargs.pop("remaining_credits"))

        return cls(
            monitor_name=kwargs.pop("name"),
            credit_quota=credit_quota,
            used_credits=used_credits,
            remaining_credits=remaining_credits,
            suspend_at_pct=suspend_at_pct,
            suspend_immediately_at_pct=suspend_immediately_at_pct,
            **kwargs,
        )


def main(
    logger: structlog.BoundLogger,
    aws_account_name: str,
    secret_name: str,
    sns_topic_name: str,
    aws_profile: str | None = None,
):
    aws_session = AWSSession(profile_name=aws_profile, region_name="us-east-1")
    snowpark_session = get_snowpark_session(
        secretsmanager_client=aws_session.client(service_name="secretsmanager"),
        secret_name=secret_name,
    )
    sns_client = SNSClient.factory(
        session=aws_session, topic_name=sns_topic_name, logger=logger
    )
    df = snowpark_session.sql("SHOW RESOURCE MONITORS;")
    resource_monitors = [
        ResourceMonitor.factory(**row.as_dict()) for row in df.to_local_iterator()
    ]
    all_statuses = "\n\n".join([rm.status for rm in resource_monitors])
    message = f"{aws_account_name} Environment Resource Monitor Status Report:\n\n{all_statuses}"
    sns_client.send_notification(
        subject=f"{aws_account_name}: Resource Monitor Status Report", message=message
    )
    snowpark_session.close()


if __name__ == "__main__":
    # @params: [JOB_NAME]
    args = getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "sns_topic_name",
            "secret_name",
            "aws_account_name",
        ],
    )
    sc = SparkContext()
    glue_context = GlueContext(sc)
    structlogger = get_glue_logger(glue_context)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args["JOB_NAME"] + args["aws_account_name"], args)

    main(
        logger=structlogger,
        secret_name=args["secret_name"],
        sns_topic_name=args["sns_topic_name"],
        aws_account_name=args["aws_account_name"],
    )

    job.commit()
