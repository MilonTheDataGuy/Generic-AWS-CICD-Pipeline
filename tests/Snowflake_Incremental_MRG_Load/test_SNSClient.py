from unittest.mock import patch
import pytest
from structlog import get_logger
from structlog.testing import capture_logs
from botocore.stub import Stubber
from boto3.session import Session
from botocore.client import BaseClient
from scripts.Snowflake_Incremental_MRG_Load import SNSClient


@pytest.fixture(scope="module")
def session() -> Session:
    return Session(profile_name="nonprod", region_name="us-east-1")


@pytest.fixture(scope="module")
def s3_client(session) -> BaseClient:
    return session.client("sns")


logger = get_logger()


class TestSNSClient:
    def test_get_topic_arn_happy_path(self, s3_client):
        stubber = Stubber(s3_client)
        stubber.add_response(
            "list_topics",
            {
                "Topics": [
                    {"TopicArn": "arn:aws:sns:us-east-1:124567890:TopicArn1"},
                    {"TopicArn": "arn:aws:sns:us-east-1:124567890:TopicArn2"},
                    {"TopicArn": "arn:aws:sns:us-east-1:124567890:TopicArn3"},
                ],
                "NextToken": "string",
            },
            {},
        )

        with stubber:
            topic_arn = SNSClient.get_topic_arn(
                client=s3_client,
                topic_name="TopicArn3",
                logger=logger,
            )
            assert topic_arn == "arn:aws:sns:us-east-1:124567890:TopicArn3"

    def test_get_topic_not_found(self, s3_client):
        stubber = Stubber(s3_client)
        stubber.add_response(
            "list_topics",
            {
                "Topics": [
                    {"TopicArn": "arn:aws:sns:us-east-1:124567890:TopicArn1"},
                    {"TopicArn": "arn:aws:sns:us-east-1:124567890:TopicArn2"},
                    {"TopicArn": "arn:aws:sns:us-east-1:124567890:TopicArn3"},
                ],
                "NextToken": "string",
            },
            {},
        )

        with capture_logs() as cap_logs:
            with stubber:
                with pytest.raises(ValueError):
                    SNSClient.get_topic_arn(
                        client=s3_client,
                        topic_name="TopicArn4",
                        logger=logger,
                    )
            assert any(
                log["event"] == "SNS Topic Not Found: TopicArn4" for log in cap_logs
            )

    def test_get_topic_multiple_found(self, s3_client):
        stubber = Stubber(s3_client)
        stubber.add_response(
            "list_topics",
            {
                "Topics": [
                    {"TopicArn": "arn:aws:sns:us-east-1:124567890:TopicArn1"},
                    {"TopicArn": "arn:aws:sns:us-east-1:124567890:TopicArn3"},
                    {"TopicArn": "arn:aws:sns:us-east-1:124567890:TopicArn3"},
                ],
                "NextToken": "string",
            },
            {},
        )

        with capture_logs() as cap_logs:
            with stubber:
                with pytest.raises(ValueError):
                    SNSClient.get_topic_arn(
                        client=s3_client,
                        topic_name="TopicArn3",
                        logger=logger,
                    )
            assert any(
                "Multiple SNS Topic Matches Found" in log["event"] for log in cap_logs
            )

    @patch.object(SNSClient, "get_topic_arn", return_value="my-arn")
    def test_factory_happy_path(self, _, session):
        with capture_logs() as cap_logs:
            SNSClient.factory(
                session=session,
                topic_name="GW_CDA_Ingestion_Kfb",
                logger=logger,
            )
            assert all(
                "topic_arn" in log.keys() and log["topic_arn"] == "my-arn"
                for log in cap_logs
            )

    def test_send_notification(self, s3_client, session):
        sns_client = SNSClient(
            _client=s3_client,
            _topic_arn="my-arn",
            _logger=logger,
        )

        stubber = Stubber(s3_client)
        stubber.add_response(
            "publish",
            {"MessageId": "string", "SequenceNumber": "string"},
            {"Message": "my message", "Subject": "my subject", "TopicArn": "my-arn"},
        )

        with capture_logs() as cap_logs:
            with stubber:
                sns_client.send_notification(subject="my subject", message="my message")
                assert all(
                    "subject" in log.keys()
                    and log["subject"] == "my subject"
                    and "sns_message" in log.keys()
                    and log["sns_message"] == "my message"
                    for log in cap_logs
                )
