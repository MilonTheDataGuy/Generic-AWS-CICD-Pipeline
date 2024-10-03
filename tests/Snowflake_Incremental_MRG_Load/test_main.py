import os
import pytest
from boto3.session import Session
from structlog import get_logger
from scripts.Snowflake_Incremental_MRG_Load import main


@pytest.fixture(scope="module")
def session() -> Session:
    if "REQUESTS_CA_BUNDLE" in os.environ:
        os.environ.pop("REQUESTS_CA_BUNDLE")
    return Session(profile_name="nonprod", region_name="us-east-1")


logger = get_logger()


def test_main(session):
    main(
        logger=logger,
        job_name="Snowflake_Incremental_MRG_Load_Zane_Version",
        secret_name="kfbmic-edr-np-dev-glue-snowflake-service-secret",
        batch_id="-1234",
        core_centers="pc,bc,cc,cm",
        sns_topic_name="kfbmic-edr-np-dev-snstopic",
        aws_profile="nonprod",
    )
