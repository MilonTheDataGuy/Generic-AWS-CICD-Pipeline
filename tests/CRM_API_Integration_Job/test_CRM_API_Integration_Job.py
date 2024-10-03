import dataclasses
import logging
import os
import asyncio
import boto3
import pytest
import pytest_asyncio
from snowflake.snowpark import Session
from scripts.CRM_API_Integration_Job import Client, Handler, SOURCES, Source

logger = logging.getLogger(__name__)


def get_test_source(table_name: str) -> Source:
    source_dict = dataclasses.asdict(SOURCES[table_name])
    new_source_query = source_dict["source_query"].replace(
        "WHERE SUBMISSION_STATUS NOT IN ('SUCCESS')", ""
    )
    new_source_query = new_source_query.replace(
        "AND SUBMISSION_ATTEMPT_COUNT < MAX_SUBMISSION_ATTEMPT_COUNT", ""
    )
    source_dict["source_query"] = new_source_query
    return Source(**source_dict)


@pytest.fixture(scope="session")
def event_loop():
    return asyncio.get_event_loop()


@pytest_asyncio.fixture(scope="session")
async def client() -> Client:
    session = boto3.Session(
        profile_name="nonprod",
        region_name="us-east-1",
    )
    client = session.client(service_name="secretsmanager")
    return await Client.factory(
        logger=logger,
        base_url="https://kyfb.cvsapphire.com",
        root_endpoint="cvapi",
        secret_name="kfbmic-edr-np-test-crm-secret",
        secretmanager_client=client,
    )


@pytest.fixture(scope="session")
def snowflake_session() -> Session:
    if "REQUESTS_CA_BUNDLE" in os.environ:
        os.environ.pop("REQUESTS_CA_BUNDLE")
    return Session.builder.configs(
        {
            "account": "kkb74824.us-east-1.privatelink",
            "user": os.environ["sf_dev_db_userid"],
            "password": os.environ["sf_dev_db_pw"],
            "role": "DATA_ENGINEER__B_ROLE",
            "warehouse": "WH_DATA_ENGNR_MD",
            "database": "KFB_ECDP_DEV",
        }
    ).create()


@pytest_asyncio.fixture(scope="session")
async def handler(client: Client, snowflake_session: Session) -> Handler:
    post_queue = asyncio.Queue()
    update_queue = asyncio.Queue()
    logger = logging.getLogger(__name__)

    return Handler(
        post_queue=post_queue,
        update_queue=update_queue,
        session=snowflake_session,
        client=client,
        logger=logger,
    )


@pytest.mark.asyncio
async def test_member_update(client: Client):
    data = [
        {
            "ORGCD": "0016565845",
            "ORGALTCD": "0003244545",
            "ORGNAME": "Taylorsville Lake Marina",
            "ADDRESS1": "17308 Shakes Creek Dr",
            "ADDRESS2": " ",
            "CITY": "Fisherville",
            "STATECD": "KY",
            "ZIP": "400237797",
            "COUNTRY": None,
            "HOMEPHONE": "502-0799318",
            "CUSTOMERCDLINK": "0016565844",
            "CONTACTNAME": "BARRETT T JENNIFER",
            "PREFIX": "MR",
            "FIRSTNAME": "BARRETT",
            "LASTNAME": "JENNIFER",
            "SUFFIX": "JR",
            "WORKPHONE": "502-3642881",
            "WORKPHONEEXT": "",
            "MOBILEPHONE": "502-1268130",
            "EMAIL": "KFBEmail34370@kyfb.com",
            "ISMEMBERFLG": "Y",
            "STATUSSTT": "Inactive",
            "ORGTYPE": "NFB",
            "JOINDATE": "2023-09-13 00:00:00.000",
            "FIRSTJOINDATE": "2023-09-13 00:00:00.000",
            "TERMINATIONDATE": "9999-12-31 00:00:00.000",
            "CONTACTSTT": "Mail",
            "JOURNALFLG": "",
            "HASPETSFLG": "",
            "EMAILFORMAT": "Text",
            "CHAPTERID": None,
            "LINKEDCHAPTERCD": "",
            "IDENTITY": 1,
        },
        {
            "ORGCD": "0016566067",
            "ORGALTCD": "0003244622",
            "ORGNAME": "Lake Monroe Boat Rental Inc",
            "ADDRESS1": "2938 Shopes Creek Rd",
            "ADDRESS2": " ",
            "CITY": "Ashland",
            "STATECD": "KY",
            "ZIP": "411029753",
            "COUNTRY": None,
            "HOMEPHONE": "502-2734591",
            "CUSTOMERCDLINK": "0016566065",
            "CONTACTNAME": "BUCHANAN T RICHARD",
            "PREFIX": "MRS",
            "FIRSTNAME": "BUCHANAN",
            "LASTNAME": "RICHARD",
            "SUFFIX": "JR",
            "WORKPHONE": "502-8874529",
            "WORKPHONEEXT": "",
            "MOBILEPHONE": "502-9665131",
            "EMAIL": "KFBEmail20812@kyfb.com",
            "ISMEMBERFLG": "Y",
            "STATUSSTT": "Inactive",
            "ORGTYPE": "FBM",
            "JOINDATE": "2023-09-13 00:00:00.000",
            "FIRSTJOINDATE": "2023-09-13 00:00:00.000",
            "TERMINATIONDATE": "9999-12-31 00:00:00.000",
            "CONTACTSTT": "Mail",
            "JOURNALFLG": "",
            "HASPETSFLG": "",
            "EMAILFORMAT": "Text",
            "CHAPTERID": None,
            "LINKEDCHAPTERCD": "",
            "IDENTITY": 2,
        },
        {
            "ORGCD": "0016568861",
            "ORGALTCD": "0003244810",
            "ORGNAME": "The Fangs Wiki Company Inc",
            "ADDRESS1": "938 Fangs World Drive",
            "ADDRESS2": " ",
            "CITY": "Las Vegas",
            "STATECD": "CA",
            "ZIP": "48027",
            "COUNTRY": None,
            "HOMEPHONE": "",
            "CUSTOMERCDLINK": None,
            "CONTACTNAME": "TEST 1",
            "PREFIX": None,
            "FIRSTNAME": "",
            "LASTNAME": "",
            "SUFFIX": "",
            "WORKPHONE": "",
            "WORKPHONEEXT": "",
            "MOBILEPHONE": "",
            "EMAIL": "fangs@gmail.com",
            "ISMEMBERFLG": "Y",
            "STATUSSTT": "Inactive",
            "ORGTYPE": "NFB",
            "JOINDATE": "2023-11-08 00:00:00.000",
            "FIRSTJOINDATE": "2023-11-08 00:00:00.000",
            "TERMINATIONDATE": "9999-12-31 00:00:00.000",
            "CONTACTSTT": "Mail",
            "JOURNALFLG": "N",
            "HASPETSFLG": "N",
            "EMAILFORMAT": "Text",
            "CHAPTERID": None,
            "LINKEDCHAPTERCD": "",
            "IDENTITY": 10,
        },
    ]
    async with client as client:
        response = await client.post(endpoint="memberUpdate", data=data)
        assert response["success"] is True
        assert response["message"] == "The data has been loaded"


@pytest.mark.asyncio
async def test_organization_update(client):
    data = [
        {
            "ORGCD": "9876",
            "ORGNAME": "1020 York LLC",
            "ORGTYPE": "Associate",
            "CITY": "Bowling Green",
            "STATECD": "KY",
        },
        {
            "ORGCD": "9877",
            "ORGNAME": "1234 Lex LLC",
            "ORGTYPE": "Associate",
            "CITY": "Bowling Green",
            "STATECD": "KY",
        },
        {
            "ORGCD": "9878",
            "ORGNAME": "6345 Ray llc",
            "ORGTYPE": "Associate",
            "CITY": "Bowling Green",
            "STATECD": "KY",
        },
    ]
    async with client as client:
        response = await client.post(endpoint="organizationUpdate", data=data)
        assert response["success"] is True
        assert response["message"] == "The data has been loaded"


@pytest.mark.asyncio
async def test_subscription_update(client):
    data = [
        {
            "SUBSCRIPTIONNAME": "ASSOCIATE",
            "CUSTOMERCD": "100000002",
            "CUSTOMERTYPECD": "I",
            "PAYEETYPECD": "I",
            "PAYEECD": "100000002",
            "PRICEAMT": "100",
            "DUEDATE": "01/10/2024",
            "STARTDATE": "01/10/2024",
            "EXPIRATIONDATE": "01/10/2025",
            "PAIDFLG": "Y",
            "CANCELLEDFLG": "N",
            "CANCELDATE": "10/02/2023",
            "PAIDDATE": "01/10/2024",
            "BILLPERIODNUM": "12",
            "PERIODQTY": "1",
            "ACKNOWLEDGEDFLG": "Y",
            "ACKNOWLEDGEDATE": "01/10/2022",
            "LASTRENEWALDATE": "01/10/2021",
            "STARTDATEORIGINAL": "01/10/1999",
            "ISRENEWED": "Y",
        },
        {
            "SUBSCRIPTIONNAME": "ASSOCIATE",
            "CUSTOMERCD": "100000003",
            "CUSTOMERTYPECD": "I",
            "PAYEETYPECD": "I",
            "PAYEECD": "100000003",
            "PRICEAMT": "100",
            "DUEDATE": "01/10/2024",
            "STARTDATE": "01/10/2024",
            "EXPIRATIONDATE": "01/10/2025",
            "PAIDFLG": "Y",
            "CANCELLEDFLG": "N",
            "CANCELDATE": "10/02/2023",
            "PAIDDATE": "01/10/2024",
            "BILLPERIODNUM": "12",
            "PERIODQTY": "1",
            "ACKNOWLEDGEDFLG": "Y",
            "ACKNOWLEDGEDATE": "01/10/2022",
            "LASTRENEWALDATE": "01/10/2021",
            "STARTDATEORIGINAL": "01/10/1999",
            "ISRENEWED": "Y",
        },
    ]
    async with client as client:
        response = await client.post(endpoint="subscriptionUpdate", data=data)
        assert response["success"] is True
        assert response["message"] == "The data has been loaded"


@pytest.mark.asyncio
async def test_check_process(client):
    async with client as client:
        response = await client.get(endpoint="checkProcess")
        assert "results" in response
        assert "rows" in response


@pytest.mark.asyncio
async def test_process_member(handler: Handler):
    source = get_test_source(table_name="MEMBER")
    await handler.process(source=source)


@pytest.mark.asyncio
async def test_process_organization(handler: Handler):
    source = get_test_source(table_name="ORGANIZATION")
    await handler.process(source=source)


@pytest.mark.asyncio
async def test_process_subs_all(handler: Handler):
    source = get_test_source(table_name="SUBS_ALL")
    await handler.process(source=source)
