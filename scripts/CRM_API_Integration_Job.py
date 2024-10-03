from __future__ import annotations

import asyncio
import json
import logging
import sys
from dataclasses import dataclass
from json import JSONDecodeError

import aiohttp
import boto3
from aiohttp import ClientSession
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col

# noinspection PyBroadException
try:
    GLUE_JOB = True
    # noinspection PyUnresolvedReferences
    from awsglue.context import GlueContext

    # noinspection PyUnresolvedReferences
    from awsglue.job import Job

    # noinspection PyUnresolvedReferences
    from awsglue.transforms import *

    # noinspection PyUnresolvedReferences
    from awsglue.utils import getResolvedOptions

    # noinspection PyUnresolvedReferences
    from pyspark.context import SparkContext
except ModuleNotFoundError:
    logging.basicConfig(level=logging.DEBUG)

"""
Open questions: 
- Columns we have that they don't. Can we remove them? 

Should I poll it in the same Glue job and just keep the job alive? 
Or, will it take long enough that we should get the status some other way? 
should I have some other How to poll checkProcess for completion? 
How long should I wait for a checkProcess result before I complain?
"""


@dataclass(frozen=True)
class Source:
    schema_name: str
    table_name: str
    identity_column_name: str
    source_query: str
    endpoint: str


SOURCES: dict[str, Source] = {
    "MEMBER": Source(
        schema_name="MRG_CRM",
        table_name="MEMBER",
        identity_column_name="IDENTITY",
        source_query="""
            -- noinspection SqlResolveForFile
            -- noinspection SqlNoDataSourceInspectionForFile
            SELECT CUSTOMERCD,
                   CUSTOMERALTCD,
                   PREFIX,
                   FIRSTNAME,
                   MIDDLEINITIAL,
                   MIDDLENAME,
                   LASTNAME,
                   SUFFIX,
                   BILLTOSTT,
                   ISMEMBERFLG,
                   CUSTOMERTYPE,
                   CUSTOMERCLASSSTT,
                   STATUSSTT,
                   JOINDATE,
                   FIRSTJOINDATE,
                   TERMINATIONDATE,
                   CHAPTERID,
                   ALTCHAPTERCD,
                   ADDRESSTYPE,
                   ADDRESS1,
                   ADDRESS2,
                   ADDRESS3,
                   CITY,
                   STATECD,
                   ZIP,
                   COUNTRY,
                   COUNTY,
                   PHONEPREFTYPE,
                   WORKPHONE,
                   WORKPHONEEXT,
                   HOMEPHONE,
                   MOBILEPHONE,
                   FAXPHONE,
                   ALTFAXPHONE,
                   ALTPHONE,
                   ALTPHONEEXT,
                   EMAIL,
                   ALTEMAIL,
                   EMAILFORMAT,
                   CONTACTSTT,
                   NOCALLFLG,
                   NOMAILFLG,
                   NOEMAILFLG,
                   NOFAXFLG,
                   NOSELLFLG,
                   GENDER,
                   BIRTHDATE,
                   DECEASEDDATE,
                   MARITALSTT,
                   JOURNALFLG,
                   HASPETSFLG,
                   IDENTITY
            FROM MRG_CRM.MEMBER
            WHERE SUBMISSION_STATUS NOT IN ('SUCCESS')
              AND SUBMISSION_ATTEMPT_COUNT < MAX_SUBMISSION_ATTEMPT_COUNT
        """,
        endpoint="memberUpdate",
    ),
    "ORGANIZATION": Source(
        schema_name="MRG_CRM",
        table_name="ORGANIZATION",
        identity_column_name="IDENTITY",
        source_query="""
            -- noinspection SqlResolveForFile
            -- noinspection SqlNoDataSourceInspectionForFile
            SELECT ORGCD,
                   ORGALTCD,
                   ORGNAME,
                   ADDRESS1,
                   ADDRESS2,
                   CITY,
                   STATECD,
                   ZIP,
                   COUNTRY,
                   HOMEPHONE,
                   CUSTOMERCDLINK,
                   CONTACTNAME,
                   PREFIX,
                   FIRSTNAME,
                   LASTNAME,
                   SUFFIX,
                   WORKPHONE,
                   WORKPHONEEXT,
                   MOBILEPHONE,
                   EMAIL,
                   ISMEMBERFLG,
                   STATUSSTT,
                   ORGTYPE,
                   JOINDATE,
                   FIRSTJOINDATE,
                   TERMINATIONDATE,
                   CONTACTSTT,
                   JOURNALFLG,
                   NONDISCFLG,
                   CHAPTERID,
                   LINKEDCHAPTERCD,
                   BATCHID,
                   IDENTITY
            FROM MRG_CRM.ORGANIZATION
            WHERE SUBMISSION_STATUS NOT IN ('SUCCESS')
              AND SUBMISSION_ATTEMPT_COUNT < MAX_SUBMISSION_ATTEMPT_COUNT
        """,
        endpoint="organizationUpdate",
    ),
    "SUBS_ALL": Source(
        schema_name="MRG_CRM",
        table_name="SUBS_ALL",
        identity_column_name="IDENTITY",
        source_query="""
            -- noinspection SqlResolveForFile
            -- noinspection SqlNoDataSourceInspectionForFile
            SELECT SUBSCRIPTIONNUM,
                   CANCELFLG,
                   CANCELDATE,
                   PAIDFLG,
                   IDENTITY
            FROM MRG_CRM.SUBS_ALL
            WHERE SUBMISSION_STATUS NOT IN ('SUCCESS')
              AND SUBMISSION_ATTEMPT_COUNT < MAX_SUBMISSION_ATTEMPT_COUNT
        """,
        endpoint="subscriptionUpdate",
    ),
}


@dataclass(frozen=True)
class PostTask:
    source: Source
    payload: list[dict]


@dataclass(frozen=True)
class UpdateTask(PostTask):
    status: str


@dataclass(frozen=True)
class TableStatus:
    schema_name: str
    table_name: str
    status: str


def get_snowflake_session(
    secretsmanager_client, secret_name: str, database_name: str
) -> Session:
    get_secret_value_response = secretsmanager_client.get_secret_value(
        SecretId=secret_name
    )
    secrets_value = json.loads(get_secret_value_response["SecretString"])
    session = Session.builder.configs(
        {
            "account": secrets_value["SNOWFLAKE_ACCOUNT"],
            "user": secrets_value["SNOWFLAKE_USER"],
            "password": secrets_value["SNOWFLAKE_PASSWORD"],
            "role": secrets_value["SNOWFLAKE_ROLE"],
            "warehouse": secrets_value["SNOWFLAKE_WAREHOUSE"],
            "database": database_name,
        }
    ).create()
    return session


class Client:
    base_url: str
    root_endpoint: str
    client_id: str
    client_secret: str
    auth_token: str
    logger: logging.Logger
    _session: ClientSession | None = None

    def __init__(
        self,
        base_url: str,
        root_endpoint: str,
        client_id: str,
        client_secret: str,
        auth_token: str,
        logger: logging.Logger,
    ):
        self.base_url = base_url
        self.root_endpoint = root_endpoint
        self.client_id = client_id
        self.client_secret = client_secret
        self.auth_header = {"Authorization": f"oauth_token={auth_token}"}
        self.logger = logger

    @property
    def headers(self) -> dict:
        return {**self.auth_header}

    async def __aenter__(self):
        self._session = ClientSession(base_url=self.base_url, headers=self.headers)
        return self

    async def __aexit__(self, *err):
        await self._session.close()
        self._session = None

    async def post(self, endpoint: str, data: object) -> dict:
        async with self._session.post(
            url=f"/{self.root_endpoint}/{endpoint}", data=json.dumps(data)
        ) as resp:
            resp.raise_for_status()
            # The API returns a 200 in the event of an invalid payload.
            try:
                return await resp.json()
            except JSONDecodeError as e:
                resp = await resp.text()
                self.logger.error(str(resp))
                raise Exception(resp) from e

    async def get(self, endpoint: str) -> dict:
        async with self._session.get(url=f"/{self.root_endpoint}/{endpoint}") as resp:
            resp.raise_for_status()
            # The API returns a 200 in the event of an invalid payload.
            try:
                return await resp.json()
            except JSONDecodeError as e:
                resp = await resp.text()
                self.logger.error(str(resp))
                raise Exception(resp) from e

    @staticmethod
    async def get_auth_token(
        base_url: str, root_endpoint: str, client_id: str, client_secret: str
    ) -> str:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{base_url}/{root_endpoint}/authorize/{client_id}/{client_secret}"
            ) as response:
                response.raise_for_status()
                payload = await response.json()
                return payload["access_token"][1:-1]

    @staticmethod
    def get_credentials(secret_name: str, client) -> tuple[str, str]:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        secrets_value = json.loads(get_secret_value_response["SecretString"])
        return (secrets_value["client_id"], secrets_value["client_secret"])

    @classmethod
    async def factory(
        cls,
        logger: logging.Logger,
        base_url: str,
        root_endpoint: str,
        secret_name: str,
        secretmanager_client,
    ) -> Client:
        client_id, client_secret = cls.get_credentials(
            secret_name=secret_name, client=secretmanager_client
        )
        auth_token = await cls.get_auth_token(
            base_url=base_url,
            root_endpoint=root_endpoint,
            client_id=client_id,
            client_secret=client_secret,
        )
        return cls(
            base_url=base_url,
            root_endpoint=root_endpoint,
            client_id=client_id,
            client_secret=client_secret,
            auth_token=auth_token,
            logger=logger,
        )


@dataclass(frozen=True)
class Handler:
    logger: logging.Logger
    post_queue: asyncio.Queue[PostTask]
    update_queue: asyncio.Queue[UpdateTask]
    session: Session
    client: Client

    async def populate_post_queue(self, source: Source) -> None:
        limit = 1000000
        offset = 0
        while True:
            results = (
                self.session.sql(source.source_query)
                .order_by(col(source.identity_column_name))
                .limit(limit, offset=offset)
                .collect()
            )

            if len(results) == 0:
                self.logger.info("All records have been processed")
                return

            self.logger.info(
                f"Sending {len(results)} records from {source.table_name} to post_queue"
            )
            payload = [result.as_dict() for result in results]

            task = PostTask(source=source, payload=payload)
            self.post_queue.put_nowait(task)

            offset += limit

    async def post_worker(self) -> None:
        while True:
            async with self.client as client:
                post_task = await self.post_queue.get()
                # noinspection PyBroadException
                try:
                    self.logger.info(
                        f"Posting {len(post_task.payload)} records from {post_task.source.table_name} "
                        f"to {post_task.source.endpoint}"
                    )

                    payload_sans_identity = [
                        {
                            k: result_dict[k]
                            for k in result_dict
                            if k != post_task.source.identity_column_name
                        }
                        for result_dict in post_task.payload
                    ]

                    response = await client.post(
                        endpoint=post_task.source.endpoint, data=payload_sans_identity
                    )

                    status = "SUCCESS" if response.get("success") else "FAILURE"

                    update_task = UpdateTask(
                        source=post_task.source,
                        payload=post_task.payload,
                        status=status,
                    )

                    self.update_queue.put_nowait(update_task)
                except Exception as e:
                    self.logger.exception(
                        f"Posting {post_task.source.table_name} to {post_task.source.endpoint} failed"
                    )
                    # Since process won't continue until the queue is empty, empty the queue artificially
                    while not self.post_queue.empty():
                        self.post_queue.get_nowait()
                        self.post_queue.task_done()
                    raise e
                finally:
                    self.post_queue.task_done()

    async def update_snowflake_status(self) -> None:
        while True:
            update_task = await self.update_queue.get()
            try:
                identity_values = [
                    str(val[update_task.source.identity_column_name])
                    for val in update_task.payload
                ]
                # noinspection SqlNoDataSourceInspection
                self.session.sql(
                    f"""
                UPDATE {update_task.source.schema_name}.{update_task.source.table_name}
                SET SUBMISSION_STATUS        = '{update_task.status}',
                    SUBMISSION_ATTEMPT_COUNT = SUBMISSION_ATTEMPT_COUNT + 1
                WHERE IDENTITY IN ({', '.join(identity_values)})
                """
                ).collect()
            finally:
                self.update_queue.task_done()

    async def process(self, source: Source):
        populate_post_queue_task = asyncio.create_task(
            self.populate_post_queue(source=source), name="populate_post_queue_task"
        )
        post_worker = asyncio.create_task(self.post_worker(), name="post_worker")
        update_snowflake_status_task = asyncio.create_task(
            self.update_snowflake_status(), name="update_snowflake_status_task"
        )

        await asyncio.gather(populate_post_queue_task)
        self.logger.info("----- Post Queue population completed -----")

        await self.post_queue.join()

        post_worker.cancel()

        # Catch exceptions raised by post_worker
        try:
            await post_worker
        except asyncio.CancelledError:
            pass

        self.logger.info("----- Posting completed -----")

        await self.update_queue.join()

        update_snowflake_status_task.cancel()

        # Catch exceptions raised by post_worker
        try:
            await update_snowflake_status_task
        except asyncio.CancelledError:
            pass


async def main(
    logger: logging.Logger,
    source: Source,
    aws_region: str,
    euclid_secret_name: str,
    snowflake_glue_service_user_secret_name: str,
    base_url: str,
    root_endpoint: str,
    database_name: str,
):
    # Create a Secrets Manager client
    client = boto3.client(service_name="secretsmanager", region_name=aws_region)
    session = get_snowflake_session(
        secretsmanager_client=client,
        secret_name=snowflake_glue_service_user_secret_name,
        database_name=database_name,
    )

    client = await Client.factory(
        logger=logger,
        base_url=base_url,
        root_endpoint=root_endpoint,
        secret_name=euclid_secret_name,
        secretmanager_client=client,
    )

    post_queue = asyncio.Queue()
    update_queue = asyncio.Queue()

    handler = Handler(
        post_queue=post_queue,
        update_queue=update_queue,
        session=session,
        client=client,
        logger=logger,
    )

    await handler.process(source=source)


if __name__ == "__main__":
    # @params: [JOB_NAME]
    args = getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "glue_sf_secret",
            "sns_topic_name",  # TODO: Not implemented
            "base_api",
            "sf_table",
            "root_endpoint",
            "euclid_secret",
            "database_name",
        ],
    )

    # noinspection SqlNoDataSourceInspection,SqlResolve
    src = SOURCES[args["sf_table"]]

    sc = SparkContext()
    glue_context = GlueContext(sc)
    glue_logger = glue_context.get_logger()
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args["JOB_NAME"] + args["sf_table"], args)

    loop = asyncio.get_event_loop()
    loop.set_debug(True)
    try:
        loop.run_until_complete(
            main(
                logger=glue_logger,
                source=src,
                aws_region="us-east-1",  # TODO: Consider making this a parameter
                euclid_secret_name=args["euclid_secret"],
                snowflake_glue_service_user_secret_name=args["glue_sf_secret"],
                base_url=args["base_api"],
                root_endpoint=args["root_endpoint"],
                database_name=args["database_name"],
            )
        )
    finally:
        loop.close()

    # Finish the Glue job
    job.commit()
