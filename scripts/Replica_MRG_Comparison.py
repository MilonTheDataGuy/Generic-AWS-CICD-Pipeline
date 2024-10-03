from __future__ import annotations

import dataclasses
import json
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from textwrap import dedent
from typing import Any

import pandas as pd
from psycopg2.pool import ThreadedConnectionPool

from boto3 import Session
from botocore.client import BaseClient
from snowflake.snowpark import Session as SFSession, DataFrame as SFDataFrame, Row

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


class TableException(Exception):
    schema_name: str
    table_name: str
    exclusion_reason: str | None
    cause: Exception | None

    def __init__(
        self,
        schema_name: str,
        table_name: str,
        exclusion_reason: str | None = None,
        cause: Exception | None = None,
        *args,
    ):
        self.schema_name = schema_name
        self.table_name = table_name
        self.exclusion_reason = exclusion_reason
        self.cause = cause
        super().__init__(*args)

    @property
    def formatted(self) -> str:
        fmt = f"Schema Name: {self.schema_name}\n" f"Table Name: {self.table_name}\n"
        if self.exclusion_reason:
            fmt = fmt + f"Variance: {self.exclusion_reason}\n"
        else:
            fmt = fmt + f"Exception: {str(self.cause).splitlines()[-1]}\n"
        return fmt


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


def get_postgres_connection_pool(
    core_center: str, secretsmanager_client, secret_name: str
) -> Any:
    get_secret_value_response = secretsmanager_client.get_secret_value(
        SecretId=secret_name
    )
    secrets_value = json.loads(get_secret_value_response["SecretString"])
    cc_config = secrets_value["CORE_CENTERS"].get(core_center.lower())

    if cc_config is None:
        raise Exception(f"{core_center} credentials not fund in {secret_name}")

    return ThreadedConnectionPool(
        minconn=1,
        maxconn=30,
        database=cc_config["DATABASE"],
        host=secrets_value["HOST"],
        password=cc_config["PASSWORD"],
        port=secrets_value["PORT"],
        user=cc_config["USERNAME"],
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


REPLICA_ROW_COUNT_QUERY = r"""
    WITH ROWCOUNTS AS (SELECT UPPER(TABLE_CATALOG) AS TABLE_CATALOG,
                              UPPER(TABLE_SCHEMA) AS TABLE_SCHEMA,
                              UPPER(TABLE_NAME) AS TABLE_NAME,
                              (XPATH(
                                      '/row/c/text()',
                                      QUERY_TO_XML(
                                              FORMAT('SELECT COUNT(*) AS C FROM %I.%I', TABLE_SCHEMA, TABLE_NAME),
                                              FALSE,
                                              TRUE,
                                              ''
                                      )
                              ))[1]::TEXT::INT AS ROW_COUNT
                       FROM INFORMATION_SCHEMA.TABLES
                       WHERE UPPER(TABLE_SCHEMA) = 'PUBLIC'
                         AND NOT (
                           TABLE_NAME ILIKE 'bc_tmp%'
                               OR TABLE_NAME ILIKE 'bcst\_%'
                               OR TABLE_NAME ILIKE 'bct\_%'
                               OR TABLE_NAME ILIKE 'bctt\_%'
                               OR TABLE_NAME ILIKE 'cc_TMP%'
                               OR TABLE_NAME ILIKE 'ccst_%'
                               OR TABLE_NAME ILIKE 'cct\_%'
                               OR TABLE_NAME ILIKE 'cctt\_%'
                               OR TABLE_NAME ILIKE 'cc\_%workitem'
                               OR TABLE_NAME ILIKE 'ab_tmp%'
                               OR TABLE_NAME ILIKE 'abst_%'
                               OR TABLE_NAME ILIKE 'abt\_%'
                               OR TABLE_NAME ILIKE 'abtt\_%'
                               OR TABLE_NAME ILIKE 'xx\_%'
                               OR TABLE_NAME ILIKE 'ab\_%workitem'
                               OR TABLE_NAME ILIKE 'pc_tmp%'
                               OR TABLE_NAME ILIKE 'pcst_%'
                               OR TABLE_NAME ILIKE 'pct\_%'
                               OR TABLE_NAME ILIKE 'pctt\_%'
                               OR TABLE_NAME ILIKE 'xx\_%'
                               OR TABLE_NAME ILIKE 'pc\_%workitem'
                               OR UPPER(TABLE_NAME) IN (
                                 'PG_STAT_STATEMENTS',
                                 'BC_ACTIVESCHEDULEDJOB',
                                 'BC_ARRAYDATADIST',
                                 'BC_BATCHPROCESSLEASE',
                                 'BC_BATCHPROCESSLEASEHISTORY',
                                 'BC_BEANVERSIONDATADIST',
                                 'BC_BLOBCOLDATADIST',
                                 'BC_BOOLEANCOLDATADIST',
                                 'BC_BROADCASTBATCH',
                                 'BC_BRVALIDATOR',
                                 'BC_CACHESTATSROLLUPSNAP',
                                 'BC_CLAIMSNAPSHOT',
                                 'BC_CLOBCOLDATADIST',
                                 'BC_CLUSTERMEMBERDATA',
                                 'BC_CONTACTFINGERPRINT',
                                 'BC_CUSTOMDATADISTREQ',
                                 'BC_DATABASEDATADIST',
                                 'BC_DATADISTQUERYEXEC',
                                 'BC_DATAGENINFO',
                                 'BC_DATAGENINFODETAIL',
                                 'BC_DATEANALYSISDATADIST',
                                 'BC_DATEBINNEDDATADIST',
                                 'BC_DATESPANDATADIST',
                                 'BC_DBCONSISTCHECKQUERYEXEC',
                                 'BC_DBCONSISTCHECKRUN',
                                 'BC_DBPERFREPORT',
                                 'BC_DESTINATIONLEASE',
                                 'BC_DESTINATIONLEASEHISTORY',
                                 'BC_DYNAMIC_ASSIGN',
                                 'BC_ENCRYPTEDCOLUMNREGISTRY',
                                 'BC_FORKEYDATADIST',
                                 'BC_FRPTUPGRADE',
                                 'BC_GENERICGROUPCOLUMNDATADIST',
                                 'BC_GENERICGROUPCOUNTDATADIST',
                                 'BC_GENERICGROUPDATADIST',
                                 'BC_HOURANALYSISDATADIST',
                                 'BC_INBOUNDHISTORY',
                                 'BC_INSTRUMENTEDWORKER',
                                 'BC_INSTRUMENTEDWORKERTASK',
                                 'BC_INSTRUMENTEDWORKEXECUTOR',
                                 'BC_LOADCALLBACK',
                                 'BC_LOADCALLBACKRESULT',
                                 'BC_LOADCOMMAND',
                                 'BC_LOADDBSTATISTICSCOMMAND',
                                 'BC_LOADENCRYPTCHUNK',
                                 'BC_LOADENCRYPTTABLE',
                                 'BC_LOADERROR',
                                 'BC_LOADERRORROW',
                                 'BC_LOADEXCLUSION',
                                 'BC_LOADINSERTSELECT',
                                 'BC_LOADINTEGRITYCHECK',
                                 'BC_LOADOPERATION',
                                 'BC_LOADPARAMETER',
                                 'BC_LOADROWCOUNT',
                                 'BC_LOADSTEP',
                                 'BC_LOADUPDATESTATISTICSSELECT',
                                 'BC_MAXKEY',
                                 'BC_MESSAGE',
                                 'BC_MESSAGEHISTORY',
                                 'BC_MESSAGEREQUESTLEASE',
                                 'BC_MESSAGEREQUESTLEASEHISTORY',
                                 'BC_NULLABLECOLUMNDATADIST',
                                 'BC_PLUGINLEASE',
                                 'BC_PLUGINLEASEHISTORY',
                                 'BC_PROCESSHISTORY',
                                 'BC_PROFILERCONFIG',
                                 'BC_ROLLING',
                                 'BC_SCHEDULEDJOB',
                                 'BC_SEQUENCE',
                                 'BC_STANDARDWORKQUEUE',
                                 'BC_SYSTEMPARAMETER',
                                 'BC_TABLEDATADIST',
                                 'BC_TABLEREGISTRY',
                                 'BC_TABLEUPDATESTATSSTATEMENT',
                                 'BC_TEMPNEWPOLICYCMSNPAYABLE',
                                 'BC_TEMPOWNERIDTACCTIDNEWBAL',
                                 'BC_TRANSACTIONID',
                                 'BC_TYPECODECOUNTDATADIST',
                                 'BC_TYPECODEUSAGERPT',
                                 'BC_TYPEKEYDATADIST',
                                 'BC_UNCTMPNEWPLCYCMSNPAYABLE',
                                 'BC_UPGRADEDATAMODELINFO',
                                 'BC_UPGRADEDBSTORAGESET',
                                 'BC_UPGRADEDBSTORAGESETCOLUMNS',
                                 'BC_UPGRADEDBSTORAGESETRESULTS',
                                 'BC_UPGRADEINSTANCE',
                                 'BC_UPGRADEROWCOUNT',
                                 'BC_UPGRADESCHEMAVERSION',
                                 'BC_UPGRADETABLEREGISTRY',
                                 'BC_UPGRADEVTDBMSDUMP',
                                 'BC_WORKFLOWLOG',
                                 'BC_WORKFLOWSTEPSTATS',
                                 'BC_WORKITEMSET',
                                 'BC_WORKQUEUESTATE',
                                 'BC_WORKQUEUEWORKERCONTROL',
                                 'GEOGRAPHY_COLUMNS',
                                 'SPATIAL_REF_SYS',
                                 'CC_ACTIVESCHEDULEDJOB',
                                 'CC_AGGLIMITRPT',
                                 'CC_ARRAYDATADIST',
                                 'CC_ARRAYSIZECNTDD',
                                 'CC_ASSIGNABLEFORKEYDATADIST',
                                 'CC_ASSIGNABLEFORKEYSIZECNTDD',
                                 'CC_BATCHPROCESSLEASE',
                                 'CC_BATCHPROCESSLEASEHISTORY',
                                 'CC_BEANVERSIONDATADIST',
                                 'CC_BLOBCOLDATADIST',
                                 'CC_BOOLEANCOLDATADIST',
                                 'CC_BROADCASTBATCH',
                                 'CC_BRVALIDATOR',
                                 'CC_CACHESTATSROLLUPSNAP',
                                 'CC_CLAIMAGGLIMITRPT',
                                 'CC_CLAIMSNAPSHOT',
                                 'CC_CLOBCOLDATADIST',
                                 'CC_CLUSTERMEMBERDATA',
                                 'CC_CONTACTFINGERPRINT',
                                 'CC_CUSTOMDATADISTREQ',
                                 'CC_CUSTOMDDCOLUMNS',
                                 'CC_CUSTOMDDRESULTS',
                                 'CC_DATABASEDATADIST',
                                 'CC_DATABASEUPDATESTATS',
                                 'CC_DATADISTQUERYEXEC',
                                 'CC_DATAGENINFO',
                                 'CC_DATAGENINFODETAIL',
                                 'CC_DATEANALYSISDATADIST',
                                 'CC_DATEBINNEDDATADIST',
                                 'CC_DATEBINNEDDDDATEBIN',
                                 'CC_DATEBINNEDDDVALUE',
                                 'CC_DATESPANDATADIST',
                                 'CC_DBCONSISTCHECKQUERYEXEC',
                                 'CC_DBCONSISTCHECKRUN',
                                 'CC_DBMSREPORT',
                                 'CC_DBPERFREPORT',
                                 'CC_DESTINATIONLEASE',
                                 'CC_DESTINATIONLEASEHISTORY',
                                 'CC_DYNAMIC_ASSIGN',
                                 'CC_ENCRYPTEDCOLUMNREGISTRY',
                                 'CC_FORKEYDATADIST',
                                 'CC_FRPTUPGRADE',
                                 'CC_GENERICGROUPCOLUMNDATADIST',
                                 'CC_GENERICGROUPCOUNTDATADIST',
                                 'CC_GENERICGROUPDATADIST',
                                 'CC_HOURANALYSISDATADIST',
                                 'CC_INBOUNDHISTORY',
                                 'CC_INSTRUMENTEDWORKER',
                                 'CC_INSTRUMENTEDWORKERTASK',
                                 'CC_INSTRUMENTEDWORKEXECUTOR',
                                 'CC_LOADCALLBACK',
                                 'CC_LOADCALLBACKRESULT',
                                 'CC_LOADCOMMAND',
                                 'CC_LOADDBSTATISTICSCOMMAND',
                                 'CC_LOADENCRYPTCHUNK',
                                 'CC_LOADENCRYPTTABLE',
                                 'CC_LOADERROR',
                                 'CC_LOADERRORROW',
                                 'CC_LOADEXCLUSION',
                                 'CC_LOADINSERTSELECT',
                                 'CC_LOADINTEGRITYCHECK',
                                 'CC_LOADOPERATION',
                                 'CC_LOADPARAMETER',
                                 'CC_LOADROWCOUNT',
                                 'CC_LOADSTEP',
                                 'CC_LOADUPDATESTATISTICSSELECT',
                                 'CC_MAXKEY',
                                 'CC_MESSAGE',
                                 'CC_MESSAGEHISTORY',
                                 'CC_MESSAGEREQUESTLEASE',
                                 'CC_MESSAGEREQUESTLEASEHISTORY',
                                 'CC_NULLABLECOLUMNDATADIST',
                                 'CC_PLUGINLEASE',
                                 'CC_PLUGINLEASEHISTORY',
                                 'CC_PROCESSHISTORY',
                                 'CC_PROFILERCONFIG',
                                 'CC_ROLLING',
                                 'CC_ROLLING_DM',
                                 'CC_SCHEDULEDJOB',
                                 'CC_SEQUENCE',
                                 'CC_STANDARDWORKQUEUE',
                                 'CC_SYSTEMPARAMETER',
                                 'CC_TABLEDATADIST',
                                 'CC_TABLEREGISTRY',
                                 'CC_TABLEUPDATESTATS',
                                 'CC_TABLEUPDATESTATSSTATEMENT',
                                 'CC_TMPAGGLIMITORA',
                                 'CC_TMPAGGLIMITRPT',
                                 'CC_TMPBIITEMINFO',
                                 'CC_TMPBULKINVOICESUMS',
                                 'CC_TMPCHECKRPT',
                                 'CC_TMPCHECKRPTCHECKGROUP',
                                 'CC_TMPCHECKRPTIGNOREPMTS',
                                 'CC_TMPCHECKSET',
                                 'CC_TMPCLAIMACCESS',
                                 'CC_TMPCONTACTADDRESSLINK',
                                 'CC_TMPCOVERAGERISKUNITMAP',
                                 'CC_TMPEXPRPTSTAGING',
                                 'CC_TMPEXPTOINCIDENTCOPY',
                                 'CC_TMPMIXEDCHECKGROUPS',
                                 'CC_TMPNEWTACCTJOINTBL',
                                 'CC_TMPPIPPRIMARYDOCTOR',
                                 'CC_TMPREJTACCTCONTRIBTXNS',
                                 'CC_TMPSEARCHCOLUMNS',
                                 'CC_TMPSEVERITYTOINCCOPY',
                                 'CC_TMPSRCTARGETTACCOUNTS',
                                 'CC_TMPSTAGGLIMIT',
                                 'CC_TMPSTAGGLIMIT2',
                                 'CC_TMPSTAGINGEXPOSURERPT',
                                 'CC_TMPSTCLAIMACCESS',
                                 'CC_TMPSTECLAIMACCESS',
                                 'CC_TMPTACCOUNTCREATEJOIN',
                                 'CC_TMPTACCOUNTCREDITDEBIT',
                                 'CC_TMPTACCOUNTJOIN',
                                 'CC_TMPTACCOUNTLINEITEM2ROW',
                                 'CC_TMPTACCOUNTRESERVELINE',
                                 'CC_TMPTACCTBALANCECOPY',
                                 'CC_TMPTACCTCONTRIBTXNS',
                                 'CC_TRANSACTIONID',
                                 'CC_TYPECODECOUNTDATADIST',
                                 'CC_TYPECODEUSAGERPT',
                                 'CC_TYPEKEYDATADIST',
                                 'CC_UPGRADEDATAMODELINFO',
                                 'CC_UPGRADEDBSTORAGESET',
                                 'CC_UPGRADEDBSTORAGESETCOLUMNS',
                                 'CC_UPGRADEDBSTORAGESETRESULTS',
                                 'CC_UPGRADEINSTANCE',
                                 'CC_UPGRADEROWCOUNT',
                                 'CC_UPGRADESCHEMAVERSION',
                                 'CC_UPGRADEVTDBMSDUMP',
                                 'CC_WORKFLOWLOG',
                                 'CC_WORKFLOWSTEPSTATS',
                                 'CC_WORKITEMSET',
                                 'CC_WORKQUEUESTATE',
                                 'CC_WORKQUEUEWORKERCONTROL',
                                 'CCX_REVIEWCASE',
                                 'GEOGRAPHY_COLUMNS',
                                 'PG_STAT_STATEMENTS',
                                 'SPATIAL_REF_SYS',
                                 'AB_ACTIVESCHEDULEDJOB',
                                 'AB_ARCHIVEFAILUREDETAILS',
                                 'AB_ARRAYDATADIST',
                                 'AB_BATCHPROCESSLEASE',
                                 'AB_BATCHPROCESSLEASEHISTORY',
                                 'AB_BEANVERSIONDATADIST',
                                 'AB_BLOBCOLDATADIST',
                                 'AB_BOOLEANCOLDATADIST',
                                 'AB_BROADCASTBATCH',
                                 'AB_CACHESTATSROLLUPSNAP',
                                 'AB_CLAIMSNAPSHOT',
                                 'AB_CLOBCOLDATADIST',
                                 'AB_CLUSTERMEMBERDATA',
                                 'AB_CONTACTFINGERPRINT',
                                 'AB_CUSTOMDATADISTREQ',
                                 'AB_DATABASEDATADIST',
                                 'AB_DATADISTQUERYEXEC',
                                 'AB_DATAGENINFO',
                                 'AB_DATAGENINFODETAIL',
                                 'AB_DATEANALYSISDATADIST',
                                 'AB_DATEBINNEDDATADIST',
                                 'AB_DATESPANDATADIST',
                                 'AB_DBCONSISTCHECKQUERYEXEC',
                                 'AB_DBCONSISTCHECKRUN',
                                 'AB_DBMSREPORT',
                                 'AB_DBPERFREPORT',
                                 'AB_DESTINATIONLEASE',
                                 'AB_DESTINATIONLEASEHISTORY',
                                 'AB_DYNAMIC_ASSIGN',
                                 'AB_ENCRYPTEDCOLUMNREGISTRY',
                                 'AB_FORKEYDATADIST',
                                 'AB_FRPTUPGRADE',
                                 'AB_GENERICGROUPCOLUMNDATADIST',
                                 'AB_GENERICGROUPCOUNTDATADIST',
                                 'AB_GENERICGROUPDATADIST',
                                 'AB_HOURANALYSISDATADIST',
                                 'AB_INBOUNDCHUNWORKITEM',
                                 'AB_INBOUNDFILE',
                                 'AB_INBOUNDFILECONFIG',
                                 'AB_INBOUNDFILEPURGEWORKITEM',
                                 'AB_INBOUNDHISTORY',
                                 'AB_INBOUNDRECORD',
                                 'AB_INBOUNDRECORDS',
                                 'AB_INBOUNDSUBRECORD',
                                 'AB_INSTRUMENTEDMESSAGE',
                                 'AB_INSTRUMENTEDWORKER',
                                 'AB_INSTRUMENTEDWORKERTASK',
                                 'AB_INSTRUMENTEDWORKEXECUTOR',
                                 'AB_LOADCALLBACK',
                                 'AB_LOADCALLBACKRESULT',
                                 'AB_LOADCOMMAND',
                                 'AB_LOADDBSTATISTICSCOMMAND',
                                 'AB_LOADENCRYPTCHUNK',
                                 'AB_LOADENCRYPTTABLE',
                                 'AB_LOADERROR',
                                 'AB_LOADERRORROW',
                                 'AB_LOADEXCLUSION',
                                 'AB_LOADINSERTSELECT',
                                 'AB_LOADINTEGRITYCHECK',
                                 'AB_LOADOPERATION',
                                 'AB_LOADPARAMETER',
                                 'AB_LOADROWCOUNT',
                                 'AB_LOADSTEP',
                                 'AB_LOADUPDATESTATISTICSSELECT',
                                 'AB_MAXKEY',
                                 'AB_MESSAGE',
                                 'AB_MESSAGEHISTORY',
                                 'AB_MESSAGEREQUESTLEASE',
                                 'AB_MESSAGEREQUESTLEASEHISTORY',
                                 'AB_NULLABLECOLUMNDATADIST',
                                 'AB_OUTBOUNDFILE',
                                 'AB_OUTBOUNDFILECONFIG',
                                 'AB_OUTBOUNDFILEPURGEWORKITEM',
                                 'AB_OUTBOUNDRECORD',
                                 'AB_OUTBOUNDRECPURGEWORKITEM',
                                 'AB_PARAMETER',
                                 'AB_PENDINGCONTACTCHANGE',
                                 'AB_PLUGINLEASE',
                                 'AB_PLUGINLEASEHISTORY',
                                 'AB_PROCESSHISTORY',
                                 'AB_PROFILERCONFIG',
                                 'AB_ROLLING',
                                 'AB_RUNTIMEPROPERTY',
                                 'AB_SCHEDULEDJOB',
                                 'AB_SEQUENCE',
                                 'AB_STANDARDWORKQUEUE',
                                 'AB_SYSTEMPARAMETER',
                                 'AB_TABLEDATADIST',
                                 'AB_TABLEREGISTRY',
                                 'AB_TABLEUPDATESTATSSTATEMENT',
                                 'AB_TEMPNEWPOLICYCMSNPAYABLE',
                                 'AB_TEMPOWNERIDTACCTIDNEWBAL',
                                 'AB_TRANSACTIONID',
                                 'AB_TYPECODECOUNTDATADIST',
                                 'AB_TYPECODEUSAGERPT',
                                 'AB_TYPEKEYDATADIST',
                                 'AB_UNCTMPNEWPLCYCMSNPAYABLE',
                                 'AB_UPGRADEDATAMODELINFO',
                                 'AB_UPGRADEDBSTORAGESET',
                                 'AB_UPGRADEDBSTORAGESETCOLUMNS',
                                 'AB_UPGRADEDBSTORAGESETRESULTS',
                                 'AB_UPGRADEINSTANCE',
                                 'AB_UPGRADEROWCOUNT',
                                 'AB_UPGRADESCHEMAVERSION',
                                 'AB_UPGRADETABLEREGISTRY',
                                 'AB_UPGRADEVTDBMSDUMP',
                                 'AB_WORKFLOWLOG',
                                 'AB_WORKFLOWSTEPSTATS',
                                 'AB_WORKITEMSET',
                                 'AB_WORKQUEUESTATE',
                                 'AB_WORKQUEUEWORKERCONTROL',
                                 'ABTL_INBOUNDCHUNKSTATUS',
                                 'ABTL_INBOUNDFILECONFIG',
                                 'ABTL_INBOUNDFILESTATUS',
                                 'ABTL_INBOUNDRECORDSTATUS',
                                 'ABTL_OUTBOUNDFILECONFIG',
                                 'ABTL_OUTBOUNDRECORDSTATUS',
                                 'ABTL_RUNTIMEPROPERTYGROUP',
                                 'GEOGRAPHY_COLUMNS',
                                 'PG_STAT_STATEMENTS',
                                 'SPATIAL_REF_SYS',
                                 'GEOGRAPHY_COLUMNS',
                                 'PC_ACTIVESCHEDULEDJOB',
                                 'PC_AGGLIMITRPT',
                                 'PC_ARRAYDATADIST',
                                 'PC_ASSIGNABLEFORKEYDATADIST',
                                 'PC_BATCHPROCESSLEASE',
                                 'PC_BATCHPROCESSLEASEHISTORY',
                                 'PC_BEANVERSIONDATADIST',
                                 'PC_BLOBCOLDATADIST',
                                 'PC_BOOLEANCOLDATADIST',
                                 'PC_BROADCASTBATCH',
                                 'PC_BRVALIDATOR',
                                 'PC_CACHESTATSROLLUPSNAP',
                                 'PC_CHECKRPT',
                                 'PC_CLAIMAGGLIMITRPT',
                                 'PC_CLOBCOLDATADIST',
                                 'PC_CLUSTERMEMBERDATA',
                                 'PC_CONTACTFINGERPRINT',
                                 'PC_CUSTOMDATADISTREQ',
                                 'PC_DATABASEDATADIST',
                                 'PC_DATADISTQUERYEXEC',
                                 'PC_DATAGENINFO',
                                 'PC_DATAGENINFODETAIL',
                                 'PC_DATEANALYSISDATADIST',
                                 'PC_DATEBINNEDDATADIST',
                                 'PC_DATESPANDATADIST',
                                 'PC_DBCONSISTCHECKQUERYEXEC',
                                 'PC_DBCONSISTCHECKRUN',
                                 'PC_DBPERFREPORT',
                                 'PC_DESTINATIONLEASE',
                                 'PC_DESTINATIONLEASEHISTORY',
                                 'PC_DYNAMIC_ASSIGN',
                                 'PC_ENCRYPTEDCOLUMNREGISTRY',
                                 'PC_FORKEYDATADIST',
                                 'PC_FRPTUPGRADE',
                                 'PC_GENERICGROUPCOLUMNDATADIST',
                                 'PC_GENERICGROUPCOUNTDATADIST',
                                 'PC_GENERICGROUPDATADIST',
                                 'PC_HOURANALYSISDATADIST',
                                 'PC_INBOUNDHISTORY',
                                 'PC_INSTRUMENTEDWORKER',
                                 'PC_INSTRUMENTEDWORKERTASK',
                                 'PC_INSTRUMENTEDWORKEXECUTOR',
                                 'PC_LOADCALLBACK',
                                 'PC_LOADCALLBACKRESULT',
                                 'PC_LOADCOMMAND',
                                 'PC_LOADDBSTATISTICSCOMMAND',
                                 'PC_LOADENCRYPTCHUNK',
                                 'PC_LOADENCRYPTTABLE',
                                 'PC_LOADERROR',
                                 'PC_LOADERRORROW',
                                 'PC_LOADEXCLUSION',
                                 'PC_LOADINSERTSELECT',
                                 'PC_LOADINTEGRITYCHECK',
                                 'PC_LOADOPERATION',
                                 'PC_LOADPARAMETER',
                                 'PC_LOADROWCOUNT',
                                 'PC_LOADSTEP',
                                 'PC_LOADUPDATESTATISTICSSELECT',
                                 'PC_MAXKEY',
                                 'PC_MESSAGE',
                                 'PC_MESSAGEHISTORY',
                                 'PC_MESSAGEREQUESTLEASE',
                                 'PC_MESSAGEREQUESTLEASEHISTORY',
                                 'PC_NULLABLECOLUMNDATADIST',
                                 'PC_PLUGINLEASE',
                                 'PC_PLUGINLEASEHISTORY',
                                 'PC_PMPATTERNACTIVATIONENTITY',
                                 'PC_PROCESSHISTORY',
                                 'PC_PROFILERCONFIG',
                                 'PC_ROLLING',
                                 'PC_SCHEDULEDJOB',
                                 'PC_SEQUENCE',
                                 'PC_STANDARDWORKQUEUE',
                                 'PC_SYSTEMPARAMETER',
                                 'PC_TABLEDATADIST',
                                 'PC_TABLEREGISTRY',
                                 'PC_TABLEUPDATESTATSSTATEMENT',
                                 'PC_TMPACCOUNTHOLDERCOUNT',
                                 'PC_TMPAGGLIMITORA',
                                 'PC_TMPAGGLIMITRPT',
                                 'PC_TMPBIITEMINFO',
                                 'PC_TMPBULKINVOICESUMS',
                                 'PC_TMPCHECKRPT',
                                 'PC_TMPCHECKRPTCHECKGROUP',
                                 'PC_TMPCHECKRPTIGNOREPMTS',
                                 'PC_TMPCHECKSET',
                                 'PC_TMPCLAIMACCESS',
                                 'PC_TMPCONTACTADDRESSLINK',
                                 'PC_TMPCOVERAGERISKUNITMAP',
                                 'PC_TMPEXPRPTSTAGING',
                                 'PC_TMPEXPTOINCIDENTCOPY',
                                 'PC_TMPMIXEDCHECKGROUPS',
                                 'PC_TMPNEWTACCTJOINTBL',
                                 'PC_TMPPIPPRIMARYDOCTOR',
                                 'PC_TMPREJTACCTCONTRIBTXNS',
                                 'PC_TMPSEARCHCOLUMNS',
                                 'PC_TMPSEVERITYTOINCCOPY',
                                 'PC_TMPSRCTARGETTACCOUNTS',
                                 'PC_TMPSTAGGLIMIT',
                                 'PC_TMPSTAGGLIMIT2',
                                 'PC_TMPSTAGINGEXPOSURERPT',
                                 'PC_TMPSTCLAIMACCESS',
                                 'PC_TMPTACCOUNTCREATEJOIN',
                                 'PC_TMPTACCOUNTCREDITDEBIT',
                                 'PC_TMPTACCOUNTJOIN',
                                 'PC_TMPTACCOUNTLINEITEM2ROW',
                                 'PC_TMPTACCOUNTRESERVELINE',
                                 'PC_TMPTACCTBALANCECOPY',
                                 'PC_TMPTACCTCONTRIBTXNS',
                                 'PC_TRANSACTIONID',
                                 'PC_TYPECODECOUNTDATADIST',
                                 'PC_TYPECODEUSAGERPT',
                                 'PC_TYPEKEYDATADIST',
                                 'PC_UPGRADEDATAMODELINFO',
                                 'PC_UPGRADEDATAMODELINFO',
                                 'PC_UPGRADEDBSTORAGESET',
                                 'PC_UPGRADEDBSTORAGESETCOLUMNS',
                                 'PC_UPGRADEDBSTORAGESETRESULTS',
                                 'PC_UPGRADEINSTANCE',
                                 'PC_UPGRADEINSTANCE',
                                 'PC_UPGRADEROWCOUNT',
                                 'PC_UPGRADESCHEMAVERSION',
                                 'PC_UPGRADETABLEREGISTRY',
                                 'PC_UPGRADEVTDBMSDUMP',
                                 'PC_WORKFLOWLOG',
                                 'PC_WORKFLOWSTEPSTATS',
                                 'PC_WORKITEMSET',
                                 'PC_WORKQUEUESTATE',
                                 'PC_WORKQUEUEWORKERCONTROL',
                                 'PG_STAT_STATEMENTS',
                                 'SPATIAL_REF_SYS'
                               )
                           ))
    SELECT RC.TABLE_NAME,
           RC.ROW_COUNT                     AS REPLICA_ROW_COUNT,
           ARRAY_AGG(UPPER(C.COLUMN_NAME))  AS REPLICA_COLUMNS
    FROM ROWCOUNTS AS RC
             INNER JOIN INFORMATION_SCHEMA.COLUMNS AS C
                        ON UPPER(RC.TABLE_CATALOG) = UPPER(C.TABLE_CATALOG)
                            AND UPPER(RC.TABLE_SCHEMA) = UPPER(C.TABLE_SCHEMA)
                            AND UPPER(RC.TABLE_NAME) = UPPER(C.TABLE_NAME)
    GROUP BY 1, 2;
"""


@dataclasses.dataclass
class Handler:
    logger: logging.Logger
    sns_client: SNSClient
    sf_session: SFSession
    pg_connection_pool: Any
    batch_id: str
    job_name: str
    core_center: str
    mrg_schema_name: str
    table_pattern: str

    @staticmethod
    def get_mrg_schema_name(sf_session: SFSession, core_center: str) -> str:
        cc_details = sf_session.sql(
            f"""
            SELECT MERGE_SCHEMA_NAME
            FROM ETL_CTRL.JC_CORE_CENTER_DETAILS
            WHERE CORE_CENTER = '{core_center}';
            """
        )

        mrg_schema_name = cc_details.collect()[0].MERGE_SCHEMA_NAME
        return mrg_schema_name

    @staticmethod
    def get_table_pattern(mrg_schema_name: str) -> str:
        table_patterns: dict[str, str] = {
            "MRG_PC": "PC%",
            "MRG_BC": "BC%",
            "MRG_CC": "CC%",
            "MRG_CM": "AB%",
        }
        table_pattern = table_patterns.get(mrg_schema_name)
        if table_pattern is None:
            raise Exception(f"No table pattern found for schema: {mrg_schema_name}")
        return table_pattern

    @classmethod
    def factory(
        cls,
        logger: logging.Logger,
        sns_client: SNSClient,
        sf_session: SFSession,
        pg_connection_pool: Any,
        batch_id: str,
        job_name: str,
        core_center: str,
    ) -> Handler:
        mrg_schema_name = cls.get_mrg_schema_name(
            sf_session=sf_session, core_center=core_center
        )

        table_pattern = cls.get_table_pattern(mrg_schema_name=mrg_schema_name)

        return cls(
            logger=logger,
            job_name=job_name,
            core_center=core_center,
            sns_client=sns_client,
            sf_session=sf_session,
            pg_connection_pool=pg_connection_pool,
            batch_id=batch_id,
            mrg_schema_name=mrg_schema_name,
            table_pattern=table_pattern,
        )

    def merge_mrg_columns(self) -> None:
        self.sf_session.sql(
            f"""
                MERGE INTO ETL_CTRL.JC_{self.core_center.upper()}_REPLICA_MRG_TABLE_COMPARISON AS TGT
                    USING (SELECT {self.batch_id}                   AS BATCH_ID,
                                  UPPER(COL.TABLE_SCHEMA)           AS SCHEMA_NAME,
                                  UPPER(COL.TABLE_NAME)             AS TABLE_NAME,
                                  MFST.CDA_FOLDER,
                                  MFST.CDA_FOLDER_TMS,
                                  MFST.CDA_SCHEMA_ID,
                                  ARRAY_AGG(UPPER(COL.COLUMN_NAME)) AS MRG_COLUMNS
                           FROM INFORMATION_SCHEMA.COLUMNS AS COL
                                    LEFT JOIN ETL_CTRL.JC_{self.core_center.upper()}_MANIFEST_DETAILS AS MFST
                                              ON UPPER(COL.TABLE_NAME) = UPPER(MFST.TABLE_NAME)
                           WHERE UPPER(COL.TABLE_SCHEMA) = UPPER('{self.mrg_schema_name}')
                             AND COL.TABLE_NAME ILIKE '{self.table_pattern}'
                           GROUP BY ALL) AS SRC
                    ON TGT.BATCH_ID = SRC.BATCH_ID
                        AND TGT.SCHEMA_NAME = SRC.SCHEMA_NAME
                        AND TGT.TABLE_NAME = SRC.TABLE_NAME
                    WHEN MATCHED THEN
                        UPDATE SET
                            TGT.CDA_FOLDER = SRC.CDA_FOLDER,
                            TGT.CDA_FOLDER_TMS = SRC.CDA_FOLDER_TMS,
                            TGT.CDA_SCHEMA_ID = SRC.CDA_SCHEMA_ID,
                            TGT.MRG_COLUMNS = SRC.MRG_COLUMNS,
                            TGT.ROW_UPDATE_TMS = CURRENT_TIMESTAMP()
                    WHEN NOT MATCHED THEN INSERT (
                                                  BATCH_ID,
                                                  SCHEMA_NAME,
                                                  TABLE_NAME,
                                                  CDA_FOLDER,
                                                  CDA_FOLDER_TMS,
                                                  CDA_SCHEMA_ID,
                                                  MRG_COLUMNS,
                                                  ROW_INSERT_TMS,
                                                  ROW_UPDATE_TMS
                        ) VALUES (SRC.BATCH_ID,
                                  SRC.SCHEMA_NAME,
                                  SRC.TABLE_NAME,
                                  SRC.CDA_FOLDER,
                                  SRC.CDA_FOLDER_TMS,
                                  SRC.CDA_SCHEMA_ID,
                                  SRC.MRG_COLUMNS,
                                  CURRENT_TIMESTAMP(),
                                  CURRENT_TIMESTAMP())
            """
        ).collect()

    def get_mrg_tables(self) -> SFDataFrame:
        return self.sf_session.sql(
            f"""
                SELECT
                    TABLE_CATALOG, 
                    TABLE_SCHEMA, 
                    TABLE_NAME
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = '{self.mrg_schema_name}'
                  AND TABLE_NAME LIKE '{self.table_pattern}'
            """
        )

    def merge_mrg_row_counts(self) -> None:
        tables = self.get_mrg_tables()
        row_count_query = "UNION ALL".join(
            [
                dedent(
                    f"""
            SELECT
                {self.batch_id} AS BATCH_ID,
                UPPER('{schema_name}') AS SCHEMA_NAME,
                UPPER('{table_name}')  AS TABLE_NAME,
                COUNT(*)              AS MRG_ROW_COUNT 
            FROM {table_catalog}.{schema_name}.{table_name}
            WHERE GW_DELETE_FLAG ='N'
            """
                )
                for table_catalog, schema_name, table_name in tables.collect()
            ]
        )
        self.sf_session.sql(
            f"""
                MERGE INTO ETL_CTRL.JC_{self.core_center.upper()}_REPLICA_MRG_TABLE_COMPARISON AS TGT
                    USING ({row_count_query}) AS SRC
                ON TGT.BATCH_ID = SRC.BATCH_ID
                    AND TGT.SCHEMA_NAME = SRC.SCHEMA_NAME
                    AND TGT.TABLE_NAME = SRC.TABLE_NAME
                WHEN MATCHED THEN
                    UPDATE SET
                        TGT.MRG_ROW_COUNT = SRC.MRG_ROW_COUNT,
                        TGT.ROW_UPDATE_TMS = CURRENT_TIMESTAMP()
                WHEN NOT MATCHED THEN INSERT (
                                              BATCH_ID,
                                              SCHEMA_NAME,
                                              TABLE_NAME,
                                              MRG_ROW_COUNT,
                                              ROW_INSERT_TMS,
                                              ROW_UPDATE_TMS
                    ) VALUES (SRC.BATCH_ID,
                              SRC.SCHEMA_NAME,
                              SRC.TABLE_NAME,
                              SRC.MRG_ROW_COUNT,
                              CURRENT_TIMESTAMP(),
                              CURRENT_TIMESTAMP());
            """
        ).collect()

    def merge_replica_columns_row_counts(self):
        connection = self.pg_connection_pool.getconn()
        try:
            with connection.cursor() as cursor:
                cursor.execute(dedent(REPLICA_ROW_COUNT_QUERY))
                df = pd.DataFrame(
                    cursor.fetchall(),
                    columns=[
                        "TABLE_NAME",
                        "REPLICA_ROW_COUNT",
                        "REPLICA_COLUMNS",
                    ],
                )
                self.sf_session.write_pandas(
                    df=df,
                    table_name=f"REPLICA_{self.core_center.upper()}_ROW_COUNT_COLUMNS",
                    schema="ETL_CTRL",
                    auto_create_table=True,
                    table_type="temporary",
                    overwrite=True,
                )
                self.sf_session.sql(
                    f"""
                        MERGE INTO ETL_CTRL.JC_{self.core_center.upper()}_REPLICA_MRG_TABLE_COMPARISON AS TGT
                        USING (
                            SELECT
                                {self.batch_id}                  AS BATCH_ID,
                                UPPER('{self.mrg_schema_name}')  AS SCHEMA_NAME,
                                UPPER(TABLE_NAME)                AS TABLE_NAME,
                                REPLICA_ROW_COUNT,
                                REPLICA_COLUMNS,
                                IFF(
                                    REPLICA_ROW_COUNT = 0, 
                                    ['REPLICA_ROW_COUNT_IS_ZERO'], 
                                    []
                                )                                AS EXCLUSION_REASON
                            FROM ETL_CTRL.REPLICA_{self.core_center.upper()}_ROW_COUNT_COLUMNS
                        ) AS SRC
                            ON TGT.BATCH_ID = SRC.BATCH_ID
                                AND TGT.SCHEMA_NAME = SRC.SCHEMA_NAME
                                AND TGT.TABLE_NAME = SRC.TABLE_NAME
                        WHEN MATCHED THEN
                            UPDATE SET
                                TGT.REPLICA_ROW_COUNT = SRC.REPLICA_ROW_COUNT,
                                TGT.REPLICA_COLUMNS = SRC.REPLICA_COLUMNS,
                                TGT.EXCLUSION_REASON = ARRAY_CAT(
                                    TGT.EXCLUSION_REASON,
                                    SRC.EXCLUSION_REASON
                                ),
                                TGT.ROW_UPDATE_TMS = CURRENT_TIMESTAMP()
                        WHEN NOT MATCHED THEN INSERT (
                            BATCH_ID,
                            SCHEMA_NAME,
                            TABLE_NAME,
                            REPLICA_ROW_COUNT,
                            REPLICA_COLUMNS,
                            EXCLUSION_REASON,
                            ROW_INSERT_TMS,
                            ROW_UPDATE_TMS
                        ) VALUES (
                            SRC.BATCH_ID,
                            SRC.SCHEMA_NAME,
                            SRC.TABLE_NAME,
                            SRC.REPLICA_ROW_COUNT,
                            SRC.REPLICA_COLUMNS,
                            SRC.EXCLUSION_REASON,
                            CURRENT_TIMESTAMP(),
                            CURRENT_TIMESTAMP()
                        )
                    """
                ).collect()
        finally:
            self.pg_connection_pool.putconn(connection)

    def update_variances(self) -> None:
        self.sf_session.sql(
            f"""
                UPDATE ETL_CTRL.JC_{self.core_center.upper()}_REPLICA_MRG_TABLE_COMPARISON
                SET
                    COLUMNS_VARIANCE = CASE
                        WHEN REPLICA_COLUMNS IS NULL 
                            THEN MRG_COLUMNS
                        WHEN MRG_COLUMNS IS NULL 
                            THEN IFF(REPLICA_ROW_COUNT = 0 
                                OR ETL_CTRL.TABLE_NAME_IS_BLOCKLISTED(TABLE_NAME => TABLE_NAME), 
                                [], 
                                REPLICA_COLUMNS
                            )
                        ELSE ARRAY_EXCEPT(ARRAY_CAT(
                                ARRAY_EXCEPT(REPLICA_COLUMNS, MRG_COLUMNS),
                                ARRAY_EXCEPT(MRG_COLUMNS, REPLICA_COLUMNS)
                            ), [
                                'BATCHID',
                                'GWCBI___CONNECTOR_TS_MS',
                                'GWCBI___LSN',
                                'GWCBI___OPERATION',
                                'GWCBI___PAYLOAD_TS_MS',
                                'GWCBI___SEQVAL',
                                'GWCBI___SEQVAL_HEX',
                                'GWCBI___TX_ID',
                                'GW_DELETE_FLAG',
                                'ROW_INSERT_TMS',
                                'ROW_UPDATE_TMS'
                            ])
                    END,
                    ROW_COUNT_VARIANCE = COALESCE(REPLICA_ROW_COUNT, 0) - COALESCE(MRG_ROW_COUNT, 0),
                    EXCLUSION_REASON = CASE
                        WHEN MRG_ROW_COUNT IS NULL 
                            AND REPLICA_ROW_COUNT IS NOT NULL
                            AND REPLICA_ROW_COUNT != 0
                            AND NOT ETL_CTRL.TABLE_NAME_IS_BLOCKLISTED(TABLE_NAME => TABLE_NAME)
                                THEN ARRAY_APPEND(EXCLUSION_REASON, 'TABLE_MISSING_IN_MRG')
                        WHEN REPLICA_ROW_COUNT IS NULL 
                            AND MRG_ROW_COUNT IS NOT NULL
                                THEN ARRAY_APPEND(EXCLUSION_REASON, 'TABLE_MISSING_IN_REPLICA')
                        ELSE EXCLUSION_REASON
                    END
                WHERE BATCH_ID = {self.batch_id}
            """
        ).collect()

        self.sf_session.sql(
            f"""
                UPDATE ETL_CTRL.JC_{self.core_center.upper()}_REPLICA_MRG_TABLE_COMPARISON
                SET
                    EXCLUSION_REASON = IFF(
                        ROW_COUNT_VARIANCE != 0, 
                        ARRAY_APPEND(EXCLUSION_REASON, 'ROW_COUNT_VARIANCE'), 
                        EXCLUSION_REASON
                    )
                WHERE BATCH_ID = {self.batch_id}
            """
        ).collect()

        self.sf_session.sql(
            f"""
                UPDATE ETL_CTRL.JC_{self.core_center.upper()}_REPLICA_MRG_TABLE_COMPARISON
                SET
                    EXCLUSION_REASON = IFF(
                        ARRAY_SIZE(COLUMNS_VARIANCE) > 0, 
                        ARRAY_APPEND(EXCLUSION_REASON, 'COLUMN_VARIANCE'), 
                        EXCLUSION_REASON
                    )
                WHERE BATCH_ID = {self.batch_id}
            """
        ).collect()

    def get_comparison_tables(self) -> list[Row]:
        return self.sf_session.sql(
            f"""
                SELECT
                    BATCH_ID,
                    SCHEMA_NAME,
                    TABLE_NAME
                FROM ETL_CTRL.JC_{self.core_center.upper()}_REPLICA_MRG_TABLE_COMPARISON
                WHERE BATCH_ID = {self.batch_id}
                    AND NOT ARRAYS_OVERLAP(EXCLUSION_REASON, [
                        'REPLICA_ROW_COUNT_IS_ZERO',
                        'TABLE_MISSING_IN_MRG'
                    ])
                    AND ARRAY_CONTAINS('BEANVERSION'::VARIANT, MRG_COLUMNS)
                    AND ARRAY_CONTAINS('BEANVERSION'::VARIANT, REPLICA_COLUMNS)
            """
        ).collect()

    def merge_all_mrg_records(self, tables: list[Row]) -> None:
        row_count_query = "UNION ALL".join(
            [
                dedent(
                    f"""
                        SELECT
                            {batch_id}              AS BATCH_ID,
                            UPPER('{schema_name}')  AS SCHEMA_NAME,
                            UPPER('{table_name}')   AS TABLE_NAME,
                            ID                      AS MRG_ID,
                            BEANVERSION             AS MRG_BEANVERSION
                        FROM {schema_name}.{table_name}
                        WHERE GW_DELETE_FLAG ='N'
                    """
                )
                for batch_id, schema_name, table_name in tables
            ]
        )
        self.sf_session.sql(
            f"""
                MERGE INTO ETL_CTRL.JC_{self.core_center.upper()}_REPLICA_MRG_RECORD_COMPARISON AS TGT
                    USING ({row_count_query}) AS SRC
                ON TGT.BATCH_ID = SRC.BATCH_ID
                    AND TGT.SCHEMA_NAME = SRC.SCHEMA_NAME
                    AND TGT.TABLE_NAME = SRC.TABLE_NAME
                    AND TGT.MRG_ID = SRC.MRG_ID
                WHEN MATCHED THEN
                    UPDATE SET
                        TGT.MRG_BEANVERSION = SRC.MRG_BEANVERSION,
                        TGT.ROW_UPDATE_TMS = CURRENT_TIMESTAMP()
                WHEN NOT MATCHED THEN INSERT (
                                              BATCH_ID,
                                              SCHEMA_NAME,
                                              TABLE_NAME,
                                              MRG_ID,
                                              MRG_BEANVERSION,
                                              ROW_INSERT_TMS,
                                              ROW_UPDATE_TMS
                    ) VALUES (SRC.BATCH_ID,
                              SRC.SCHEMA_NAME,
                              SRC.TABLE_NAME,
                              SRC.MRG_ID,
                              SRC.MRG_BEANVERSION,
                              CURRENT_TIMESTAMP(),
                              CURRENT_TIMESTAMP());
            """
        ).collect()

    def insert_replica_records_for_table(
        self, batch_id: int, schema_name: str, table_name: str
    ):
        connection = self.pg_connection_pool.getconn()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    dedent(
                        f"""
                            SELECT {batch_id} AS BATCH_ID,
                                   UPPER('{schema_name}')           AS SCHEMA_NAME,
                                   UPPER('{table_name}')            AS TABLE_NAME,
                                   ID                               AS REPLICA_ID,
                                   BEANVERSION::VARCHAR             AS REPLICA_BEANVERSION
                            FROM PUBLIC.{table_name}
                        """
                    )
                )
                df = pd.DataFrame(
                    cursor.fetchall(),
                    columns=[
                        "BATCH_ID",
                        "SCHEMA_NAME",
                        "TABLE_NAME",
                        "REPLICA_ID",
                        "REPLICA_BEANVERSION",
                    ],
                )
                self.sf_session.write_pandas(
                    df=df,
                    table_name=f"JC_{self.core_center.upper()}_REPLICA_RECORD",
                    schema="ETL_CTRL",
                    auto_create_table=False,
                    overwrite=False,
                )
        except Exception as exc:
            raise TableException(
                schema_name=schema_name,
                table_name=table_name,
                exclusion_reason=None,
                cause=exc,
            )
        finally:
            self.pg_connection_pool.putconn(connection)

    def insert_all_replica_records(
        self, tables: list[Row], max_workers: int
    ) -> list[TableException]:
        exceptions: list[TableException] = []

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures_table_dict = {
                executor.submit(
                    self.insert_replica_records_for_table,
                    batch_id=batch_id,
                    schema_name=schema_name,
                    table_name=table_name,
                ): table_name
                for batch_id, schema_name, table_name in tables
            }
            # Wait for all tasks to complete
            for future in as_completed(futures_table_dict):
                table_name = futures_table_dict[future]
                try:
                    future.result()
                    self.logger.info(
                        f"{table_name}: replica records inserted into "
                        f"ETL_CTRL.JC_{self.core_center.upper()}_REPLICA_RECORD"
                    )
                except TableException as exc:
                    exceptions.append(exc)
        return exceptions

    def merge_all_replica_records(self) -> None:
        self.sf_session.sql(
            f"""
                MERGE INTO ETL_CTRL.JC_{self.core_center.upper()}_REPLICA_MRG_RECORD_COMPARISON AS TGT
                    USING (
                        SELECT
                            BATCH_ID,
                            SCHEMA_NAME,
                            TABLE_NAME,
                            REPLICA_ID,
                            REPLICA_BEANVERSION
                        FROM ETL_CTRL.JC_{self.core_center.upper()}_REPLICA_RECORD
                    ) AS SRC
                ON TGT.BATCH_ID = SRC.BATCH_ID
                    AND TGT.SCHEMA_NAME = SRC.SCHEMA_NAME
                    AND TGT.TABLE_NAME = SRC.TABLE_NAME
                    AND TGT.MRG_ID = SRC.REPLICA_ID
                WHEN MATCHED THEN
                    UPDATE SET
                        TGT.REPLICA_ID = SRC.REPLICA_ID,
                        TGT.REPLICA_BEANVERSION = SRC.REPLICA_BEANVERSION,
                        TGT.ROW_UPDATE_TMS = CURRENT_TIMESTAMP()
                WHEN NOT MATCHED THEN INSERT (
                                              BATCH_ID,
                                              SCHEMA_NAME,
                                              TABLE_NAME,
                                              REPLICA_ID,
                                              REPLICA_BEANVERSION,
                                              ROW_INSERT_TMS,
                                              ROW_UPDATE_TMS
                    ) VALUES (
                        SRC.BATCH_ID,
                        SRC.SCHEMA_NAME,
                        SRC.TABLE_NAME,
                        SRC.REPLICA_ID,
                        SRC.REPLICA_BEANVERSION,
                        CURRENT_TIMESTAMP(),
                        CURRENT_TIMESTAMP()
                    )
            """
        ).collect()

    def update_beanversion_mismatches(self) -> None:
        self.sf_session.sql(
            f"""
                UPDATE ETL_CTRL.JC_{self.core_center.upper()}_REPLICA_MRG_TABLE_COMPARISON AS TGT
                SET TGT.ID_BEANVERSION_MISMATCHES = SRC.ID_BEANVERSION_MISMATCHES,
                    TGT.EXCLUSION_REASON = IFF(
                        SRC.ID_BEANVERSION_MISMATCHES != 0, 
                        ARRAY_APPEND(EXCLUSION_REASON, 'ID_BEANVERSION_MISMATCHES'), 
                        EXCLUSION_REASON
                    )
                FROM (SELECT BATCH_ID,
                             SCHEMA_NAME,
                             TABLE_NAME,
                             COUNT(*) AS ID_BEANVERSION_MISMATCHES
                      FROM ETL_CTRL.JC_{self.core_center.upper()}_REPLICA_MRG_RECORD_COMPARISON
                      WHERE BATCH_ID = {self.batch_id} 
                         AND (NOT EQUAL_NULL(MRG_ID, REPLICA_ID)
                              OR NOT EQUAL_NULL(MRG_BEANVERSION, REPLICA_BEANVERSION))
                      GROUP BY ALL) AS SRC
                WHERE TGT.BATCH_ID = SRC.BATCH_ID
                  AND TGT.SCHEMA_NAME = SRC.SCHEMA_NAME
                  AND TGT.TABLE_NAME = SRC.TABLE_NAME
            """
        ).collect()

    def get_variances(self) -> list[TableException]:
        variances = self.sf_session.sql(
            f"""
                SELECT SCHEMA_NAME,
                       TABLE_NAME,
                       ARRAY_TO_STRING(EXCLUSION_REASON, ', ') AS EXCLUSION_REASON
                FROM ETL_CTRL.JC_{self.core_center.upper()}_REPLICA_MRG_TABLE_COMPARISON
                WHERE BATCH_ID = {self.batch_id}
                  AND NOT ARRAY_CONTAINS('REPLICA_ROW_COUNT_IS_ZERO'::VARIANT, EXCLUSION_REASON)
                  AND ARRAY_SIZE(ARRAY_EXCEPT(EXCLUSION_REASON, ['REPLICA_ROW_COUNT_IS_ZERO'])) > 0
            """
        ).collect()

        return [
            TableException(
                schema_name=schema_name,
                table_name=table_name,
                exclusion_reason=exclusion_reason,
            )
            for schema_name, table_name, exclusion_reason in variances
        ]

    def send_success_notification(self) -> None:
        self.sns_client.send_notification(
            subject=f"SUCCESS: Replica MRG Comparison "
            f"Succeeded for '{self.core_center}' core center",
            message=dedent(
                "Hi team,\n\n"
                f"SUCCESS: The glue job {self.job_name} has detected "
                f"no variances for the '{self.core_center}' core center."
            ),
        )

    def send_failure_notification(
        self, exc: Exception, exceptions: list[TableException]
    ) -> None:
        exception_message = (
            str(exc)
            if not exceptions
            else "\n\n\n".join([exc.formatted for exc in exceptions])
        )
        if len(exception_message) > 1500:
            exception_message = exception_message[:1500]

        message = dedent(
            "Hi team,\n\n"
            f"FAILURE: The {self.job_name} glue job for the '{self.core_center}' core center failed to validate"
            f"the MRG layer against the Replica. The errors are listed below: \n\n{exception_message}\n\n"
            "For more information please check Cloudwatch latest log streams.\n\n"
            "Thanks"
        )

        self.sns_client.send_notification(
            subject=f"FAILURE: Replica MRG Comparison "
            f"failed for '{self.core_center}' core center",
            message=message,
        )


def main(
    logger: Any,
    job_name: str,
    core_center: str,
    sns_topic_name: str,
    snowflake_secret_name: str,
    replica_secret_name: str,
    database_name: str,
    batch_id: str,
    aws_profile: None | str = None,
):
    max_workers = 20
    logger.info(f"The {job_name} job has started for {core_center}")
    aws_session = Session(profile_name=aws_profile, region_name="us-east-1")
    sf_session = get_snowpark_session(
        secretsmanager_client=aws_session.client("secretsmanager"),
        secret_name=snowflake_secret_name,
        database_name=database_name,
    )
    sns_client = SNSClient.factory(
        session=aws_session, topic_name=sns_topic_name, logger=logger
    )
    pg_connection_pool = get_postgres_connection_pool(
        core_center=core_center,
        secretsmanager_client=aws_session.client("secretsmanager"),
        secret_name=replica_secret_name,
    )

    handler = Handler.factory(
        logger=logger,
        job_name=job_name,
        core_center=core_center,
        sns_client=sns_client,
        sf_session=sf_session,
        pg_connection_pool=pg_connection_pool,
        batch_id=batch_id,
    )
    exceptions: list[TableException] = []
    try:
        sf_session.sql(
            f"TRUNCATE ETL_CTRL.JC_{core_center.upper()}_REPLICA_MRG_RECORD_COMPARISON"
        ).collect()
        sf_session.sql(
            f"TRUNCATE ETL_CTRL.JC_{core_center.upper()}_REPLICA_RECORD"
        ).collect()
        handler.merge_mrg_columns()
        handler.merge_mrg_row_counts()
        handler.merge_replica_columns_row_counts()
        handler.update_variances()

        tables = handler.get_comparison_tables()

        handler.merge_all_mrg_records(tables=tables)
        exceptions.extend(
            handler.insert_all_replica_records(tables=tables, max_workers=max_workers)
        )
        if exceptions:
            raise exceptions[0]

        handler.merge_all_replica_records()

        handler.update_beanversion_mismatches()

        exceptions.extend(handler.get_variances())
        if exceptions:
            raise exceptions[0]

        sf_session.sql(
            f"TRUNCATE ETL_CTRL.JC_{core_center.upper()}_REPLICA_MRG_RECORD_COMPARISON"
        ).collect()
        sf_session.sql(
            f"TRUNCATE ETL_CTRL.JC_{core_center.upper()}_REPLICA_RECORD"
        ).collect()

        handler.send_success_notification()
        handler.logger.info(f"{job_name} successful for all {core_center} tables")

    except Exception as exc:
        handler.send_failure_notification(exc=exc, exceptions=exceptions)
        raise exc


if __name__ == "__main__" and IS_GLUE_JOB:
    ## @params: [JOB_NAME]
    _args = getResolvedOptions(
        sys.argv,
        [
            # Hard-coded parameters
            "JOB_NAME",
            "sns_topic_name",
            "snowflake_secret_name",
            "replica_secret_name",
            # Supplied by Matillion
            "batch_id",
            "core_center",
            "database_name",
        ],
    )
    sc = SparkContext()
    glue_context = GlueContext(sc)
    glue_logger = glue_context.get_logger()
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(_args["JOB_NAME"] + _args["core_center"], _args)

    main(
        logger=glue_logger,
        job_name=_args["JOB_NAME"],
        core_center=_args["core_center"],
        sns_topic_name=_args["sns_topic_name"],
        snowflake_secret_name=_args["snowflake_secret_name"],
        replica_secret_name=_args["replica_secret_name"],
        database_name=_args["database_name"],
        batch_id=_args["batch_id"],
    )

    job.commit()
elif __name__ == "__main__" and not IS_GLUE_JOB:
    if "REQUESTS_CA_BUNDLE" in os.environ:
        os.environ.pop("REQUESTS_CA_BUNDLE")

    _logger = logging.getLogger(__name__)
    _logger.setLevel(logging.DEBUG)

    _args = {
        "JOB_NAME": "Replica_MRG_Comparison",
        "core_center": "cc",
        "sns_topic_name": "kfbmic-edr-np-dev-snstopic",
        "snowflake_secret_name": "kfbmic-edr-np-dev-glue-snowflake-service-secret",
        "replica_secret_name": "kfbmic-edr-np-dev-matillion-replica-service-secret",
        "database_name": "KFB_ECDP_DEV",
        "batch_id": "-4567",
    }

    main(
        logger=_logger,
        job_name=_args["JOB_NAME"],
        core_center=_args["core_center"],
        sns_topic_name=_args["sns_topic_name"],
        snowflake_secret_name=_args["snowflake_secret_name"],
        replica_secret_name=_args["replica_secret_name"],
        database_name=_args["database_name"],
        batch_id=_args["batch_id"],
        aws_profile="nonprod",
    )
