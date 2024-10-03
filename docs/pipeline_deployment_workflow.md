# Pipeline Deployment Workflow

* [Setting Up A Development Environment](#setting-up-a-development-environment)
* [Prerequisites Before Deploying the Pipeline](#prerequisites-before-deploying-the-pipeline)
* [Deploying to the Development Environment](#deploying-to-the-development-environment)
* [Environment Promotion](#environment-promotion)
* [Access Denied or Pipeline Fail](#access-denied-or-pipeline-fail)

## Setting Up A Development Environment

If you haven't done so already, complete the steps defined
in [Setting up a Development Environment](./setting_up_dev_env.md).

## Initial Deployment

1. **Delete AWS Resources**:
    1. Access the AWS Management Console.
    2. Locate and terminate any resources with names starting with `kfbmic-edr` or `kfbmic-awsenv (kfbmic-np/pp/p)` to
       ensure a clean deployment environment.
    3. Search for the following resources:
        - IAM ROLE
        - IAM POLICY
        - GLUE
            - JOB
            - SECURITY CONFIGURATION
            - CONNECTION
            - CRAWLER
        - S3 BUCKET
        - KMS
        - SNS
        - SECRET MANAGER

2. **Update Bitbucket Deployment variables**:

   Update [Deployments Variables](https://bitbucket.org/kyfb_git/dataplatform_glue/admin/pipelines/deployment-settings)
   with the correct values for your deployment.
    - **Role_Arn**: OIDC Role ARN provided by the Cloud Ops team.
    - **Sf_Account**: Environment-specific Snowflake account.
    - **All_Pwd**: Manually enter a random value.
    - **CRM_Client_Id**: Obtain from the CRM (Euclid) team.
    - **CRM_Client_Secret**: Obtain from the CRM (Euclid) team.

3. **Update Environment Parameters:**

   Update the environment-specific [/parameters](./../parameters) file with the correct values for your deployment.
    - **AWSEnvironment**: AWS Environment name (np, p, pp).
    - **GWEnvironment**: GW environment name (dev, qa, preprod, prod).
    - **AccountNumber**: KFB AWS account number.
    - **EmailAddress1 to EmailAddress5**: Email addresses to receive email alerts.
    - **GwBcDir, GwPcDir, GwCcDir, GwCmDir**: GW BC, PC, CC, CM CDA subdirectories.
    - **GwBcS3Bucket, GwPcS3Bucket, GwCcS3Bucket, GwCmS3Bucket**: GW CDA BC, PC, CC, CM bucket names.
    - **KfbBcS3Bucket, KfbPcS3Bucket, KfbCcS3Bucket, KfbCmS3Bucket**: KFB BC, PC, CC, CM landing bucket names.
    - **SfDb**: Snowflake DB name.
    - **IAMRoleId**: SSO IAM Role User ID.
    - **MatillionServiceUser, MatillionServiceRole, GlueServiceUser, GlueServiceRole, MatillionBankServiceUser,
      MatillionBankServiceRole**: Obtain from the Snowflake environment.
    - **EnvironmentPrefix**: Environment prefix (e.g., `DEV`, `QA`, `PREPROD`).
    - **EnvironmentType**: Mention the environment name like `NONPROD`, `PROD`.
    - **CRMEnvironment**: Use `test` for QA/Dev/anyEnv environments and `live` for the prod environment.

4. **Deploy to the Environment:**
   Now the pipeline is ready for the initial deployment. Follow the [Development Workflow](./development_workflow.md) to
   implement changes and deploy them to the specific environment.

5. **Supply the Snowflake Storage Integration IAM Role ARN to the Snowflake Admin:**

   The Snowflake Security Integration requires values from the initial environment deployment. Provide the Snowflake
   storage integration IAM Role ARN to the Snowflake Admin.

    - **Role ARN structure**: `arn:aws:iam::kfb_aws_account_number:role/role_name`.

6. **Redeploy for Storage Integration:**

   The initial deployment lacked information that can only be obtained after the deployment. To circumvent this mutual
   dependency, the initial deploy if performed with default values that are updated in a subsequent redeploy.

   - **StorageAWSIAMUserARN**: Ask the Snowflake Admin for the Storage Integration IAM User ARN
   - **StorageAWSExternalID**: Ask the Snowflake Admin for the Storage Integration External ID
   
   Redeploy the pipeline and ensure the updated configuration is applied.

## Subsequent Deployments

For subsequent deployments, follow these steps:

1. Make any necessary changes to the pipeline configuration or code.
2. Now the pipeline is ready to deploy. Follow the [Development Workflow](./development_workflow.md) to implement
   changes and deploy them to the specific environment.

## Deploying to the Development Environment

See [Development Workflow](./development_workflow.md) for step-by-step instructions on how to implement changes and
deploy them to the Development environment.

## Environment Promotion

See [Environment Promotion](./environment_promotion.md) for the environment promotion playbook.

## Access Denied or Pipeline Fail

If you encounter an "access denied" error or the pipeline fails, follow these steps:

- Validate that your user has access to the resources created in the template.
- Trace the error on the CloudFormation stack.
- For more information, please cross-verify with both locations:
    - [Bitbucket Environment](https://bitbucket.org/kyfb_git/dataplatform_glue/admin/pipelines/)
    - [Setting up a Development Environment](./setting_up_dev_env.md). 

