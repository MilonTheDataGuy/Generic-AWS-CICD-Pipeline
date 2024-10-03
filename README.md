# AWS Resources Repository #

## Description ##
The project utilizes a CI/CD pipeline with Bitbucket and AWS CloudFormation for automated deployments to higher environments, ensuring faster and more reliable releases without manual intervention.

# Table of Contents

- [Repo Table of Contents](#markdown-header-repo-table-of-contents)
- [Objectives](#markdown-header-objectives)
- [Documentation](#markdown-header-documentation)

# Repo Table of Contents

* [/docs](./docs): Documentation and supporting assets.
* [/parameters](./parameters): Contains environment-specific parameters, used by the [pipeline](bitbucket-pipelines.yml).
* [/scripts](./scripts): **Glue jobs** for ingesting and loading data into a CDP environment from different sources.
* [/tags](./tags): Contains environment-specific tags or labels that the [pipeline](bitbucket-pipelines.yml) applies to Cloudformation deployments.
* [/template](./template): Contains **CloudFormation template** files that define the infrastructure and resources required for deploying and managing AWS resources.
* [/tests](./tests): Contains the test case suite.
* [/bitbucket-pipelines.yml](./bitbucket-pipelines.yml): The **Bitbucket pipeline** file in the repository is used to define the CI/CD pipeline configuration. It specifies the steps and actions that need to be executed when changes are pushed to the repository. This includes tasks such as building, testing, and deploying the application or infrastructure defined in the repository.

# Objectives

- **Streamlined Deployment**: The objective is to streamline the deployment process by automating the deployment of changes to higher environments. This eliminates the need for manual intervention, reducing the time and effort required for deployments and ensuring a faster and more efficient deployment workflow.

- **Consistent Environments**: The objective is to ensure consistent environments across different stages of the deployment pipeline. By using CloudFormation templates, the infrastructure and resources required for each environment can be defined and provisioned automatically, ensuring that all environments are consistent and eliminating any potential configuration drift.

- **Increased Agility**: The objective is to enable faster and more agile development cycles by automating the deployment process. With automated deployments, developers can quickly iterate on their code and have it automatically deployed to higher environments, allowing for rapid feedback and faster time to market for new features and bug fixes.

- **Improved Reliability**: The objective is to improve the reliability of deployments by reducing the risk of human error. By automating the deployment process, the chances of manual mistakes or inconsistencies are minimized, resulting in more reliable and predictable deployments to higher environments.

# Documentation:

* [Development Workflow](./docs/development_workflow.md)
* [Environment Promotion](./docs/environment_promotion.md)
* [Pipeline Deployment Workflow](./docs/pipeline_deployment_workflow.md)
* [Setting up a Development Environment](./docs/setting_up_dev_env.md)
