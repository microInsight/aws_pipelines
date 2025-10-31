# AWS HealthOmics Bioinformatics Pipeline

This project provides a complete, automated pipeline for running nf-core bioinformatics workflows on AWS HealthOmics. It uses AWS CloudFormation for infrastructure as code, AWS Step Functions for orchestration, and AWS Lambda for event-driven processing.

## Overview

The pipeline is designed to be triggered by the upload of a `run_manifest.json` file to a designated S3 bucket. It automates the following nf-core workflows:

- `nf-core/mag`: Metagenomics Assembly and Binning
- `nf-core/metatdenovo`: Metagenome De-Novo Assembly

## Prerequisites

Before you begin, ensure you have the following installed and configured:

1. **Linux**: The automations in this makefile only work on a Linux OS.
2.  **AWS CLI**: Make sure the AWS CLI is installed and configured with credentials for your target AWS account.
3.  **AWS Profile**: The deployment scripts use an AWS profile named `microbial-insights`. You can either create this profile or update the `AWS_PROFILE` variable in the scripts to match your configuration.
    ```bash
    aws configure --profile microbial-insights
    ```
4.  **AWS SAM CLI**: The project uses AWS SAM (Serverless Application Model) CLI for building and deploying infrastructure. Install it from https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/install-sam-cli.html and ensure it is available in your PATH.
5.  **Make**: The project uses a Makefile for automation. Most systems have `make` installed by default.
6. **Python 3.12**: Ensure Python 3.12 is installed and available in your system PATH. You can download it from https://www.python.org/downloads/ and verify installation with:
    ```bash
    python3 --version
    ```
    The output should show Python 3.12.x.
7. You must update the workflow bundles and prepare them as zip files in the format nf-core-{**workflow**}_{**version number**}.zip. Follow instructions in this workshop to prep packages: https://catalog.us-east-1.prod.workshops.aws/workshops/76d4a4ff-fe6f-436a-a1c2-f7ce44bc5d17/en-US/workshop/project-setup. You must also upload them to a path exactly as follows: s3://healthomics-nfcore-bundles-125434852769/{**workflow**}/nf-core-{**workflow**}/{**version number**}.zip

## Uploading Samples and Starting a Run

The pipeline is now managed through a single unified entrypoint using the `manage_samples.py` automation.

You can upload new samples, generate sample sheets, and register a new run with HealthOmics using the Make target `upload-samples`.

### Make Target

```makefile
# Upload new samples for a job using the unified Python script
upload-samples: check-env
	@echo "$(BLUE)Uploading samples via unified script...$(NC)"
	@if [ -z "$(INPUT_BUCKET)" ]; then echo "$(RED)INPUT_BUCKET is required$(NC)"; exit 1; fi
	@if [ -z "$(SAMPLES_DIR)" ]; then echo "$(RED)SAMPLES_DIR is required$(NC)"; exit 1; fi
	@python3 automations/manage_samples.py \
		--samples-dir $(SAMPLES_DIR) \
		--input-bucket $(INPUT_BUCKET) \
		--job-name $(or $(JOB_NAME),$(notdir $(SAMPLES_DIR))) \
		--workflows $(or $(WORKFLOWS),mag metatdenovo) \
		--mag-params $(MAG_PARAMS) \
		--metatdenovo-params $(METATDENOVO_PARAMS) \
		--aws-profile $(AWS_PROFILE) \
		--region $(AWS_REGION)
	@echo "$(GREEN)✓ Samples and parameters uploaded$(NC)"
```

### Usage Example with Test Data

```bash
make upload-samples \
  SAMPLES_DIR=./test_data/fastq_pass \
  JOB_NAME=run_01 \
  MAG_PARAMS=./test_data/mag-artifacts/mag-test-input.json \ # must be in the formate mag-*.json
  METATDENOVO_PARAMS=./test_data/meta-artifacts/metatdenovo-test-input.json # must be in format metatdenovo-*.json
```

This command will:

* Generate `samplesheet_mag.csv` and `samplesheet_metatdenovo.csv`
* Upload all FASTQ files, both CSVs, the `run_manifest.json`, and both parameter JSONs to
  `s3://$(INPUT_BUCKET)/$(JOB_NAME)/`
* Initiate the run for submission through Step Functions or AWS HealthOmics. The run_manifest.json upload triggers the start of the job.

## Deployment Options (no need to run unless updates are made)

### Quick Start 

```bash
# Deploy everything (infrastructure + workflow bundles)
make deploy

```

### Full Deployment (Recommended)

Deploy the entire pipeline with a single command:

```bash
make deploy SNS_EMAIL1=user@example.com
```

This will:
1. Create workflow bundle files locally (mag and metatdenovo)
2. Deploy S3 buckets infrastructure
3. Upload workflow bundles to the S3 code bucket
4. Deploy core infrastructure (Omics workflows, DynamoDB, etc.)
5. Deploy Step Functions pipeline and configure S3 triggers

### Modular Deployment

Deploy components separately for more control:

```bash
# Step 1: Deploy S3 buckets only
make s3-buckets

# Step 2: Upload bundles to S3 (requires S3 buckets from step 2)
make bundles-upload

# Step 3: Deploy core infrastructure (requires bundles in S3)
make core-infrastructure

```

**Note**: The correct order is critical:
1. **s3-buckets**: Deploys S3 buckets required for storing bundles and data
2. **bundles-upload**: Uploads the bundles to the S3 code bucket
3. **core-infrastructure**: Deploys Omics workflows (which need bundles in S3), DynamoDB, and Step Functions


## Monitoring

Check the status of your deployment and pipeline runs:

```bash
# Show deployment status
make status

# List workflow bundles in S3
make bundles-list
```

Monitor pipeline execution in the AWS Management Console:

-   **AWS Step Functions**: View the execution graph and logs
-   **AWS HealthOmics**: View individual workflow run status and results


## Makefile Targets

| Target | Description |
|---|---|
| `all` |  |
| `bundles` |  |
| `check-env` |  |
| `clean` |  |
| `deploy` |  |
| `help` |  |
| `infrastructure` |  |
| `status` |  |
| `upload-samples` |  |

### Make Variables (selected)

| Variable | Default |
|---|---|
| `AWS_PROFILE` | `microbial-insights` |
| `AWS_REGION` | `us-east-1` |
| `SNS_EMAIL1` | `apartin@microbe.com` |
| `SNS_EMAIL2` | `operations@microbe.com` |
| `WORKFLOW_REPOS` | `https://github.com/nf-core/mag@3.1.0,https://github.com/nf-core/metatdenovo@1.0.1,https://github.com/nf-core/taxprofiler@1.2.0` |
| `WORKFLOW_CONFIG` | `$(shell \` |
| `ACCOUNT_ID` | `$(shell aws sts get-caller-identity --profile $(AWS_PROFILE) --query Account --output text 2>/dev/null || echo "000000000000")` |
| `INPUT_BUCKET` | `healthomics-nfcore-input-$(ACCOUNT_ID)` |
| `OUTPUT_BUCKET` | `healthomics-nfcore-output-$(ACCOUNT_ID)` |
| `CODE_BUCKET` | `healthomics-nfcore-bundles-$(ACCOUNT_ID)` |
## Configuration

### Environment Variables

Configure the pipeline behavior using environment variables:

```bash
export AWS_PROFILE=microbial-insights
export AWS_REGION=us-east-1
export SNS_EMAIL1=alerts@example.com
export SNS_EMAIL2=backup@example.com
export WORKFLOW_REPOS='https://github.com/nf-core/mag,https://github.com/nf-core/ampliseq'
```

### Make Variables

Override default values when running make commands:

```bash
make deploy AWS_PROFILE=production AWS_REGION=eu-west-1
make bundles WORKFLOW_REPOS='https://github.com/nf-core/rnaseq'
```

### Workflow Versions

The pipeline supports multiple ways to specify workflow versions:

```bash
# Use latest version (default)
make bundles WORKFLOW_REPOS='https://github.com/nf-core/mag'

# Specify exact version using @ syntax
make bundles WORKFLOW_REPOS='https://github.com/nf-core/mag@2.5.1'

# Mix latest and specific versions
make bundles WORKFLOW_REPOS='https://github.com/nf-core/mag@2.5.1,https://github.com/nf-core/ampliseq'

# Interactive mode - select versions from a list
make bundles-interactive
```

## Cleanup

Remove all deployed AWS resources:

```bash
make clean
```

**Warning**: This will delete all CloudFormation stacks and S3 buckets, including any data stored in them.

## Troubleshooting

### Validate Templates

Check CloudFormation templates for syntax errors:

```bash
make validate
```

### Common Issues

-   **S3 Upload Failures**: The bundle upload may fail on slow connections. Retry with:
    ```bash
    make bundles-upload
    ```

-   **IAM Permissions**: Ensure your AWS user/role has permissions to create:
    - CloudFormation stacks
    - S3 buckets
    - Lambda functions
    - HealthOmics workflows
    - IAM roles
    - Step Functions

-   **SNS Notifications**: Check email and confirm SNS subscriptions to receive pipeline notifications.

## Repository Contents

### Scripts (automations/)

- **automations/manage_samples.py**: uploads parameter files, creates and upload samplesheets and run manifests.
- **automations/manage_workflow_bundles.sh**: Creates and uploads workflow bundles

### CloudFormation Templates (infra/)

- **infra/healthomics-s3.yml**: S3 bucket infrastructure
- **infra/healthomics-nfcore.yml**: Core HealthOmics resources
- **infra/stepfunctions-omics-pipeline.yml**: Step Functions orchestration

### Directories

- **healthomics/lambda_src/**: Lambda function source code
- **test_data/**: Sample FASTQ files for testing