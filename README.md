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

## Quick Start

```bash
# Deploy everything (infrastructure + workflow bundles)
make deploy

# Run tests with sample data
make test

# Check deployment status
make status
```

## Deployment Options

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
# Step 1: Create workflow bundles locally
make bundles-create

# Step 2: Deploy S3 buckets only
make s3-buckets

# Step 3: Upload bundles to S3 (requires S3 buckets from step 2)
make bundles-upload

# Step 4: Deploy core infrastructure (requires bundles in S3)
make core-infrastructure

# Alternative: Use custom workflows with latest versions
make bundles-create WORKFLOW_REPOS='https://github.com/nf-core/mag,https://github.com/nf-core/ampliseq'

# Alternative: Use specific versions
make bundles-create WORKFLOW_REPOS='https://github.com/nf-core/mag@2.5.1,https://github.com/nf-core/ampliseq@2.5.0'
```

**Note**: The correct order is critical:
1. **bundles-create**: Creates workflow bundle ZIP files locally
2. **s3-buckets**: Deploys S3 buckets required for storing bundles and data
3. **bundles-upload**: Uploads the bundles to the S3 code bucket
4. **core-infrastructure**: Deploys Omics workflows (which need bundles in S3), DynamoDB, and Step Functions

### Interactive Bundle Management

For interactive workflow bundle creation:

```bash
make bundles-interactive
```

## Running the Pipeline

To run the pipeline with your data:

1.  **Place FASTQ Files**: Place your paired-end FASTQ files (e.g., `sample1_R1.fastq.gz`, `sample1_R2.fastq.gz`) in a directory.

2.  **Generate Samplesheets**: Navigate to the directory containing your FASTQ files and run:
    ```bash
    # From your FASTQ directory
    /path/to/repository/automations/generate_samplesheets.sh
    ```

3.  **Upload Data**: From the same directory, run:
    ```bash
    export INPUT_BUCKET=<your-input-s3-bucket-name>
    export AWS_PROFILE=<your-aws-profile>
    /path/to/repository/automations/upload_to_s3.sh
    ```

## Testing

Test the pipeline with included sample data:

```bash
# Run full test (generate samplesheets + upload)
make test

# Or run test steps separately
make test-generate  # Generate samplesheets
make test-upload    # Upload to S3
make test-clean     # Clean up test data
```

## Monitoring

Check the status of your deployment and pipeline runs:

```bash
# Show deployment status
make status

# View recent execution logs
make logs

# List workflow bundles in S3
make bundles-list
```

Monitor pipeline execution in the AWS Management Console:

-   **AWS Step Functions**: View the execution graph and logs
-   **AWS HealthOmics**: View individual workflow run status and results

## Makefile Targets

| Target | Description |
|--------|-------------|
| `make deploy` | Full deployment (infrastructure + bundles) |
| `make infrastructure` | Deploy AWS infrastructure only |
| `make bundles` | Create and upload workflow bundles |
| `make test` | Run pipeline with test data |
| `make status` | Show deployment status |
| `make clean` | Remove all AWS resources |
| `make validate` | Validate CloudFormation templates |
| `make logs` | Show recent execution logs |
| `make help` | Show all available targets |

### Bundle Management

| Target | Description |
|--------|-------------|
| `make bundles-create` | Create bundles from GitHub |
| `make bundles-upload` | Upload existing bundles |
| `make bundles-list` | List bundles in S3 |
| `make bundles-interactive` | Interactive bundle creation |

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

- **Makefile**: Main automation interface
- **automations/deploy_infrastructure.sh**: Deploys AWS infrastructure
- **automations/manage_workflow_bundles.sh**: Creates and uploads workflow bundles
- **automations/cleanup_pipeline.sh**: Removes all AWS resources
- **automations/generate_samplesheets.sh**: Generates pipeline samplesheets
- **automations/upload_to_s3.sh**: Uploads data and triggers pipeline

### CloudFormation Templates (infra/)

- **infra/healthomics-s3.yml**: S3 bucket infrastructure
- **infra/healthomics-nfcore.yml**: Core HealthOmics resources
- **infra/stepfunctions-omics-pipeline.yml**: Step Functions orchestration

### Directories

- **lambda_src/**: Lambda function source code
- **test_data/**: Sample FASTQ files for testing

## Advanced Usage

### Custom Workflows

Add custom nf-core workflows:

```bash
# Set custom repositories
export WORKFLOW_REPOS='https://github.com/your-org/custom-workflow'

# Create and upload bundles
make bundles
```

### Direct Script Usage

Use scripts directly for more control:

```bash
# Bundle management
./automations/manage_workflow_bundles.sh create
./automations/manage_workflow_bundles.sh upload
./automations/manage_workflow_bundles.sh list
```
