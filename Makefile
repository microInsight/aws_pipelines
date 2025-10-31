# AWS HealthOmics Pipeline Makefile
# This Makefile provides automation for deploying, testing, and managing the pipeline

.PHONY: all help infrastructure bundles test clean deploy status check-env upload-samples

# Default AWS settings - can be overridden
AWS_PROFILE ?= microbial-insights
AWS_REGION ?= us-east-1
SNS_EMAIL1 ?= apartin@microbe.com
SNS_EMAIL2 ?= operations@microbe.com

# Default workflow repositories - can be overridden
WORKFLOW_CONFIG := mag:3.4.0,metatdenovo:1.2.0

MAG_BUNDLE := nf-core-mag_3.4.0.zip
METATDENOVO_BUNDLE := nf-core-metatdenovo_1.2.0.zip

# Export environment variables for scripts
export AWS_PROFILE
export AWS_REGION
export SNS_EMAIL1
export SNS_EMAIL2

# Get AWS account ID
ACCOUNT_ID := $(shell aws sts get-caller-identity --profile $(AWS_PROFILE) --query Account --output text 2>/dev/null || echo "000000000000")
INPUT_BUCKET := healthomics-nfcore-input-$(ACCOUNT_ID)
OUTPUT_BUCKET := healthomics-nfcore-output-$(ACCOUNT_ID)
CODE_BUCKET := healthomics-nfcore-bundles-$(ACCOUNT_ID)

# Color output
BLUE := \033[0;34m
GREEN := \033[0;32m
RED := \033[0;31m
YELLOW := \033[1;33m
NC := \033[0m

# Default target
all: deploy

# Help target
help:
	@echo "$(BLUE)AWS HealthOmics Pipeline Automation$(NC)"
	@echo ""
	@echo "$(GREEN)Usage:$(NC)"
	@echo "  make [target] [VARIABLE=value ...]"
	@echo ""
	@echo "$(GREEN)Main Targets:$(NC)"
	@echo "  $(YELLOW)deploy$(NC)              - Full deployment (infrastructure + bundles)"
	@echo "  $(YELLOW)infrastructure$(NC)      - Deploy AWS infrastructure only"
	@echo "  $(YELLOW)bundles$(NC)             - Create and upload workflow bundles"
	@echo "  $(YELLOW)test$(NC)                - Run pipeline with test data"
	@echo "  $(YELLOW)clean$(NC)               - Remove all AWS resources"
	@echo "  $(YELLOW)status$(NC)              - Show deployment status"
	@echo ""
	@echo "$(GREEN)Bundle Management:$(NC)"
	@echo "  $(YELLOW)bundles-upload$(NC)      - Upload existing bundles to S3"
	@echo "  $(YELLOW)bundles-list$(NC)        - List bundles in S3"
	@echo ""
	@echo "$(GREEN)Variables:$(NC)"
	@echo "  AWS_PROFILE         - AWS profile (default: $(AWS_PROFILE))"
	@echo "  AWS_REGION          - AWS region (default: $(AWS_REGION))"
	@echo "  SNS_EMAIL1          - First notification email"
	@echo "  SNS_EMAIL2          - Second notification email"
	@echo "  WORKFLOW_REPOS      - Comma-separated GitHub URLs for bundles"
	@echo ""
	@echo "$(GREEN)Examples:$(NC)"
	@echo "  make deploy SNS_EMAIL1=user@example.com"
	@echo "  make bundles WORKFLOW_REPOS='https://github.com/nf-core/mag,https://github.com/nf-core/ampliseq'"
	@echo "  make test"

# Check environment
check-env:
	@echo "$(BLUE)Checking environment...$(NC)"
	@if ! command -v aws >/dev/null 2>&1; then \
		echo "$(RED)Error: AWS CLI not found$(NC)"; \
		exit 1; \
	fi
	@if ! aws sts get-caller-identity --profile $(AWS_PROFILE) >/dev/null 2>&1; then \
		echo "$(RED)Error: Unable to authenticate with AWS profile '$(AWS_PROFILE)'$(NC)"; \
		exit 1; \
	fi
	@echo "$(GREEN)✓ AWS CLI configured$(NC)"
	@echo "$(GREEN)✓ AWS Profile: $(AWS_PROFILE)$(NC)"
	@echo "$(GREEN)✓ AWS Region: $(AWS_REGION)$(NC)"
	@echo "$(GREEN)✓ AWS Account: $(ACCOUNT_ID)$(NC)"

# Deploy S3 buckets only
s3-buckets: check-env
	@echo "$(BLUE)Deploying S3 buckets...$(NC)"
	@sam build -t healthomics-pipeline/template.yaml
	@sam deploy --stack-name healthomics-head-template --capabilities CAPABILITY_IAM CAPABILITY_AUTO_EXPAND --resolve-s3 \
		--parameter-overrides ParameterKey=FirstPass,ParameterValue=true \
		ParameterKey=SecondPass,ParameterValue=false \
		ParameterKey=ThirdPass,ParameterValue=false \
		ParameterKey=WorkflowConfig,ParameterValue=$(WORKFLOW_CONFIG)
	@echo "$(GREEN)✓ S3 buckets deployed$(NC)"

# Deploy core infrastructure (Omics, Step Functions, etc.)
core-infrastructure: check-env
	@echo "$(BLUE)Deploying core infrastructure...$(NC)"
	@sam build -t healthomics-pipeline/template.yaml
	@sam deploy --stack-name healthomics-head-template --capabilities CAPABILITY_IAM CAPABILITY_NAMED_IAM CAPABILITY_AUTO_EXPAND --resolve-s3 \
		--parameter-overrides ParameterKey=FirstPass,ParameterValue=true \
		ParameterKey=SecondPass,ParameterValue=true \
		ParameterKey=ThirdPass,ParameterValue=false \
		ParameterKey=WorkflowConfig,ParameterValue=$(WORKFLOW_CONFIG)
	@echo "$(GREEN)✓ Core infrastructure deployed$(NC)"

# Deploy s3 notification to Lambda
s3-notification: check-env
	@echo "$(BLUE)Deploying s3 notification to Lambda...$(NC)"
	@sam build -t healthomics-pipeline/template.yaml
	@sam deploy --stack-name healthomics-head-template --capabilities CAPABILITY_IAM CAPABILITY_NAMED_IAM CAPABILITY_AUTO_EXPAND --resolve-s3 \
		--parameter-overrides ParameterKey=FirstPass,ParameterValue=true \
		ParameterKey=SecondPass,ParameterValue=true \
		ParameterKey=ThirdPass,ParameterValue=true \
		ParameterKey=WorkflowConfig,ParameterValue=$(WORKFLOW_CONFIG)
	@echo "$(GREEN)✓ S3 notification to Lambda deployed$(NC)"

# Deploy infrastructure (full - for backward compatibility)
infrastructure: s3-buckets bundles-upload core-infrastructure s3-notification
	@echo "$(GREEN)✓ Infrastructure deployment complete$(NC)"

# Manage workflow bundles
bundles: check-env bundles-upload

bundles-upload: check-env
	@echo "$(BLUE)Uploading workflow bundles...$(NC)"
	@python3 automations/upload_workflow_bundles.py --bucket $(CODE_BUCKET) \
      --map mag=/mag --map metatdenovo=/metatdenovo \
       $(MAG_BUNDLE) $(METATDENOVO_BUNDLE) 
	@echo "$(GREEN)✓ Bundle upload complete$(NC)"

bundles-list: check-env
	@echo "$(BLUE)Listing workflow bundles in S3...$(NC)"
	@chmod +x automations/manage_workflow_bundles.sh
	@./automations/manage_workflow_bundles.sh list

# Full deployment
deploy: s3-buckets bundles-upload core-infrastructure s3-notification
	@echo "$(GREEN)✓ Full deployment complete!$(NC)"
	@echo ""
	@echo "$(BLUE)Next steps:$(NC)"
	@echo "1. Run 'make upload-samples SAMPLES_DIR=path/to/samples' to upload your samples."
	@echo "2. Monitor the pipeline execution in the AWS Step Functions console."

destroy: check-env
	@echo "$(BLUE)Destroying infrastructure...$(NC)"
	@sam build -t healthomics-pipeline
	@sam delete --stack-name healthomics-head-template
	@echo "$(GREEN)✓ Infrastructure destroyed$(NC)"

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

# Validate templates
validate: check-env
	@echo "$(BLUE)Validating CloudFormation templates...$(NC)"
	@aws cloudformation validate-template \
		--template-body file://healthomics-pipeline/infra/healthomics-s3.yml \
		--profile $(AWS_PROFILE) \
		--region $(AWS_REGION) >/dev/null && \
		echo "$(GREEN)✓ healthomics-s3.yml$(NC)"
	@aws cloudformation validate-template \
		--template-body file://healthomics-pipeline/infra/healthomics-nfcore.yml \
		--profile $(AWS_PROFILE) \
		--region $(AWS_REGION) >/dev/null && \
		echo "$(GREEN)✓ healthomics-nfcore.yml$(NC)"
	@aws cloudformation validate-template \
		--template-body file://healthomics-pipeline/infra/healthomics-stepfunctions.yml \
		--profile $(AWS_PROFILE) \
		--region $(AWS_REGION) >/dev/null && \
		echo "$(GREEN)✓ stepfunctions-omics-pipeline.yml$(NC)"