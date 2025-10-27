#!/usr/bin/env bash
set -euo pipefail

#####################################################################
# Upload to S3 Script
#####################################################################
# This script uploads FASTQ files, samplesheets, and manifest to S3
# in the correct order to trigger the Step Functions pipeline.
#
# Prerequisites:
# 1. Run generate_samplesheets.sh first
# 2. Review/edit the generated CSV files if needed
# 3. Set required environment variables
#
# Required environment variables:
#   - AWS_PROFILE: AWS profile to use
#   - INPUT_BUCKET: S3 bucket for raw FASTQ uploads
# 
# Optional environment variables:
#   - RUN_ID: Run identifier (defaults to current directory name)
#   - AWS_REGION: AWS region (defaults to us-east-1)
#####################################################################

# Validate required environment variables
INPUT_BUCKET=${INPUT_BUCKET:?"Error: INPUT_BUCKET environment variable not set."}
AWS_PROFILE=${AWS_PROFILE:?"Error: AWS_PROFILE environment variable not set."}
AWS_REGION=${AWS_REGION:-"us-east-1"}

# Set run ID from environment or use current directory name
RUN_ID=${RUN_ID:-$(basename "$PWD")}
S3_PREFIX="s3://${INPUT_BUCKET}/${RUN_ID}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

#####################################################################
# Helper Functions
#####################################################################

# Function to print colored status messages
print_status() {
    echo -e "${GREEN}[$(date '+%H:%M:%S')]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1" >&2
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

# Function to check if required files exist
check_prerequisites() {
    local missing_files=0
    
    print_status "Checking prerequisites..."
    
    # Check for samplesheets
    if [[ ! -f "samplesheet_mag.csv" ]]; then
        print_error "samplesheet_mag.csv not found"
        missing_files=1
    else
        print_info "Found samplesheet_mag.csv"
    fi
    
    if [[ ! -f "samplesheet_metatdenovo.csv" ]]; then
        print_error "samplesheet_metatdenovo.csv not found"
        missing_files=1
    else
        print_info "Found samplesheet_metatdenovo.csv"
    fi
    
    # Check for FASTQ files
    local fastq_count=$(ls -1 *.fastq.gz 2>/dev/null | wc -l)
    if [[ $fastq_count -eq 0 ]]; then
        print_error "No FASTQ files found in current directory"
        missing_files=1
    else
        print_info "Found $fastq_count FASTQ files"
    fi
    
    if [[ $missing_files -eq 1 ]]; then
        print_error "Missing required files. Please run generate_samplesheets.sh first."
        return 1
    fi
    
    return 0
}

# Function to upload FASTQ files
upload_fastq_files() {
    print_status "Uploading FASTQ files to ${S3_PREFIX}/"
    
    # Count FASTQ files
    local fastq_count=$(ls -1 *.fastq.gz 2>/dev/null | wc -l)
    print_info "Uploading $fastq_count FASTQ files..."
    
    # Upload only FASTQ files
    aws s3 sync . "${S3_PREFIX}/" \
        --exclude "*" \
        --include "*.fastq.gz" \
        --region "${AWS_REGION}" \
        --profile "${AWS_PROFILE}" \
        --no-progress
    
    if [[ $? -eq 0 ]]; then
        print_status "Successfully uploaded $fastq_count FASTQ files"
    else
        print_error "Failed to upload FASTQ files"
        return 1
    fi
    
    return 0
}

# Function to upload samplesheets and manifest
upload_samplesheets_and_manifest() {
#     # Create run manifest
#     print_status "Creating run manifest..."
#     cat <<EOF > run_manifest.json
# {
#   "run_id": "${RUN_ID}",
#   "mag_samplesheet": "${S3_PREFIX}/samplesheet_mag.csv",
#   "metatdenovo_samplesheet": "${S3_PREFIX}/samplesheet_metatdenovo.csv",
#   "timestamp": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
#   "source": "upload_to_s3.sh"
# }
# EOF
    
    # Upload MetaTDeNovo samplesheet first
    print_status "Uploading MetaTDeNovo samplesheet..."
    aws s3 cp samplesheet_metatdenovo.csv "${S3_PREFIX}/" \
        --region "${AWS_REGION}" \
        --profile "${AWS_PROFILE}" \
        --no-progress
    
    if [[ $? -ne 0 ]]; then
        print_error "Failed to upload MetaTDeNovo samplesheet"
        return 1
    fi
    
    # Upload MAG samplesheet second
    print_status "Uploading MAG samplesheet..."
    aws s3 cp samplesheet_mag.csv "${S3_PREFIX}/" \
        --region "${AWS_REGION}" \
        --profile "${AWS_PROFILE}" \
        --no-progress
    
    if [[ $? -ne 0 ]]; then
        print_error "Failed to upload MAG samplesheet"
        return 1
    fi
    
    # Upload manifest last to trigger the pipeline
    print_status "Uploading run manifest to trigger pipeline..."
    aws s3 cp run_manifest.json "${S3_PREFIX}/" \
        --region "${AWS_REGION}" \
        --profile "${AWS_PROFILE}" \
        --no-progress
    
    if [[ $? -ne 0 ]]; then
        print_error "Failed to upload run manifest"
        return 1
    fi
    
    print_status "Successfully uploaded all samplesheets and manifest"
    return 0
}

# Function to verify uploads
verify_uploads() {
    print_status "Verifying uploads..."
    
    # List all uploaded files
    local uploaded_files=$(aws s3 ls "${S3_PREFIX}/" \
        --region "${AWS_REGION}" \
        --profile "${AWS_PROFILE}" \
        --recursive | wc -l)
    
    print_info "Total files uploaded: $uploaded_files"
    
    # Check for specific files
    local manifest_exists=$(aws s3 ls "${S3_PREFIX}/run_manifest.json" \
        --region "${AWS_REGION}" \
        --profile "${AWS_PROFILE}" 2>/dev/null | wc -l)
    
    if [[ $manifest_exists -eq 1 ]]; then
        print_info "✓ Run manifest uploaded successfully"
        return 0
    else
        print_error "Run manifest not found in S3"
        return 1
    fi
}

#####################################################################
# Main Execution
#####################################################################

main() {
    print_status "Starting upload process for run: ${RUN_ID}"
    print_status "Target S3 location: ${S3_PREFIX}"
    
    # Check prerequisites
    check_prerequisites || exit 1
    
    print_info "Proceeding with upload automatically..."
    
    # Step 1: Upload FASTQ files first
    upload_fastq_files || exit 1
    
    # Step 2: Upload samplesheets and manifest in correct order
    upload_samplesheets_and_manifest || exit 1
    
    # Step 3: Verify uploads
    verify_uploads || exit 1
    
    # Success message
    echo ""
    echo -e "${GREEN}✓ Upload complete!${NC}"
    print_status "The Step Functions pipeline has been triggered"
    print_status "Monitor progress in the AWS Step Functions console"
    
    # Provide console links
    echo ""
    print_info "Useful links:"
    echo "  Step Functions: https://console.aws.amazon.com/states/home?region=${AWS_REGION}#/statemachines"
    echo "  HealthOmics: https://console.aws.amazon.com/omics/home?region=${AWS_REGION}#/runs"
    echo "  S3 Bucket: https://s3.console.aws.amazon.com/s3/buckets/${INPUT_BUCKET}?prefix=${RUN_ID}/"
    
    # Clean up local manifest file (optional)
    if [[ "${CLEANUP:-false}" == "true" ]]; then
        print_status "Cleaning up local manifest file..."
        rm -f run_manifest.json
    fi
}

# Run main function
main 