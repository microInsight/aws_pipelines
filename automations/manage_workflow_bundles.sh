#!/usr/bin/env bash
set -euo pipefail

#####################################################################
# Workflow Bundle Management Script for HealthOmics Pipeline
#####################################################################
# This script handles creating and uploading nf-core workflow bundles:
# 1. Creates bundles from GitHub repositories
# 2. Uploads bundles to S3
#####################################################################

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default values
AWS_REGION=${AWS_REGION:-"us-east-1"}
AWS_PROFILE=${AWS_PROFILE:-"microbial-insights"}
ACTION=${1:-""}

# S3 Bucket Names
ACCOUNT_ID=$(aws sts get-caller-identity --profile $AWS_PROFILE --query Account --output text)
CODE_BUCKET="healthomics-nfcore-bundles-${ACCOUNT_ID}"

# Array to store workflow bundle information
declare -A WORKFLOW_BUNDLES=()

# Function to print colored messages
print_status() {
    echo -e "${GREEN}[$(date '+%H:%M:%S')]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1" >&2
}

print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

# Function to upload a file to S3 with retries
upload_with_retries() {
    local file_path=$1
    local s3_uri=$2
    local retries=3
    local count=0
    local delay=5

    while [ $count -lt $retries ]; do
        aws s3 cp "$file_path" "$s3_uri" --profile "$AWS_PROFILE" && return 0
        count=$((count + 1))
        if [ $count -lt $retries ]; then
            print_warning "Upload failed for $file_path. Retrying in $delay seconds... ($count/$retries)"
            sleep $delay
        fi
    done

    print_error "Failed to upload $file_path after $retries attempts."
    exit 1
}

# Function to create workflow bundles from GitHub URLs
create_workflow_bundles() {
    print_status "Creating workflow bundles from GitHub repositories..."
    
    if [[ "$ACTION" == "interactive" ]]; then
        echo -e "\n${BLUE}Enter GitHub repository URLs (one per line, press Ctrl+D when done):${NC}"
        echo -e "${BLUE}Example: https://github.com/nf-core/mag${NC}"
        
        local repo_urls=()
        while IFS= read -r url; do
            [[ -n "$url" ]] && repo_urls+=("$url")
        done
    else
        # Use command line arguments or environment variable
        local repo_urls=()
        if [[ -n "${WORKFLOW_REPOS:-}" ]]; then
            IFS=',' read -ra repo_urls <<< "$WORKFLOW_REPOS"
        else
            # Default repositories
            repo_urls=(
                "https://github.com/nf-core/mag"
                "https://github.com/nf-core/metatdenovo"
            )
        fi
    fi
    
    if [[ ${#repo_urls[@]} -eq 0 ]]; then
        print_warning "No repository URLs provided."
        return
    fi
    
    # Create temporary directory for cloning
    local temp_dir=$(mktemp -d)
    trap "rm -rf $temp_dir" EXIT
    
    for url_spec in "${repo_urls[@]}"; do
        # Support format: https://github.com/org/repo[@version]
        local url version_override=""
        if [[ "$url_spec" =~ ^([^@]+)@(.+)$ ]]; then
            url="${BASH_REMATCH[1]}"
            version_override="${BASH_REMATCH[2]}"
        else
            url="$url_spec"
        fi
        
        print_info "Processing repository: $url"
        
        # Extract repository name and create bundle name
        local repo_name=$(basename "$url" .git)
        local org_name=$(basename "$(dirname "$url")")
        
        # Get available tags/versions
        print_info "Fetching available versions..."
        local tags=$(git ls-remote --tags "$url" | grep -E 'refs/tags/[0-9]+\.[0-9]+' | sed 's/.*refs\/tags\///' | grep -v '\^{}' | sort -V | tail -10)
        
        local version
        if [[ -n "$version_override" ]]; then
            # Use the specified version
            version="$version_override"
            print_info "Using specified version: $version"
        elif [[ -z "$tags" ]]; then
            print_warning "No version tags found for $repo_name, using main branch"
            version="latest"
        elif [[ "$ACTION" == "interactive" ]]; then
            # Show available versions in interactive mode
            echo -e "\n${BLUE}Available versions for $repo_name:${NC}"
            echo "$tags" | nl -w2 -s'. '
            echo -e "\n${BLUE}Enter version number (or press Enter for latest):${NC}"
            read -r selected_version
            
            if [[ -z "$selected_version" ]]; then
                # Use the latest version
                version=$(echo "$tags" | tail -1)
            else
                version="$selected_version"
            fi
            print_info "Using version: $version"
        else
            # In non-interactive mode, use the latest version
            version=$(echo "$tags" | tail -1)
            print_info "Using latest version: $version"
        fi
        
        # Clone the repository
        if [[ "$version" == "latest" ]]; then
            # Clone the main branch
            if ! git clone --depth 1 "$url" "$temp_dir/$repo_name" 2>/dev/null; then
                print_error "Failed to clone repository: $url"
                continue
            fi
        else
            # Clone the specific version
            if ! git clone --depth 1 --branch "$version" "$url" "$temp_dir/$repo_name" 2>/dev/null; then
                print_error "Failed to clone repository at version $version: $url"
                continue
            fi
        fi
        
        print_info "Cloned $repo_name version: $version"
        
        # Create bundle filename
        local bundle_name="${org_name}-${repo_name}_${version}.zip"
        local bundle_path="${temp_dir}/${bundle_name}"
        
        # Create zip file
        print_info "Creating bundle: $bundle_name"
        (cd "$temp_dir" && zip -r "$bundle_name" "$repo_name" -x "*.git*" "*.github*" "test*" "*tests*" > /dev/null 2>&1)
        
        if [[ -f "$bundle_path" ]]; then
            # Move to current directory
            mv "$bundle_path" .
            
            # Add to workflow bundles array
            local s3_key="${repo_name}/${bundle_name}"
            WORKFLOW_BUNDLES["$s3_key"]="$bundle_name"
            print_status "Successfully created bundle: $bundle_name"
        else
            print_error "Failed to create bundle for: $url"
            print_error "Bundle path: $bundle_path"
            # Show zip errors for debugging
            (cd "$temp_dir" && zip -r "$bundle_name" "$repo_name" -x "*.git*" "*.github*" "test*" "*tests*")
        fi
        
        # Clean up cloned repo
        rm -rf "$temp_dir/$repo_name"
    done
}

# Function to upload workflow bundles to S3
upload_workflow_bundles() {
    print_status "Checking and uploading workflow bundles to S3..."
    
    # Temporarily disable strict unbound variable checking for array operations
    set +u
    local bundle_count=${#WORKFLOW_BUNDLES[@]}
    set -u
    
    # If no bundles in array, look for existing zip files
    if [[ $bundle_count -eq 0 ]]; then
        print_info "Scanning for existing workflow bundles..."
        
        # Find all nf-core bundle zip files
        for bundle_file in nf-core-*.zip; do
            if [[ -f "$bundle_file" ]]; then
                # Extract workflow name from filename
                local workflow_name=$(echo "$bundle_file" | sed -E 's/^nf-core-([^_]+)_.*/\1/')
                local s3_key="${workflow_name}/${bundle_file}"
                WORKFLOW_BUNDLES["$s3_key"]="$bundle_file"
                print_info "Found bundle: $bundle_file"
            fi
        done
        
        # Re-check bundle count
        set +u
        bundle_count=${#WORKFLOW_BUNDLES[@]}
        set -u
        
        if [[ $bundle_count -eq 0 ]]; then
            print_error "No workflow bundles found to upload!"
            print_info "Please run 'make bundles-create' first."
            exit 1
        fi
    fi
    
    if [[ $bundle_count -eq 0 ]]; then
        print_warning "No workflow bundles found to upload."
        return
    fi
    
    print_info "Found ${bundle_count} workflow bundle(s) to process."
    
    # Iterate through all workflow bundles
    set +u
    for s3_key in "${!WORKFLOW_BUNDLES[@]}"; do
        local local_file="${WORKFLOW_BUNDLES[$s3_key]}"
        set -u
        
        # Check if bundle already exists in S3
        if aws s3api head-object --bucket "$CODE_BUCKET" --key "$s3_key" --profile "$AWS_PROFILE" >/dev/null 2>&1; then
            print_info "Bundle already exists in S3: $s3_key. Skipping upload."
        else
            print_info "Bundle not found in S3. Uploading: $s3_key"
            
            # Check if local file exists
            if [[ ! -f "$local_file" ]]; then
                print_error "Bundle file not found locally: $local_file"
                continue
            fi
            
            # Upload file to S3
            upload_with_retries "$local_file" "s3://${CODE_BUCKET}/${s3_key}"
        fi
    done
    set -u  # Re-enable strict mode after loop
    
    print_status "Workflow bundle check/upload complete."
}

# Function to list bundles in S3
list_s3_bundles() {
    print_status "Listing workflow bundles in S3 bucket: $CODE_BUCKET"
    aws s3 ls "s3://${CODE_BUCKET}/" --recursive --profile "$AWS_PROFILE" | grep -E "\.zip$" || print_warning "No bundles found in S3."
}

# Function to show help
show_help() {
    echo "Usage: $0 [COMMAND]"
    echo ""
    echo "Commands:"
    echo "  create        Create workflow bundles from GitHub repositories"
    echo "  upload        Upload existing workflow bundles to S3"
    echo "  list          List workflow bundles in S3"
    echo "  interactive   Interactive mode to create and upload bundles"
    echo "  help          Show this help message"
    echo ""
    echo "Environment Variables:"
    echo "  AWS_PROFILE   AWS profile to use (default: microbial-insights)"
    echo "  AWS_REGION    AWS region (default: us-east-1)"
    echo "  WORKFLOW_REPOS Comma-separated list of GitHub URLs for 'create' command"
    echo ""
    echo "Examples:"
    echo "  # Create bundles from default repositories"
    echo "  $0 create"
    echo ""
    echo "  # Create bundles from specific repositories"
    echo "  WORKFLOW_REPOS='https://github.com/nf-core/mag,https://github.com/nf-core/ampliseq' $0 create"
    echo ""
    echo "  # Upload existing bundles"
    echo "  $0 upload"
    echo ""
    echo "  # Interactive mode"
    echo "  $0 interactive"
}

# Main function
main() {
    case "$ACTION" in
        create)
            create_workflow_bundles
            ;;
        upload)
            upload_workflow_bundles
            ;;
        list)
            list_s3_bundles
            ;;
        interactive)
            create_workflow_bundles
            upload_workflow_bundles
            ;;
        help|--help|-h)
            show_help
            ;;
        *)
            print_error "Unknown command: $ACTION"
            show_help
            exit 1
            ;;
    esac
}

# Run main function
main 