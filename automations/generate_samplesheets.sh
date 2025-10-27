#!/usr/bin/env bash
set -euo pipefail

#####################################################################
# Generate Samplesheets Script
#####################################################################
# This script generates MAG and metatdenovo samplesheets from FASTQ
# files in the current directory. It does NOT upload to S3.
#
# After running this script, you can:
# 1. Review and edit the generated CSV files
# 2. Run upload_to_s3.sh to upload everything
#####################################################################

# Set run ID from environment or use current directory name
RUN_ID=${RUN_ID:-$(basename "$PWD")}

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

print_success() {
    echo -e "${GREEN}[$(date '+%H:%M:%S')]${NC} $1"
}

# Function to create run manifest
create_run_manifest() {
    local run_id=$1
    shift
    local workflows=("$@")
    
    local manifest_file="run_manifest.json"
    print_info "Creating run manifest..."
    
    # Start JSON
    cat > "$manifest_file" << EOF
{
  "run_id": "${run_id}",
  "timestamp": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")",
  "workflows": [
EOF
    
    # Add workflow entries
    local first=true
    for workflow in "${workflows[@]}"; do
        local samplesheet="samplesheet_${workflow}.csv"
        if [[ -f "$samplesheet" ]]; then
            if [[ "$first" != "true" ]]; then
                echo "," >> "$manifest_file"
            fi
            echo -n "    \"${workflow}\"" >> "$manifest_file"
            first=false
        fi
    done
    
    # Close JSON
    cat >> "$manifest_file" << EOF

  ],
EOF
    
    # Add samplesheet references
    first=true
    for workflow in "${workflows[@]}"; do
        local samplesheet="samplesheet_${workflow}.csv"
        if [[ -f "$samplesheet" ]]; then
            if [[ "$first" == "true" ]]; then
                echo "  " >> "$manifest_file"
            else
                echo "," >> "$manifest_file"
            fi
            echo -n "  \"samplesheet_${workflow}.csv\": \"${samplesheet}\"" >> "$manifest_file"
            first=false
        fi
    done
    
    # Final close
    echo "" >> "$manifest_file"
    echo "}" >> "$manifest_file"
    
    print_status "Created run_manifest.json"
}

# Function to show usage
show_usage() {
    echo "Usage: $0 [workflow1] [workflow2] ..."
    echo ""
    echo "Generate samplesheets for nf-core workflows from FASTQ files"
    echo ""
    echo "Examples:"
    echo "  $0                          # Interactive mode, prompts for workflows"
    echo "  $0 mag metatdenovo          # Generate for specific workflows"
    echo "  $0 ampliseq                 # Generate for ampliseq only"
    echo ""
    echo "Available workflows: mag, metatdenovo, ampliseq, rnaseq, etc."
}

# Function to generate samplesheets
generate_samplesheets() {
    local run_id=$1
    
    print_status "Starting samplesheet generation for run: ${RUN_ID}"
    
    # Check if we have any .fastq.gz files
    shopt -s nullglob
    FASTQ_FILES=(*.fastq.gz)
    shopt -u nullglob

    if [ ${#FASTQ_FILES[@]} -eq 0 ]; then
        echo "${YELLOW}No FASTQ files found in the current directory.${NC}"
        echo "Please run this script from a directory containing your .fastq.gz files."
        exit 1
    fi

    echo "${GREEN}Found ${#FASTQ_FILES[@]} FASTQ files.${NC}"

    # Get available workflows from command line or use defaults
    if [ $# -gt 1 ]; then
        # Skip first argument (run_id) and use the rest as workflows
        shift
        WORKFLOWS=("$@")
        echo "${GREEN}Generating samplesheets for specified workflows: ${WORKFLOWS[*]}${NC}"
    else
        # Default workflows
        WORKFLOWS=("mag" "metatdenovo")
        
        # Try to detect available workflows from environment or prompt user
        echo -e "\n${YELLOW}Which workflows do you want to generate samplesheets for?${NC}"
        echo "Default: mag metatdenovo"
        echo "Available workflows: mag, metatdenovo, ampliseq, rnaseq, etc."
        echo -e "Enter workflow names separated by spaces (or press Enter for defaults): ${NC}"
        read -r user_workflows
        
        if [ -n "$user_workflows" ]; then
            IFS=' ' read -ra WORKFLOWS <<< "$user_workflows"
        fi
    fi

    echo "${GREEN}Generating samplesheets for workflows: ${WORKFLOWS[*]}${NC}"
    
    # Generate samplesheets for each workflow
    for workflow in "${WORKFLOWS[@]}"; do
        local samplesheet="samplesheet_${workflow}.csv"
        print_info "Generating ${samplesheet}..."
        
        # Create header based on workflow type
        case "$workflow" in
            mag|metatdenovo)
                echo "sample,fastq_1,fastq_2" > "$samplesheet"
                ;;
            ampliseq)
                echo "sampleID,forwardReads,reverseReads" > "$samplesheet"
                ;;
            rnaseq)
                echo "sample,fastq_1,fastq_2,strandedness" > "$samplesheet"
                ;;
            *)
                # Default format for unknown workflows
                echo "sample,fastq_1,fastq_2" > "$samplesheet"
                print_warning "Using default format for unknown workflow: $workflow"
                ;;
        esac
        
        # Process paired-end files
        local processed_samples=()
        for fastq in *.fastq.gz; do
            # Extract sample name and read direction
            local base=$(basename "$fastq" .fastq.gz)
            local sample=""
            local read=""
            
            if [[ "$base" =~ ^(.+)_(R[12])$ ]]; then
                sample="${BASH_REMATCH[1]}"
                read="${BASH_REMATCH[2]}"
            elif [[ "$base" =~ ^(.+)_([12])$ ]]; then
                sample="${BASH_REMATCH[1]}"
                read="R${BASH_REMATCH[2]}"
            else
                print_warning "Cannot determine read direction for: $fastq"
                continue
            fi
            
            # Skip if we've already processed this sample
            if [[ " ${processed_samples[@]} " =~ " ${sample} " ]]; then
                continue
            fi
            
            # Look for the paired file
            local r1_file=""
            local r2_file=""
            
            if [[ "$read" == "R1" ]]; then
                r1_file="$fastq"
                # Look for R2
                for pattern in "${sample}_R2.fastq.gz" "${sample}_2.fastq.gz"; do
                    if [[ -f "$pattern" ]]; then
                        r2_file="$pattern"
                        break
                    fi
                done
            else
                r2_file="$fastq"
                # Look for R1
                for pattern in "${sample}_R1.fastq.gz" "${sample}_1.fastq.gz"; do
                    if [[ -f "$pattern" ]]; then
                        r1_file="$pattern"
                        break
                    fi
                done
            fi
            
            if [[ -n "$r1_file" ]] && [[ -n "$r2_file" ]]; then
                # Add entry based on workflow type
                case "$workflow" in
                    mag|metatdenovo)
                        echo "${sample},${PWD}/${r1_file},${PWD}/${r2_file}" >> "$samplesheet"
                        ;;
                    ampliseq)
                        echo "${sample},${PWD}/${r1_file},${PWD}/${r2_file}" >> "$samplesheet"
                        ;;
                    rnaseq)
                        echo "${sample},${PWD}/${r1_file},${PWD}/${r2_file},auto" >> "$samplesheet"
                        ;;
                    *)
                        echo "${sample},${PWD}/${r1_file},${PWD}/${r2_file}" >> "$samplesheet"
                        ;;
                esac
                processed_samples+=("$sample")
                print_success "Added sample: $sample"
            else
                print_warning "Could not find pair for: $fastq"
            fi
        done
        
        local sample_count=$(( $(wc -l < "$samplesheet") - 1 ))
        print_status "Created $samplesheet with $sample_count samples"
    done
    
    # Create run manifest
    create_run_manifest "$run_id" "${WORKFLOWS[@]}"
}

# Main execution
main() {
    if [[ "$1" == "-h" ]] || [[ "$1" == "--help" ]]; then
        show_usage
        exit 0
    fi
    
    generate_samplesheets "$RUN_ID" "$@"
}

main "$@" 