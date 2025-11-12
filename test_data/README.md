## Manual Submitting nf-core MAG and metatdenovo Pipelines

This guide walks you through preparing inputs, uploading sample data to Amazon S3, and submitting runs to AWS HealthOmics for both the nf-core MAG and metatdenovo pipelines. It also covers monitoring runs and retrieving outputs.

### Prerequisites
- **AWS CLI v2** installed and configured (`aws configure`) with credentials that can:
  - Assume or use the specified **IAM role** for HealthOmics
  - Read from your input S3 bucket(s)
  - Write to the output S3 bucket/prefix
- **AWS account and region** where the workflows are deployed
- **Workflow IDs and role ARN** (examples provided below)
- Local copies of the parameter files in this repo:
  - `mag-test-input.json`
  - `meta-test-input.json`
- Use the S3 input bucket `s3://healthomics-nfcore-input-125434852769` (choose a prefix as needed)

### Repo Layout (relevant files)
- `./mag-artifacts/mag-test-input.json` – example parameters for MAG
- `./meta-artifacts/meta-test-input.json` – example parameters for metatdenovo

## 1) Prepare and upload input samples to S3

The pipelines expect S3 URIs for your input files (e.g., FASTQ, assemblies). Organize your samples locally first, then upload them to S3.

### 1.1 Organize your local samples
- Ensure paired-end FASTQs are consistently named, e.g.,
  - `sampleA_R1.fastq.gz`
  - `sampleA_R2.fastq.gz`
- Place all samples for a run under a single directory, e.g., `/data/omics/samples/`.
- Create samples sheets for both mag and metatdenovo runs. Refer to the example mag_samplesheet.csv and meta_samplesheet.csv for format.

### 1.2 Choose an S3 input prefix
Use the input bucket `s3://healthomics-nfcore-input-125434852769`.

Choose a prefix for this run's inputs, e.g., `s3://healthomics-nfcore-input-125434852769/mag/inputs/` or `s3://healthomics-nfcore-input-125434852769/metatdenovo/inputs/`.

### 1.3 Upload samples to S3

```bash
# MAG inputs example
aws s3 sync /data/omics/samples/ s3://healthomics-nfcore-input-125434852769/mag/inputs/ \
  --exclude "*" --include "*.fastq" --include "*.fastq.gz"

# metatdenovo inputs example
aws s3 sync /data/omics/samples/ s3://healthomics-nfcore-input-125434852769/metatdenovo/inputs/ \
  --exclude "*" --include "*.fastq" --include "*.fastq.gz"
```

Verify the upload:

```bash
aws s3 ls s3://healthomics-nfcore-input-125434852769/mag/inputs/ --recursive
aws s3 ls s3://healthomics-nfcore-input-125434852769/metatdenovo/inputs/ --recursive
```

### 1.4 Update parameter JSONs to reference your S3 inputs
Edit `mag-test-input.json` and `meta-test-input.json` so that any fields that refer to input files point to your S3 URIs.


## 2) Choose output location

Use the following output S3 bucket/prefixes:
- MAG outputs: `s3://healthomics-nfcore-output-125434852769/output/mag`
- metatdenovo outputs: `s3://healthomics-nfcore-output-125434852769/output/meta`

You can list contents later with:

```bash
aws s3 ls s3://healthomics-nfcore-output-125434852769/output/mag --recursive
aws s3 ls s3://healthomics-nfcore-output-125434852769/output/meta --recursive
```

## 3) Submit MAG pipeline run

Below is a known-good example. Adjust names/paths as needed. Ensure your `mag-test-input.json` points to valid S3 inputs.

```bash
aws omics start-run \
  --name mag_test_run_27 \
  --role-arn arn:aws:iam::125434852769:role/healthomics-head-template-HealthOm-OmicsServiceRole-QdL88Njr1el5 \
  --workflow-id 7428658 \
  --parameters file://mag-test-input.json \
  --output-uri s3://healthomics-nfcore-output-125434852769/output/mag \
  --storage-type DYNAMIC \
  --cache-id 9633171 \
  --workflow-version-name update-busco
```

Key flags:
- `--name`: Friendly name for the run
- `--role-arn`: IAM role used by HealthOmics to access S3 and other services
- `--workflow-id`: The deployed workflow ID for MAG
- `--parameters`: Local JSON file (prefixed with `file://`) with inputs/options
- `--output-uri`: S3 prefix for run outputs
- `--storage-type`: Use `DYNAMIC` unless you have a reason to choose differently
- `--cache-id`: Optional; reuse cached Docker layers/assets for faster startup if available
- `--workflow-version-name`: Optional; target a specific workflow version label

The command will return a `runId`. Save it for monitoring and auditing.

## 4) Submit metatdenovo pipeline run

Below is a known-good example. Ensure your `meta-test-input.json` points to valid S3 inputs.

```bash
aws omics start-run \
  --name meta_test_run_4 \
  --role-arn arn:aws:iam::125434852769:role/healthomics-head-template-HealthOm-OmicsServiceRole-QdL88Njr1el5 \
  --workflow-id 5800110 \
  --parameters file://meta-test-input.json \
  --output-uri s3://healthomics-nfcore-output-125434852769/output/meta \
  --storage-type DYNAMIC \
  --cache-id 9633171
```

## 5) Monitor run status

List recent runs (filter by name if desired):

```bash
aws omics list-runs --max-results 20
```

Get details for a specific run:

```bash
aws omics get-run --id <runId>
```

Optional: find a run by name and fetch its ID:

```bash
RUN_ID=$(aws omics list-runs \
  --query "runs[?name==\`mag_test_run_27\`].id | [0]" \
  --output text)

aws omics get-run --id "$RUN_ID"
```

## 6) Retrieve outputs

List outputs and optionally sync them locally:

```bash
# List
aws s3 ls s3://healthomics-nfcore-output-125434852769/output/mag --recursive
aws s3 ls s3://healthomics-nfcore-output-125434852769/output/meta --recursive

# Sync to local directories
aws s3 sync s3://healthomics-nfcore-output-125434852769/output/mag ./outputs/mag/
aws s3 sync s3://healthomics-nfcore-output-125434852769/output/meta ./outputs/meta/
```

## Notes & Tips

- **IAM Role**: The `--role-arn` must allow HealthOmics to assume it and must grant access to referenced S3 buckets/prefixes. Ensure the trust policy includes `omics.amazonaws.com`.
- **Parameters JSON**: Keep the `file://` prefix when providing local files to `--parameters`.
- **S3 paths**: Use full S3 URIs in parameter files for all inputs expected by the workflows.
- **Caching**: If you see slow starts, reusing a valid `--cache-id` may help; remove it if it causes issues.
- **Region**: If your resources are not in the default CLI region, add `--region <aws-region>` to all commands.
- **Validation**: If available, validate your parameter JSON against the workflow schema before running.

## Example: End-to-end quick checklist
1. Upload samples to S3 (`aws s3 sync ...`)
2. Update `mag-test-input.json` / `meta-test-input.json` to point to your S3 inputs
3. Run MAG or metatdenovo submission command(s)
4. Monitor with `aws omics get-run --id <runId>`
5. Download results from the output S3 prefix


