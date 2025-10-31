import boto3
import os
import json
from datetime import datetime, timezone
from urllib.parse import urlparse

omics = boto3.client("omics")
s3 = boto3.client("s3")

def _utcstamp():
    return datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")

def _parse_s3_uri(uri: str):
    """
    Parse s3://bucket/key into (bucket, key).
    Raises ValueError for invalid URIs.
    """
    if not uri or not uri.startswith("s3://"):
        raise ValueError(f"Invalid S3 URI: {uri!r}")
    p = urlparse(uri)
    bucket = p.netloc
    key = p.path.lstrip("/")
    if not bucket or not key:
        raise ValueError(f"Invalid S3 URI: {uri!r}")
    return bucket, key

def _load_json_from_s3(uri: str) -> dict:
    bucket, key = _parse_s3_uri(uri)
    obj = s3.get_object(Bucket=bucket, Key=key)
    body = obj["Body"].read()
    try:
        return json.loads(body.decode("utf-8"))
    except Exception as e:
        raise ValueError(f"param_s3 JSON is invalid ({uri}): {e}")

def handler(event, context):
    """
    Launches an Omics workflow run using a parameters DOCUMENT built by
    merging the provided param_s3 JSON with the samplesheet path.

    Required event keys:
      - workflow_name
      - job_name
      - samplesheet_s3           (s3://... CSV/manifest)
      - param_s3                 (s3://... JSON)  <-- REQUIRED, loaded & merged
      - output_bucket            (target S3 bucket for Omics outputUri)
      - (omics_workflow_id OR omics_workflow_arn)

    Optional event keys:
      - omics_workflow_version
      - omics_role
      - run_group
    """
    # --- Extract inputs ---
    workflow_name   = (event.get("workflow_name") or "").strip()
    job_name        = (event.get("job_name") or "").strip()
    samplesheet_s3  = (event.get("samplesheet_s3") or "").strip()
    param_s3        = (event.get("param_s3") or "").strip()
    out_bucket      = (event.get("output_bucket") or os.environ.get("OUT_BUCKET") or "").strip()

    wf_id  = (event.get("omics_workflow_id") or "").strip() or None

    role_arn  = (event.get("omics_role") or "").strip() or None

    # --- Validate required fields ---
    required = {
        "workflow_name": workflow_name,
        "job_name": job_name,
        "samplesheet_s3": samplesheet_s3,
        "param_s3": param_s3,         
        "output_bucket": out_bucket,
        "role_arn": role_arn,
    }
    missing = [k for k, v in required.items() if not v]
    if missing:
        raise ValueError(f"Missing required fields: {', '.join(missing)}")

    if not (wf_id):
        raise ValueError("Missing workflow reference: provide omics_workflow_id.")

    # --- Build Omics outputUri (no 'outdir' inside parameters) ---
    output_uri = f"s3://{out_bucket}/{job_name}/{workflow_name}/"

    output_prefix = f"{job_name}/{workflow_name}/"

    # --- Load params DOCUMENT from S3 and inject the input path ---
    param_doc = _load_json_from_s3(param_s3)
    # Always ensure 'input' points at the samplesheet provided by the trigger
    param_doc["input"] = samplesheet_s3

    # --- Prepare Omics start_run arguments ---
    run_name = f"{workflow_name}-{job_name}-{_utcstamp()}"

    start_args = {
        "name": run_name,
        "parameters": param_doc, 
        "outputUri": output_uri,
        "storageType": "DYNAMIC",
    }
    if wf_id:
        start_args["workflowId"] = wf_id
    if role_arn:
        start_args["roleArn"] = role_arn

    # --- Start the run ---
    try:
        resp = omics.start_run(**start_args)
    except Exception as e:
        raise RuntimeError(f"Failed to start Omics run for {workflow_name}: {e}")

    return {
        "statusCode": 200,
        "workflow": workflow_name,
        "job_name": job_name,
        "omics_run_id": resp.get("id"),
        "output_uri": output_uri,
        "output_prefix": output_prefix,
        "parameters_source": param_s3,   # for traceability
    }