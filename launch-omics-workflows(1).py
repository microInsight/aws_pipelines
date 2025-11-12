import boto3
import os
import json
from datetime import datetime, timezone

omics = boto3.client('omics')

def handler(event, context):
    """
    Launch exactly one Omics workflow run, using the single-workflow context
    passed from the Step Functions state machine.
    Expected event keys:
      - workflow_name: e.g., "mag" or "metatdenovo"
      - job_name
      - bucket
      - prefix
      - samplesheet_s3: s3 uri for the samplesheet
      - param_s3: s3 uri for the workflow parameters json (optional)
      - output_bucket
    """
    workflow_name = event.get('workflow_name')
    job_name = event.get('job_name')
    samplesheet_s3 = event.get('samplesheet_s3')
    param_s3 = event.get('param_s3')
    out_bucket = event.get('output_bucket') or os.environ.get('OUT_BUCKET')

    if not workflow_name or not job_name or not samplesheet_s3:
        raise ValueError("Missing required fields: workflow_name, run_id, samplesheet_s3")

    # Build parameters for the workflow
    params = {
        'input': samplesheet_s3,
        'outdir': f"s3://{out_bucket}/{job_name}/{workflow_name}/"
    }
    if param_s3:
        params['params'] = param_s3

    # Look up the workflow id/arn from env or map by name
    # Expect WORKFLOW_ARN_MAP env var containing JSON like {"mag":"arn:...","metatdenovo":"arn:..."}
    wf_map_json = os.environ.get('WORKFLOW_ARN_MAP')
    if not wf_map_json:
        raise ValueError("WORKFLOW_ARN_MAP env not set")
    wf_map = json.loads(wf_map_json)
    workflow_id = wf_map.get(workflow_name)
    if not workflow_id:
        raise ValueError(f"No workflow ARN/ID found for '{workflow_name}' in WORKFLOW_ARN_MAP")

    # Start a single run
    name = f"{workflow_name}-{job_name}-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"
    try:
        resp = omics.start_run(
            workflowId=workflow_id,
            name=name,
            parameters=params,
        )
    except Exception as e:
        raise RuntimeError(f"Failed to start Omics run for {workflow_name}: {e}")

    return {
        'statusCode': 200,
        'workflow': workflow_name,
        'job_name': job_name,
        'omics_run_arn': resp.get('arn') or resp.get('runArn') or resp,
    }
