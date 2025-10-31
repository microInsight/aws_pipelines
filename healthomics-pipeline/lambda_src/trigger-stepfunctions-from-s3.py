import boto3
import json
import os
import urllib.parse
from datetime import datetime, timezone

s3 = boto3.client('s3')
sfn = boto3.client('stepfunctions')
dynamodb_client = boto3.client('dynamodb')
dynamodb_resource = boto3.resource('dynamodb')

def _list_json_params(bucket: str, prefix: str):
    """Return a mapping of basename->s3:// uri and simple workflow mapping."""
    params_map = {}
    workflow_params = {}
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if key.endswith('run_manifest.json'):
                continue
            if not key.lower().endswith('.json'):
                continue
            bname = key.rsplit('/', 1)[-1]
            uri = f"s3://{bucket}/{key}"
            params_map[bname] = uri
            low = bname.lower()
            if 'metatdenovo' in low:
                workflow_params['metatdenovo'] = uri
            elif 'mag' in low:
                workflow_params['mag'] = uri
            elif 'taxprofiler' in low:
                workflow_params['taxprofiler'] = uri
    return params_map, workflow_params

def _find_samplesheet_uri(bucket: str, prefix: str, workflow: str):
    """Find samplesheet_<workflow>.csv in prefix and return its s3 uri or None."""
    target = f"samplesheet_{workflow}.csv"
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if key.rsplit('/', 1)[-1] == target:
                return f"s3://{bucket}/{key}"
    return None

def _parse_workflow_list_from_attribute_value(av_list):
    """
    Parse the AttributeValue-style list you see in the console into
    [{'name':..., 'arn':..., 'version':...}, ...]
    """
    items = []
    for el in av_list or []:
        m = el.get('M', {})
        name = (m.get('name', {}).get('S') or '').strip()
        arn = (m.get('arn', {}).get('S') or '').strip()
        version = (m.get('version', {}).get('S') or '').strip()
        if name and arn:
            items.append({'name': name, 'arn': arn, 'version': version})
    return items

def _extract_id_from_arn(arn: str) -> str | None:
    # arn:aws:omics:region:acct:workflow/123456 -> "123456"
    try:
        return arn.split('/')[-1]
    except Exception:
        return None

def _get_workflows_from_ddb():
    """
    Reads the single 'workflows' record and returns:
      - wf_by_name: { 'mag': {'id': '4734365', 'arn': '...', 'version': '3.1.0'}, ... }
      - omics_role: str | None
      - run_group: str | None
    Handles both AttributeValue-form and native Python-form items.
    """
    table_name = os.environ['WORKFLOWS_TABLE']
    pk_name = os.environ.get('WORKFLOWS_PK_NAME', 'id')
    pk_value = os.environ.get('WORKFLOWS_PK_VALUE', 'workflows')

    # First try low-level client (always returns AttributeValue format)
    resp = dynamodb_client.get_item(
        TableName=table_name,
        Key={pk_name: {'S': pk_value}}
    )
    item = resp.get('Item')
    workflows = []
    omics_role = None
    run_group = None

    if item:
        # AttributeValue format branch
        if 'workflows' in item and 'L' in item['workflows']:
            workflows = _parse_workflow_list_from_attribute_value(item['workflows']['L'])
        if 'omics_role' in item:
            omics_role = item['omics_role'].get('S') or item['omics_role'].get('SS', [None])[0]
        if 'run_group' in item:
            run_group = item['run_group'].get('S') or item['run_group'].get('SS', [None])[0]
    else:
        # Fallback to high-level resource (native Python dicts)
        table = dynamodb_resource.Table(table_name)
        resp2 = table.get_item(Key={pk_name: pk_value})
        py_item = resp2.get('Item') or {}
        workflows = py_item.get('workflows') or []
        omics_role = py_item.get('omics_role')
        run_group = py_item.get('run_group')

        # If workflows are still in AttributeValue form (console copy), normalize
        if workflows and isinstance(workflows[0], dict) and 'M' in workflows[0]:
            workflows = _parse_workflow_list_from_attribute_value(workflows)

    # Build lookup by lowercase name with id/arn/version
    wf_by_name = {}
    for wf in workflows:
        name = str(wf.get('name', '')).strip().lower()
        arn = str(wf.get('arn', '')).strip()
        version = str(wf.get('version', '')).strip() if wf.get('version') is not None else None
        if not name or not arn:
            continue
        wf_by_name[name] = {
            'id': _extract_id_from_arn(arn),
            'arn': arn,
            'version': version
        }

    return wf_by_name, omics_role, run_group

def handler(event, context):
    state_machine_arn = os.environ['STATE_MACHINE_ARN']
    out_bucket = os.environ.get('OUT_BUCKET')

    # pull workflow ids/arns/versions + role/run group from DynamoDB
    wf_lookup, omics_role, run_group = _get_workflows_from_ddb()

    launched = []

    for record in event.get('Records', []):
        bucket = record['s3']['bucket']['name']
        key = urllib.parse.unquote_plus(record['s3']['object']['key'])

        prefix = key.rsplit('/', 1)[0] + '/'
        job_name = prefix.strip('/').split('/')[-1]

        # Load manifest
        obj = s3.get_object(Bucket=bucket, Key=key)
        manifest = json.loads(obj['Body'].read().decode('utf-8'))
        workflows = manifest.get('workflows') or []

        # Discover parameter JSONs
        params_map, workflow_params = _list_json_params(bucket, prefix)

        # For each workflow, start its OWN Step Functions execution with only that workflow's context
        for wf in workflows:
            wf = str(wf).lower()
            samplesheet_uri = _find_samplesheet_uri(bucket, prefix, wf)
            if not samplesheet_uri:
                # skip workflows without samplesheets
                continue

            # choose a param json if present
            param_uri = workflow_params.get(wf)
            if not param_uri:
                for bname, uri in params_map.items():
                    if wf in bname.lower():
                        param_uri = uri
                        break

            # Lookup Omics workflow info for this wf name
            wf_info = wf_lookup.get(wf, {})

            step_params = {
                'stateMachineArn': state_machine_arn,
                'name': f"{job_name}-{wf}-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}",
                'input': {
                    'workflow_name': wf,
                    'job_name': job_name,
                    'bucket': bucket,
                    'prefix': prefix,
                    'samplesheet_s3': samplesheet_uri,
                    'param_s3': param_uri,
                    'output_bucket': out_bucket,
                    'omics_workflow_id': wf_info.get('id'),
                    'omics_workflow_arn': wf_info.get('arn'),
                    'omics_workflow_version': wf_info.get('version'),
                    'omics_role': omics_role,
                    'run_group': run_group,
                }
            }

            # Step Functions requires input to be JSON-encoded string
            step_params['input'] = json.dumps(step_params['input'])

            resp = sfn.start_execution(**step_params)
            launched.append({'workflow': wf, 'executionArn': resp['executionArn']})

    if not launched:
        # no workflows launched
        return {'statusCode': 200, 'message': 'No matching workflows to launch'}

    return {'statusCode': 200, 'launched': launched}
