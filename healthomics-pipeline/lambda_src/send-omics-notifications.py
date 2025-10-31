
import os
import re
import boto3
from datetime import datetime

sns = boto3.client("sns")
omics = boto3.client("omics")

def _fmt_ts(ts):
    if not ts:
        return "n/a"
    return str(ts)

def _parse_s3(uri):
    if not uri or not isinstance(uri, str):
        return (None, None)
    m = re.match(r"^s3://([^/]+)/?(.*)$", uri)
    if not m:
        return (None, None)
    return (m.group(1), m.group(2))

def _infer_region_from_arn(arn):
    if not arn or ":" not in arn:
        return None
    parts = arn.split(":")
    return parts[3] if len(parts) > 3 else None

def handler(event, context):
    """
    Single-workflow HealthOmics notification.
    Input: {"omics_run_id": "<run-id>"}
    Env: SNS_TOPIC_ARN (required)
    """
    topic_arn = os.environ["SNS_TOPIC_ARN"]
    omics_run_id = event.get("omics_run_id"),
    if not omics_run_id:
        raise ValueError("Missing required 'omics_run_id' in event")

    run = omics.get_run(id=omics_run_id)

    arn = run.get("arn")
    region = _infer_region_from_arn(arn)
    status = run.get("status") or "COMPLETED"
    status_msg = run.get("statusMessage") or ""
    start_time = run.get("startTime")
    stop_time = run.get("stopTime")
    name = run.get("name") or ""
    params = run.get("parameters") or {}
    input_uri = params.get("input")
    outdir = params.get("outdir")
    param_json = params.get("params")

    # Job name from outdir: s3://<bucket>/<job_name>/<workflow>/...
    job_name = event.get("job_name")

    workflow_name = event.get("workflow_name")
    out_bucket = event.get("output_bucket")
    out_prefix = event.get("output_prefix")
    in_bucket = event.get("bucket")
    in_prefix = event.get("prefix")

    subject = f"[{str(workflow_name).upper()}] {status} - {omics_run_id}"
    lines = [
        "AWS HealthOmics Workflow Notification",
        "====================================",
        "",
        f"Workflow:       {workflow_name}",
        f"Job Name:       {job_name or 'n/a'}",
        f"Run Name:       {name or 'n/a'}",
        f"Omics Run ARN:  {arn or 'n/a'}",
        f"Omics Run ID:   {run.get('id') or omics_run_id}",
        f"Status:         {status}",
        f"Started:        {_fmt_ts(start_time)}",
        f"Finished:       {_fmt_ts(stop_time)}",
        f"Message:        {status_msg or 'n/a'}",
        "",
        "IO Context:",
        f"- Input:            {input_uri or 'n/a'}",
        f"- Params JSON:      {param_json or 'n/a'}",
        f"- Output (outdir):  {outdir or 'n/a'}",
    ]

    if region and run.get("id"):
        lines += [
            "",
            "Useful Links:",
            f"- Omics Run: https://console.aws.amazon.com/omics/home?region={region}#/runs/{run['id']}",
        ]
    if in_bucket and in_prefix and region:
        lines.append(f"- Input S3:  https://s3.console.aws.amazon.com/s3/buckets/{in_bucket}?prefix={in_prefix}&region={region}")
    if out_bucket and out_prefix and region:
        lines.append(f"- Output S3: https://s3.console.aws.amazon.com/s3/buckets/{out_bucket}?prefix={out_prefix}&region={region}")

    lines += [
        "",
        "---",
        "This notification has been sent to all configured email addresses.",
    ]

    sns.publish(
        TopicArn=topic_arn,
        Subject=subject[:100],
        Message="\n".join(lines),
    )

    return {"statusCode": 200, "message": "Notification sent", "runId": omics_run_id}