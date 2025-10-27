import boto3
import json
import os

sns = boto3.client('sns')

def handler(event, context):
    topic_arn = os.environ['SNS_TOPIC_ARN']
    
    run_id = event['run_id']
    workflow_statuses = event['workflow_statuses']
    any_failed = event['any_failed']
    
    subject = f"HealthOmics Run {run_id} - {'FAILED' if any_failed else 'COMPLETED'}"
    
    message = f"""
HealthOmics Pipeline Execution Summary
=====================================

Run ID: {run_id}
Total Workflows: {len(workflow_statuses)}

Workflow Results:
"""
    
    for status in workflow_statuses:
        message += f"""

{status['workflow_name'].upper()} Pipeline:
- Status: {status['status']}
- Run ID: {status['run_id']}
- Start Time: {status['startTime']}
- Stop Time: {status['stopTime']}
- Message: {status['statusMessage']}
"""
    
    # Summary statistics
    completed = sum(1 for s in workflow_statuses if s['status'] == 'COMPLETED')
    failed = sum(1 for s in workflow_statuses if s['status'] in ['FAILED', 'CANCELLED'])
    
    message += f"""

Summary:
- Completed: {completed}/{len(workflow_statuses)}
- Failed/Cancelled: {failed}/{len(workflow_statuses)}

Overall Status: {'One or more workflows failed' if any_failed else 'All workflows completed successfully'}

---
This notification has been sent to all configured email addresses.
    """
    
    sns.publish(
        TopicArn=topic_arn,
        Subject=subject,
        Message=message
    )
    
    return {
        'statusCode': 200,
        'message': 'Notification sent successfully to all subscribers'
    }