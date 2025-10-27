import boto3
import json

omics = boto3.client('omics')

def handler(event, context):
    launched_workflows = event['launched_workflows']
    
    workflow_statuses = []
    all_complete = True
    any_failed = False
    
    for workflow in launched_workflows:
        try:
            status = omics.get_run(id=workflow['run_id'])
            state = status['status']
            
            workflow_statuses.append({
                'workflow_name': workflow['workflow_name'],
                'run_id': workflow['run_id'],
                'status': state,
                'startTime': str(status.get('startTime', '')),
                'stopTime': str(status.get('stopTime', '')),
                'statusMessage': status.get('statusMessage', '')
            })
            
            if state not in ['COMPLETED', 'FAILED', 'CANCELLED']:
                all_complete = False
            
            if state in ['FAILED', 'CANCELLED']:
                any_failed = True
        except Exception as e:
            print(f"Error polling {workflow['workflow_name']}: {str(e)}")
            workflow_statuses.append({
                'workflow_name': workflow['workflow_name'],
                'run_id': workflow['run_id'],
                'status': 'UNKNOWN',
                'startTime': '',
                'stopTime': '',
                'statusMessage': f'Error: {str(e)}'
            })
            any_failed = True
    
    return {
        'statusCode': 200,
        'run_id': event['run_id'],
        'launched_workflows': launched_workflows,
        'workflow_statuses': workflow_statuses,
        'all_complete': all_complete,
        'any_failed': any_failed
    }