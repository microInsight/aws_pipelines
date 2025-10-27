import boto3
import json
import os
from datetime import datetime
import time
import random

omics = boto3.client('omics')
s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

def retry_with_backoff(func, max_retries=5, base_delay=1):
    """Execute function with exponential backoff retry on throttling errors"""
    for attempt in range(max_retries):
        try:
            return func()
        except Exception as e:
            error_code = getattr(e, 'response', {}).get('Error', {}).get('Code', '')
            if error_code in ['ThrottlingException', 'TooManyRequestsException']:
                if attempt < max_retries - 1:
                    # Exponential backoff with jitter
                    delay = base_delay * (2 ** attempt) + random.uniform(0, 1)
                    print(f"Throttled. Retrying in {delay:.2f} seconds... (attempt {attempt + 1}/{max_retries})")
                    time.sleep(delay)
                else:
                    raise
            else:
                raise

def handler(event, context):
    # Get workflow configuration from DynamoDB
    table = dynamodb.Table(os.environ['CONFIG_TABLE'])
    config = table.get_item(Key={'id': 'workflows'})['Item']
    
    manifest = event['manifest']
    run_id = manifest['run_id']
    output_bucket = event['output_bucket']
    
    launched_workflows = []
    
    # Add delay between workflow launches to avoid throttling
    # Current quota is 0.1 TPS = 1 request per 10 seconds
    launch_delay = 10  # seconds between launches
    
    # Launch each configured workflow that has a samplesheet
    for idx, workflow in enumerate(config['workflows']):
        workflow_name = workflow['name']
        workflow_arn = workflow['arn']
        samplesheet_key = f'samplesheet_{workflow_name}.csv'
        
        # Check if this workflow should be run
        if samplesheet_key not in manifest:
            print(f"No samplesheet found for {workflow_name}, skipping")
            continue
        
        # Add delay between launches (except for the first one)
        if idx > 0 and launched_workflows:
            print(f"Waiting {launch_delay} seconds before launching next workflow...")
            time.sleep(launch_delay)
        
        # Prepare parameters
        print(f"Launching {workflow_name} workflow")
        params = {
            'input': f"s3://{event['bucket']}/{run_id}/{samplesheet_key}",
            'outdir': f"s3://{output_bucket}/{run_id}/{workflow_name}/"
        }
        
        # Start the run with retry logic
        try:
            def start_run():
                return omics.start_run(
                    workflowId=workflow_arn.split('/')[-1],
                    name=f"{workflow_name}-{run_id}",
                    roleArn=config['omics_role'],
                    parameters=params,
                    outputUri=f"s3://{output_bucket}/{run_id}/{workflow_name}/",
                    runGroupId=config['run_group'],
                    tags={
                        'run_id': run_id,
                        'workflow': workflow_name,
                        'start_time': datetime.now().isoformat()
                    }
                )
            
            response = retry_with_backoff(start_run)
            
            launched_workflows.append({
                'workflow_name': workflow_name,
                'run_id': response['id'],
                'arn': response['arn']
            })
            print(f"Successfully launched {workflow_name} workflow")
        except Exception as e:
            print(f"Failed to launch {workflow_name}: {str(e)}")
    
    if not launched_workflows:
        raise Exception("No workflows were launched. Check samplesheet availability.")
    
    return {
        'statusCode': 200,
        'run_id': run_id,
        'launched_workflows': launched_workflows,
        'workflow_count': len(launched_workflows)
    }