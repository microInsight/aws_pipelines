import boto3
import json
import os
import cfnresponse
import time

dynamodb = boto3.resource('dynamodb')
omics = boto3.client('omics')

def handler(event, context):
    try:
        if event['RequestType'] == 'Delete':
            # No action needed on delete
            cfnresponse.send(event, context, cfnresponse.SUCCESS, {})
            return
        
        table_name = event['ResourceProperties']['TableName']
        table = dynamodb.Table(table_name)
        
        workflow_specs = os.environ['WORKFLOW_CONFIG'].split(',')
        
        # Retry logic to wait for workflows to be created
        max_retries = 10
        retry_delay = 3  # Start with 3 seconds
        workflows = []
        
        if event['RequestType'] == 'Create':
            for retry in range(max_retries):
                try:
                    # List all workflows
                    workflows = omics.list_workflows()['items']
                    
                    # Check if we can find all expected workflows
                    expected_count = len(workflow_specs)
                    found_count = 0
                    
                    for spec in workflow_specs:
                        name, version = spec.split(':')
                        workflow_name = f"nfcore-{name}-{version.replace('.', '-')}"
                        
                        if any(w['name'] == workflow_name for w in workflows):
                            found_count += 1
                    
                    if found_count == expected_count:
                        print(f"Found all {expected_count} workflows after {retry + 1} attempts")
                        break
                    else:
                        print(f"Attempt {retry + 1}: Found {found_count}/{expected_count} workflows, retrying...")
                        time.sleep(retry_delay)
                        retry_delay = min(retry_delay * 1.5, 20)  # Exponential backoff, max 20 seconds
                        
                except Exception as e:
                    print(f"Error listing workflows on attempt {retry + 1}: {str(e)}")
                    if retry < max_retries - 1:
                        time.sleep(retry_delay)
                        retry_delay = min(retry_delay * 1.5, 20)
                    else:
                        raise
            
            if found_count < expected_count:
                raise Exception(f"Timeout waiting for workflows to be created after {max_retries} attempts. Found {found_count}/{expected_count}")
        else:
            # For Update requests, just list workflows without retry
            workflows = omics.list_workflows()['items']
        
        # Build configuration for each workflow
        workflow_configs = []
        
        for spec in workflow_specs:
            name, version = spec.split(':')
            # Find workflow with matching name
            # The workflow name format is: nfcore-{name}-{version with dots replaced by dashes}
            workflow_name = f"nfcore-{name}-{version.replace('.', '-')}"
            
            matching_workflow = None
            for wf in workflows:
                if wf['name'] == workflow_name:
                    matching_workflow = wf
                    break
            
            if matching_workflow:
                workflow_configs.append({
                    'name': name,
                    'version': version,
                    'arn': matching_workflow['arn']
                })
            else:
                print(f"Warning: Could not find workflow {workflow_name}")
        
        # Store configuration
        table.put_item(Item={
            'id': 'workflows',
            'workflows': workflow_configs,
            'omics_role': event['ResourceProperties']['OmicsRoleArn'],
            'run_group': event['ResourceProperties']['RunGroupId']
        })
        
        cfnresponse.send(event, context, cfnresponse.SUCCESS, {
            'WorkflowCount': str(len(workflow_configs))
        })
    except Exception as e:
        print(f"Error: {str(e)}")
        cfnresponse.send(event, context, cfnresponse.FAILED, {})