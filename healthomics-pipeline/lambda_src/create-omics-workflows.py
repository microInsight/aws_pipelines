import boto3
import json
import cfnresponse
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

omics_client = boto3.client('omics')

def handler(event, context):
    request_type = event['RequestType']
    physical_resource_id = event.get('PhysicalResourceId', 'omics-workflows')
    
    try:
        if request_type == 'Delete':
            # Get the list of workflow IDs from the response data - need to pull ids from DynamoDB table
            workflow_ids = event.get('PhysicalResourceId', '').split(',')
            
            for workflow_id in workflow_ids:
                if workflow_id:
                    try:
                        omics_client.delete_workflow(id=workflow_id)
                        logger.info(f"Deleted workflow: {workflow_id}")
                    except Exception as e:
                        logger.warning(f"Could not delete workflow {workflow_id}: {str(e)}")
                        cfnresponse.send(event, context, cfnresponse.FAILED, {}, physical_resource_id)
                        return
            
            cfnresponse.send(event, context, cfnresponse.SUCCESS, {}, physical_resource_id)
            return
        
        # Handle Create and Update
        if request_type not in ['Create', 'Update']:
            logger.error(f"Unknown request type: {request_type}")
            cfnresponse.send(event, context, cfnresponse.FAILED, {}, physical_resource_id)
            return
            
        properties = event['ResourceProperties']
        workflow_config = properties['WorkflowConfig']
        code_bucket = properties['CodeBucketName']
        storage_capacity = int(properties['DefaultStorageCapacity'])
        
        # For Update events, preserve the existing PhysicalResourceId
        if request_type == 'Update':
            # Use the existing physical resource ID to maintain resource identity
            existing_physical_id = event.get('PhysicalResourceId', 'omics-workflows')
            logger.info(f"Update event - preserving PhysicalResourceId: {existing_physical_id}")
        
        created_workflows = []
        workflow_ids = []
        
        for workflow_spec in workflow_config:
            parts = workflow_spec.split(':')
            if len(parts) != 2:
                logger.warning(f"Invalid workflow spec: {workflow_spec}")
                cfnresponse.send(event, context, cfnresponse.FAILED, {}, physical_resource_id)
                return
            
            workflow_name = parts[0]
            workflow_version = parts[1]
            
            # Validate that both name and version are non-empty
            if not workflow_name or not workflow_version:
                logger.warning(f"Invalid workflow spec (empty name or version): {workflow_spec}")
                cfnresponse.send(event, context, cfnresponse.FAILED, {}, physical_resource_id)
                return
            
            # Format version for the workflow name (replace dots with hyphens)
            version_formatted = workflow_version.replace('.', '-')
            
            # Create workflow parameters
            workflow_params = {
                'name': f'nfcore-{workflow_name}-{version_formatted}',
                'description': f'nf-core {workflow_name} workflow version {workflow_version}',
                'definitionUri': f's3://{code_bucket}/{workflow_name}/nf-core-{workflow_name}_{workflow_version}.zip',
                'storageCapacity': storage_capacity,
                'tags': {
                    'Source': 'nf-core',
                    'Pipeline': workflow_name,
                    'Version': workflow_version
                }
            }
            
            try:
                # Check if workflow already exists
                existing_workflows = omics_client.list_workflows(
                    name=workflow_params['name']
                )
                
                if existing_workflows['items']:
                    workflow_id = existing_workflows['items'][0]['id']
                    logger.info(f"Workflow already exists: {workflow_params['name']} (ID: {workflow_id})")
                    
                    # For Update, you might want to update the workflow here if needed
                    # For example, update tags or other mutable properties
                    if request_type == 'Update':
                        logger.info(f"Update event - workflow {workflow_params['name']} already exists, no changes needed")
                else:
                    # Create the workflow
                    response = omics_client.create_workflow(**workflow_params)
                    workflow_id = response['id']
                    logger.info(f"Created workflow: {workflow_params['name']} (ID: {workflow_id})")
                
                workflow_ids.append(workflow_id)
                created_workflows.append({
                    'name': workflow_params['name'],
                    'id': workflow_id,
                    'spec': workflow_spec
                })
            
            except Exception as e:
                logger.error(f"Failed to create workflow {workflow_name}: {str(e)}")
                cfnresponse.send(event, context, cfnresponse.FAILED, {}, physical_resource_id)
                # Continue with other workflows
        
        # For Update requests, preserve the existing PhysicalResourceId
        if request_type == 'Update':
            physical_resource_id = event.get('PhysicalResourceId', ','.join(workflow_ids) if workflow_ids else 'no-workflows')
        else:
            # For Create requests, generate new PhysicalResourceId
            physical_resource_id = ','.join(workflow_ids) if workflow_ids else 'no-workflows'
        
        response_data = {
            'WorkflowCount': str(len(created_workflows)),
            'Workflows': json.dumps(created_workflows)
        }
        
        cfnresponse.send(event, context, cfnresponse.SUCCESS, response_data, physical_resource_id)
    
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        cfnresponse.send(event, context, cfnresponse.FAILED, {}, physical_resource_id)