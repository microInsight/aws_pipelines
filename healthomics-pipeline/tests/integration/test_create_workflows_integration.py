import unittest
import boto3
import os
import sys
import json
from moto import mock_aws
from unittest.mock import patch, MagicMock, Mock
import requests

# Add lambda source to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../lambda_src'))


@mock_aws
class TestCreateWorkflowsIntegration(unittest.TestCase):
    """Integration tests for create-omics-workflows Lambda (CloudFormation custom resource)"""
    
    def setUp(self):
        """Set up test fixtures"""
        # Create S3 bucket for workflow code
        self.s3 = boto3.client('s3', region_name='us-east-1')
        self.code_bucket = 'omics-workflow-code-bucket'
        self.s3.create_bucket(Bucket=self.code_bucket)
        
        # Upload workflow bundles to S3
        self.s3.put_object(
            Bucket=self.code_bucket,
            Key='nf-core-mag_4.0.0.zip',
            Body=b'Mock workflow bundle for MAG'
        )
        
        self.s3.put_object(
            Bucket=self.code_bucket,
            Key='nf-core-metatdenovo_1.2.0.zip',
            Body=b'Mock workflow bundle for metatdenovo'
        )
        
        # Create lambda context
        self.context = Mock()
        self.context.log_stream_name = "test-log-stream"
        self.context.function_name = "create-omics-workflows"
        
        # Base CloudFormation event
        self.base_event = {
            'RequestType': 'Create',
            'ResponseURL': 'https://cloudformation-custom-resource-response-useast1.s3.amazonaws.com/test',
            'StackId': 'arn:aws:cloudformation:us-east-1:123456789012:stack/test-stack/test-id',
            'RequestId': 'test-request-id',
            'ResourceType': 'Custom::CreateOmicsWorkflows',
            'LogicalResourceId': 'CreateWorkflows',
            'ResourceProperties': {
                'ServiceToken': 'arn:aws:lambda:us-east-1:123456789012:function:create-omics-workflows',
                'WorkflowConfig': ['mag:4.0.0', 'metatdenovo:1.2.0'],
                'CodeBucketName': self.code_bucket,
                'DefaultStorageCapacity': '100'
            }
        }
        
        # Mock cfnresponse
        self.mock_cfnresponse = MagicMock()
        self.mock_cfnresponse.SUCCESS = "SUCCESS"
        self.mock_cfnresponse.FAILED = "FAILED"
    
    def tearDown(self):
        """Clean up after tests"""
        # Clear any imported modules
        if 'create_workflows' in sys.modules:
            del sys.modules['create_workflows']
    
    def _import_handler_with_mocked_omics(self, omics_behaviors):
        """Import handler with mocked Omics client"""
        mock_omics_client = MagicMock()
        
        # Configure Omics client behaviors
        if 'list_workflows' in omics_behaviors:
            mock_omics_client.list_workflows.return_value = omics_behaviors['list_workflows']
        if 'create_workflow' in omics_behaviors:
            if isinstance(omics_behaviors['create_workflow'], list):
                mock_omics_client.create_workflow.side_effect = omics_behaviors['create_workflow']
            else:
                mock_omics_client.create_workflow.return_value = omics_behaviors['create_workflow']
        if 'delete_workflow' in omics_behaviors:
            mock_omics_client.delete_workflow.return_value = omics_behaviors['delete_workflow']
        
        # Use the real moto S3 client
        mock_s3_client = self.s3
        
        with patch.dict(sys.modules, {'cfnresponse': self.mock_cfnresponse}), \
             patch('boto3.client') as mock_client:
            
            def client_side_effect(service_name, **kwargs):
                if service_name == 'omics':
                    return mock_omics_client
                elif service_name == 's3':
                    return mock_s3_client
                else:
                    # For any other service, return a mock
                    return MagicMock()
            
            mock_client.side_effect = client_side_effect
            
            # Clear any previously imported module
            if 'create_workflows' in sys.modules:
                del sys.modules['create_workflows']
            
            # Import the module
            import importlib.util
            spec = importlib.util.spec_from_file_location(
                "create_workflows", 
                os.path.join(os.path.dirname(__file__), '../../lambda_src/create-omics-workflows.py')
            )
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            
            return module.handler, mock_omics_client
    
    def test_create_workflows_success(self):
        """Test successful creation of multiple workflows"""
        omics_behaviors = {
            'list_workflows': {'items': []},  # No existing workflows
            'create_workflow': [
                {'id': 'workflow-mag-123', 'arn': 'arn:aws:omics:us-east-1:123:workflow/mag-123'},
                {'id': 'workflow-meta-456', 'arn': 'arn:aws:omics:us-east-1:123:workflow/meta-456'}
            ]
        }
        
        handler, mock_omics = self._import_handler_with_mocked_omics(omics_behaviors)
        handler(self.base_event, self.context)
        
        # Verify workflows were created
        self.assertEqual(mock_omics.create_workflow.call_count, 2)
        
        # Check first workflow creation
        mag_call = mock_omics.create_workflow.call_args_list[0][1]
        self.assertEqual(mag_call['name'], 'nfcore-mag-4-0-0')
        self.assertEqual(mag_call['definitionUri'], f's3://{self.code_bucket}/nf-core-mag_4.0.0.zip')
        self.assertEqual(mag_call['storageCapacity'], 100)
        
        # Check second workflow creation
        meta_call = mock_omics.create_workflow.call_args_list[1][1]
        self.assertEqual(meta_call['name'], 'nfcore-metatdenovo-1-2-0')
        
        # Verify CloudFormation response
        self.mock_cfnresponse.send.assert_called_once()
        cfn_args = self.mock_cfnresponse.send.call_args[0]
        self.assertEqual(cfn_args[2], self.mock_cfnresponse.SUCCESS)
        self.assertEqual(cfn_args[4], 'workflow-mag-123,workflow-meta-456')  # Physical resource ID
        
        # Check response data
        response_data = cfn_args[3]
        self.assertEqual(response_data['WorkflowCount'], '2')
        workflows = json.loads(response_data['Workflows'])
        self.assertEqual(len(workflows), 2)
    
    def test_workflows_already_exist(self):
        """Test handling when workflows already exist"""
        # Need to set up the mock to return the correct workflow when searched by name
        def list_workflows_side_effect(name=None, **kwargs):
            if name == 'nfcore-mag-4-0-0':
                return {'items': [{'id': 'existing-mag-123', 'name': 'nfcore-mag-4-0-0'}]}
            elif name == 'nfcore-metatdenovo-1-2-0':
                return {'items': [{'id': 'existing-meta-456', 'name': 'nfcore-metatdenovo-1-2-0'}]}
            else:
                return {'items': []}
        
        omics_behaviors = {
            'list_workflows': MagicMock(side_effect=list_workflows_side_effect)
        }
        
        handler, mock_omics = self._import_handler_with_mocked_omics(omics_behaviors)
        mock_omics.list_workflows = omics_behaviors['list_workflows']
        handler(self.base_event, self.context)
        
        # Verify no new workflows were created
        mock_omics.create_workflow.assert_not_called()
        
        # Verify CloudFormation response includes existing workflow IDs
        cfn_args = self.mock_cfnresponse.send.call_args[0]
        self.assertEqual(cfn_args[2], self.mock_cfnresponse.SUCCESS)
        self.assertEqual(cfn_args[4], 'existing-mag-123,existing-meta-456')
    
    def test_delete_workflows(self):
        """Test deletion of workflows"""
        delete_event = self.base_event.copy()
        delete_event['RequestType'] = 'Delete'
        delete_event['PhysicalResourceId'] = 'workflow-mag-123,workflow-meta-456'
        
        omics_behaviors = {
            'delete_workflow': {}
        }
        
        handler, mock_omics = self._import_handler_with_mocked_omics(omics_behaviors)
        handler(delete_event, self.context)
        
        # Verify workflows were deleted
        self.assertEqual(mock_omics.delete_workflow.call_count, 2)
        mock_omics.delete_workflow.assert_any_call(id='workflow-mag-123')
        mock_omics.delete_workflow.assert_any_call(id='workflow-meta-456')
        
        # Verify CloudFormation response
        cfn_args = self.mock_cfnresponse.send.call_args[0]
        self.assertEqual(cfn_args[2], self.mock_cfnresponse.SUCCESS)
    
    def test_partial_creation_failure(self):
        """Test handling when some workflow creations fail"""
        omics_behaviors = {
            'list_workflows': {'items': []},
            'create_workflow': [
                {'id': 'workflow-mag-123', 'arn': 'arn:aws:omics:us-east-1:123:workflow/mag-123'},
                Exception('Service limit exceeded')
            ]
        }
        
        handler, mock_omics = self._import_handler_with_mocked_omics(omics_behaviors)
        handler(self.base_event, self.context)
        
        # Verify partial success
        self.assertEqual(mock_omics.create_workflow.call_count, 2)
        
        # Verify CloudFormation response shows partial success
        cfn_args = self.mock_cfnresponse.send.call_args[0]
        self.assertEqual(cfn_args[2], self.mock_cfnresponse.SUCCESS)
        self.assertEqual(cfn_args[4], 'workflow-mag-123')  # Only one workflow ID
        
        response_data = cfn_args[3]
        self.assertEqual(response_data['WorkflowCount'], '1')
    
    def test_invalid_workflow_spec(self):
        """Test handling of invalid workflow specifications"""
        event = self.base_event.copy()
        event['ResourceProperties']['WorkflowConfig'] = ['invalid-spec', 'mag:4.0.0', 'meta:']
        
        omics_behaviors = {
            'list_workflows': {'items': []},
            'create_workflow': {'id': 'workflow-mag-123', 'arn': 'arn:aws:omics:us-east-1:123:workflow/mag-123'}
        }
        
        handler, mock_omics = self._import_handler_with_mocked_omics(omics_behaviors)
        handler(event, self.context)
        
        # Verify only valid workflow was created (mag only, not meta:)
        self.assertEqual(mock_omics.create_workflow.call_count, 1)
        
        # Check that only mag was created
        mag_call = mock_omics.create_workflow.call_args[1]
        self.assertEqual(mag_call['name'], 'nfcore-mag-4-0-0')
    
    def test_update_request_recreates_workflows(self):
        """Test that update requests are treated as create"""
        update_event = self.base_event.copy()
        update_event['RequestType'] = 'Update'
        update_event['PhysicalResourceId'] = 'old-workflow-1,old-workflow-2'
        
        omics_behaviors = {
            'list_workflows': {'items': []},
            'create_workflow': [
                {'id': 'workflow-mag-123', 'arn': 'arn:aws:omics:us-east-1:123:workflow/mag-123'},
                {'id': 'workflow-meta-456', 'arn': 'arn:aws:omics:us-east-1:123:workflow/meta-456'}
            ]
        }
        
        handler, mock_omics = self._import_handler_with_mocked_omics(omics_behaviors)
        handler(update_event, self.context)
        
        # Verify workflows were created (update is treated as create)
        self.assertEqual(mock_omics.create_workflow.call_count, 2)
        
        # Verify new physical resource ID
        cfn_args = self.mock_cfnresponse.send.call_args[0]
        self.assertEqual(cfn_args[4], 'workflow-mag-123,workflow-meta-456')
    
    def test_missing_s3_bundle(self):
        """Test error handling when S3 bundle is missing"""
        # Remove one bundle from S3
        self.s3.delete_object(Bucket=self.code_bucket, Key='nf-core-metatdenovo_1.2.0.zip')
        
        omics_behaviors = {
            'list_workflows': {'items': []},
            'create_workflow': Exception('S3 object not found')
        }
        
        handler, mock_omics = self._import_handler_with_mocked_omics(omics_behaviors)
        handler(self.base_event, self.context)
        
        # Verify CloudFormation response still succeeds with 0 workflows
        cfn_args = self.mock_cfnresponse.send.call_args[0]
        self.assertEqual(cfn_args[2], self.mock_cfnresponse.SUCCESS)
        
        response_data = cfn_args[3]
        self.assertEqual(response_data['WorkflowCount'], '0')
    
    def test_delete_with_invalid_physical_id(self):
        """Test deletion with malformed physical resource ID"""
        delete_event = self.base_event.copy()
        delete_event['RequestType'] = 'Delete'
        delete_event['PhysicalResourceId'] = 'invalid-format'
        
        omics_behaviors = {}
        
        handler, mock_omics = self._import_handler_with_mocked_omics(omics_behaviors)
        handler(delete_event, self.context)
        
        # Should handle gracefully
        self.mock_cfnresponse.send.assert_called_once()
        cfn_args = self.mock_cfnresponse.send.call_args[0]
        self.assertEqual(cfn_args[2], self.mock_cfnresponse.SUCCESS)

    def test_cloudformation_update_workflow_changes(self):
        """Test CloudFormation update flow when workflows are added/removed"""
        # Scenario: Initial config has mag:4.0.0 and metatdenovo:1.2.0
        # Update removes mag and adds ampliseq:2.1.0
        
        # Step 1: Initial Create request
        create_event = self.base_event.copy()
        create_event['ResourceProperties']['WorkflowConfig'] = ['mag:4.0.0', 'metatdenovo:1.2.0']
        
        omics_behaviors = {
            'list_workflows': {'items': []},  # No existing workflows
            'create_workflow': [
                {'id': 'workflow-mag-123', 'arn': 'arn:aws:omics:us-east-1:123:workflow/mag-123'},
                {'id': 'workflow-meta-456', 'arn': 'arn:aws:omics:us-east-1:123:workflow/meta-456'}
            ]
        }
        
        handler, mock_omics = self._import_handler_with_mocked_omics(omics_behaviors)
        handler(create_event, self.context)
        
        # Verify initial creation
        self.assertEqual(mock_omics.create_workflow.call_count, 2)
        cfn_args = self.mock_cfnresponse.send.call_args[0]
        initial_physical_id = cfn_args[4]
        self.assertEqual(initial_physical_id, 'workflow-mag-123,workflow-meta-456')
        
        # Step 2: Update request - remove mag, keep metatdenovo, add ampliseq
        update_event = self.base_event.copy()
        update_event['RequestType'] = 'Update'
        update_event['PhysicalResourceId'] = initial_physical_id
        update_event['ResourceProperties']['WorkflowConfig'] = ['metatdenovo:1.2.0', 'ampliseq:2.1.0']
        
        # Reset mocks for update
        self.mock_cfnresponse.send.reset_mock()
        
        # Configure new behaviors for update
        handler, mock_omics = self._import_handler_with_mocked_omics({})
        
        # Set up side_effect for list_workflows
        mock_omics.list_workflows.side_effect = [
            {'items': [{'name': 'nfcore-metatdenovo-1-2-0', 'id': 'workflow-meta-456'}]},  # metatdenovo exists
            {'items': []}  # ampliseq doesn't exist
        ]
        
        # Set up return value for create_workflow
        mock_omics.create_workflow.return_value = {
            'id': 'workflow-amp-789', 
            'arn': 'arn:aws:omics:us-east-1:123:workflow/amp-789'
        }
        
        handler(update_event, self.context)
        
        # Verify only ampliseq was created
        self.assertEqual(mock_omics.create_workflow.call_count, 1)
        create_args = mock_omics.create_workflow.call_args[1]
        self.assertEqual(create_args['name'], 'nfcore-ampliseq-2-1-0')
        
        # Verify new physical resource ID contains metatdenovo and ampliseq (not mag)
        cfn_args = self.mock_cfnresponse.send.call_args[0]
        new_physical_id = cfn_args[4]
        self.assertIn('workflow-meta-456', new_physical_id)
        self.assertIn('workflow-amp-789', new_physical_id)
        self.assertNotIn('workflow-mag-123', new_physical_id)
        
        # Step 3: CloudFormation will now call Delete on the old resource
        delete_event = self.base_event.copy()
        delete_event['RequestType'] = 'Delete'
        delete_event['PhysicalResourceId'] = initial_physical_id  # Old ID with mag and metatdenovo
        
        # Reset mocks for delete
        self.mock_cfnresponse.send.reset_mock()
        
        handler, mock_omics = self._import_handler_with_mocked_omics({})
        handler(delete_event, self.context)
        
        # Verify both old workflows were attempted to be deleted
        self.assertEqual(mock_omics.delete_workflow.call_count, 2)
        delete_calls = [call[1]['id'] for call in mock_omics.delete_workflow.call_args_list]
        self.assertIn('workflow-mag-123', delete_calls)
        self.assertIn('workflow-meta-456', delete_calls)
        
        # Note: In practice, deleting workflow-meta-456 might fail since it's still in use,
        # but the Lambda handles this gracefully with a warning


if __name__ == '__main__':
    unittest.main() 