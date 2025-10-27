import unittest
from unittest.mock import Mock, patch, MagicMock
import json
import sys
import os
import importlib.util

# Add the lambda source directory to the path
lambda_src_path = os.path.join(os.path.dirname(__file__), '../../lambda_src')
sys.path.insert(0, lambda_src_path)


class TestCreateWorkflowsLambda(unittest.TestCase):
    
    def setUp(self):
        """Set up test fixtures"""
        self.context = Mock()
        self.context.log_stream_name = "test-log-stream"
        
        self.base_event = {
            "RequestType": "Create",
            "ResponseURL": "https://cloudformation-custom-resource-response-useast1.s3.amazonaws.com/test",
            "StackId": "arn:aws:cloudformation:us-east-1:123456789012:stack/test-stack/test-id",
            "RequestId": "test-request-id",
            "ResourceType": "Custom::CreateOmicsWorkflows",
            "LogicalResourceId": "CreateWorkflows",
            "ResourceProperties": {
                "ServiceToken": "arn:aws:lambda:us-east-1:123456789012:function:create-omics-workflows",
                "WorkflowConfig": ["mag:4.0.0", "metatdenovo:1.2.0"],
                "CodeBucketName": "nfcore-healthomics-bundles-123456789012",
                "DefaultStorageCapacity": "100"
            }
        }
        
        # Create a fresh mock for each test
        self.mock_omics_client = MagicMock()
        
    def _import_handler_with_mock(self):
        """Import the handler with mocked boto3 client"""
        with patch('boto3.client') as mock_boto_client:
            mock_boto_client.return_value = self.mock_omics_client
            
            # Import the module
            spec = importlib.util.spec_from_file_location(
                "create_omics_workflows", 
                os.path.join(lambda_src_path, "create-omics-workflows.py")
            )
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            
            # Patch cfnresponse in the module
            module.cfnresponse = MagicMock()
            module.cfnresponse.SUCCESS = "SUCCESS"
            module.cfnresponse.FAILED = "FAILED"
            
            return module.handler, module.cfnresponse
    
    def test_create_workflows_success(self):
        """Test successful workflow creation"""
        # Set up mock behaviors
        self.mock_omics_client.list_workflows.return_value = {'items': []}
        self.mock_omics_client.create_workflow.side_effect = [
            {'id': 'workflow-id-1'},
            {'id': 'workflow-id-2'}
        ]
        
        # Import handler with mocks
        handler, mock_cfnresponse = self._import_handler_with_mock()
        
        # Call the handler
        handler(self.base_event, self.context)
        
        # Verify create_workflow was called twice
        self.assertEqual(self.mock_omics_client.create_workflow.call_count, 2)
        
        # Verify the correct parameters were passed
        calls = self.mock_omics_client.create_workflow.call_args_list
        self.assertEqual(calls[0][1]['name'], 'nfcore-mag-4-0-0')
        self.assertEqual(calls[1][1]['name'], 'nfcore-metatdenovo-1-2-0')
        
        # Verify cfnresponse.send was called with SUCCESS
        mock_cfnresponse.send.assert_called_once()
        call_args = mock_cfnresponse.send.call_args
        self.assertEqual(call_args[0][2], mock_cfnresponse.SUCCESS)
        
        # Verify response data
        response_data = call_args[0][3]
        self.assertEqual(response_data['WorkflowCount'], '2')
        workflows = json.loads(response_data['Workflows'])
        self.assertEqual(len(workflows), 2)
    
    def test_create_workflows_already_exists(self):
        """Test when workflows already exist"""
        # Set up mock behaviors
        self.mock_omics_client.list_workflows.side_effect = [
            {'items': [{'id': 'existing-workflow-1'}]},
            {'items': [{'id': 'existing-workflow-2'}]}
        ]
        
        # Import handler with mocks
        handler, mock_cfnresponse = self._import_handler_with_mock()
        
        # Call the handler
        handler(self.base_event, self.context)
        
        # Verify create_workflow was NOT called
        self.mock_omics_client.create_workflow.assert_not_called()
        
        # Verify cfnresponse.send was called with SUCCESS
        mock_cfnresponse.send.assert_called_once()
        call_args = mock_cfnresponse.send.call_args
        self.assertEqual(call_args[0][2], mock_cfnresponse.SUCCESS)
        
        # Verify response data still includes the workflows
        response_data = call_args[0][3]
        self.assertEqual(response_data['WorkflowCount'], '2')
    
    def test_delete_workflows(self):
        """Test workflow deletion"""
        # Set up delete event
        delete_event = self.base_event.copy()
        delete_event['RequestType'] = 'Delete'
        delete_event['PhysicalResourceId'] = 'workflow-id-1,workflow-id-2'
        
        # Import handler with mocks
        handler, mock_cfnresponse = self._import_handler_with_mock()
        
        # Call the handler
        handler(delete_event, self.context)
        
        # Verify delete_workflow was called twice
        self.assertEqual(self.mock_omics_client.delete_workflow.call_count, 2)
        self.mock_omics_client.delete_workflow.assert_any_call(id='workflow-id-1')
        self.mock_omics_client.delete_workflow.assert_any_call(id='workflow-id-2')
        
        # Verify cfnresponse.send was called with SUCCESS
        mock_cfnresponse.send.assert_called_once()
        call_args = mock_cfnresponse.send.call_args
        self.assertEqual(call_args[0][2], mock_cfnresponse.SUCCESS)
    
    def test_update_workflow_configuration(self):
        """Test updating workflow configuration (add and remove workflows)"""
        # Initial state: mag:4.0.0 exists with ID workflow-mag-123
        # Update adds metatdenovo:1.2.0 and removes mag:4.0.0
        update_event = self.base_event.copy()
        update_event['RequestType'] = 'Update'
        update_event['PhysicalResourceId'] = 'workflow-mag-123'  # Old workflow ID
        update_event['OldResourceProperties'] = {
            'WorkflowConfig': ['mag:4.0.0'],
            'CodeBucketName': 'test-bucket',
            'DefaultStorageCapacity': '100'
        }
        update_event['ResourceProperties']['WorkflowConfig'] = ['metatdenovo:1.2.0']
        
        # Mock list_workflows to show mag already exists
        self.mock_omics_client.list_workflows.side_effect = [
            {'items': []},  # metatdenovo doesn't exist
        ]
        
        # Mock create_workflow for new workflow
        self.mock_omics_client.create_workflow.return_value = {
            'id': 'workflow-meta-456',
            'arn': 'arn:aws:omics:us-east-1:123:workflow/meta-456'
        }
        
        handler, mock_cfnresponse = self._import_handler_with_mock()
        handler(update_event, self.context)
        
        # Verify new workflow was created
        self.mock_omics_client.create_workflow.assert_called_once()
        create_args = self.mock_omics_client.create_workflow.call_args[1]
        self.assertEqual(create_args['name'], 'nfcore-metatdenovo-1-2-0')
        
        # Verify response includes new physical resource ID
        mock_cfnresponse.send.assert_called_once()
        call_args = mock_cfnresponse.send.call_args[0]
        self.assertEqual(call_args[2], mock_cfnresponse.SUCCESS)
        # New physical resource ID should only contain the new workflow
        self.assertEqual(call_args[4], 'workflow-meta-456')
    
    def test_update_add_workflow(self):
        """Test adding a workflow while keeping existing ones"""
        # Initial state: mag:4.0.0 exists
        # Update keeps mag:4.0.0 and adds metatdenovo:1.2.0
        update_event = self.base_event.copy()
        update_event['RequestType'] = 'Update'
        update_event['PhysicalResourceId'] = 'workflow-mag-123'
        update_event['ResourceProperties']['WorkflowConfig'] = ['mag:4.0.0', 'metatdenovo:1.2.0']
        
        # Mock list_workflows responses
        self.mock_omics_client.list_workflows.side_effect = [
            {'items': [{'name': 'nfcore-mag-4-0-0', 'id': 'workflow-mag-123'}]},  # mag exists
            {'items': []},  # metatdenovo doesn't exist
        ]
        
        # Mock create_workflow for new workflow
        self.mock_omics_client.create_workflow.return_value = {
            'id': 'workflow-meta-456',
            'arn': 'arn:aws:omics:us-east-1:123:workflow/meta-456'
        }
        
        handler, mock_cfnresponse = self._import_handler_with_mock()
        handler(update_event, self.context)
        
        # Verify only new workflow was created
        self.mock_omics_client.create_workflow.assert_called_once()
        create_args = self.mock_omics_client.create_workflow.call_args[1]
        self.assertEqual(create_args['name'], 'nfcore-metatdenovo-1-2-0')
        
        # Verify response includes both workflow IDs
        mock_cfnresponse.send.assert_called_once()
        call_args = mock_cfnresponse.send.call_args[0]
        self.assertEqual(call_args[2], mock_cfnresponse.SUCCESS)
        # Physical resource ID should contain both workflows
        physical_id = call_args[4]
        self.assertIn('workflow-mag-123', physical_id)
        self.assertIn('workflow-meta-456', physical_id)
    
    def test_invalid_workflow_spec(self):
        """Test handling of invalid workflow specification"""
        # Set up event with invalid workflow spec
        event = self.base_event.copy()
        event['ResourceProperties']['WorkflowConfig'] = ["invalid-spec", "mag:4.0.0"]
        
        # Set up mock behaviors
        self.mock_omics_client.list_workflows.return_value = {'items': []}
        self.mock_omics_client.create_workflow.return_value = {'id': 'workflow-id-1'}
        
        # Import handler with mocks
        handler, mock_cfnresponse = self._import_handler_with_mock()
        
        # Call the handler
        handler(event, self.context)
        
        # Verify only one workflow was created (the valid one)
        self.assertEqual(self.mock_omics_client.create_workflow.call_count, 1)
        self.assertEqual(
            self.mock_omics_client.create_workflow.call_args[1]['name'], 
            'nfcore-mag-4-0-0'
        )
        
        # Verify cfnresponse.send was called with SUCCESS
        mock_cfnresponse.send.assert_called_once()
        call_args = mock_cfnresponse.send.call_args
        self.assertEqual(call_args[0][2], mock_cfnresponse.SUCCESS)
        
        # Verify response data shows only 1 workflow
        response_data = call_args[0][3]
        self.assertEqual(response_data['WorkflowCount'], '1')
    
    def test_create_workflow_error_handling(self):
        """Test error handling when workflow creation fails"""
        # Set up mock to raise an exception
        self.mock_omics_client.list_workflows.return_value = {'items': []}
        self.mock_omics_client.create_workflow.side_effect = Exception("API Error")
        
        # Import handler with mocks
        handler, mock_cfnresponse = self._import_handler_with_mock()
        
        # Call the handler
        handler(self.base_event, self.context)
        
        # Verify cfnresponse.send was still called with SUCCESS
        # (because the Lambda continues on error)
        mock_cfnresponse.send.assert_called_once()
        call_args = mock_cfnresponse.send.call_args
        self.assertEqual(call_args[0][2], mock_cfnresponse.SUCCESS)
        
        # Verify response data shows 0 workflows
        response_data = call_args[0][3]
        self.assertEqual(response_data['WorkflowCount'], '0')


if __name__ == '__main__':
    unittest.main() 