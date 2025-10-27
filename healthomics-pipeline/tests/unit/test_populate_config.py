import unittest
from unittest.mock import Mock, patch, MagicMock, call
import sys
import os

# Add lambda source to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../lambda_src'))


class TestPopulateConfig(unittest.TestCase):
    """Unit tests for populate-workflow-config Lambda"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.context = Mock()
        self.context.log_stream_name = "test-log-stream"
        
        self.test_event = {
            'RequestType': 'Create',
            'ResponseURL': 'https://test-url.com',
            'StackId': 'test-stack',
            'RequestId': 'test-request',
            'LogicalResourceId': 'PopulateConfig',
            'ResourceProperties': {
                'OmicsRoleArn': 'arn:aws:iam::123456789012:role/OmicsServiceRole',
                'RunGroupId': '123456789',
                'TableName': 'omics-workflow-config'
            }
        }
        
        # Mock environment variables
        self.env_patcher = patch.dict(os.environ, {
            'WORKFLOW_CONFIG': 'mag:4.0.0,metatdenovo:1.2.0'
        })
        
        # Create fresh mocks for each test
        self.mock_dynamodb = MagicMock()
        self.mock_table = MagicMock()
        self.mock_omics_client = MagicMock()
        
    def _import_handler_with_mocks(self, use_default_env=True):
        """Import the handler with mocked boto3 clients"""
        # Create mock cfnresponse module
        mock_cfnresponse_module = MagicMock()
        mock_cfnresponse_module.send = MagicMock()
        mock_cfnresponse_module.SUCCESS = "SUCCESS"
        mock_cfnresponse_module.FAILED = "FAILED"
        
        # Mock time module to avoid sleep
        mock_time = MagicMock()
        mock_time.sleep = MagicMock()
        
        with patch.dict(sys.modules, {'cfnresponse': mock_cfnresponse_module, 'time': mock_time}), \
             patch('boto3.resource') as mock_resource, \
             patch('boto3.client') as mock_client:
            
            # Set up DynamoDB mock
            mock_resource.return_value = self.mock_dynamodb
            self.mock_dynamodb.Table.return_value = self.mock_table
            
            # Set up Omics client mock
            mock_client.return_value = self.mock_omics_client
            
            # Start environment patch only if requested
            if use_default_env:
                self.env_patcher.start()
            
            # Import the module with mocked dependencies
            if 'populate_config' in sys.modules:
                del sys.modules['populate_config']
            
            import importlib.util
            spec = importlib.util.spec_from_file_location(
                "populate_config", 
                os.path.join(os.path.dirname(__file__), '../../lambda_src/populate-workflow-config.py')
            )
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            
            return module.handler, mock_cfnresponse_module
    
    def tearDown(self):
        """Clean up after tests"""
        self.env_patcher.stop()
        # Remove the imported module from sys.modules
        if 'populate_config' in sys.modules:
            del sys.modules['populate_config']
    
    def test_create_config_success(self):
        """Test successful creation of workflow configuration"""
        # Set up Omics response
        omics_response = {
            'items': [
                {'arn': 'arn:aws:omics:us-east-1:123:workflow/mag-123', 'name': 'nfcore-mag-4-0-0'},
                {'arn': 'arn:aws:omics:us-east-1:123:workflow/meta-456', 'name': 'nfcore-metatdenovo-1-2-0'}
            ]
        }
        
        # Import handler with mocks
        handler, mock_cfnresponse = self._import_handler_with_mocks()
        
        # Mock list_workflows to return workflows immediately (no retry needed)
        self.mock_omics_client.list_workflows.return_value = omics_response
        
        # Execute handler
        handler(self.test_event, self.context)
        
        # Verify DynamoDB put_item was called with correct data
        expected_item = {
            'id': 'workflows',
            'workflows': [
                {'name': 'mag', 'version': '4.0.0', 'arn': 'arn:aws:omics:us-east-1:123:workflow/mag-123'},
                {'name': 'metatdenovo', 'version': '1.2.0', 'arn': 'arn:aws:omics:us-east-1:123:workflow/meta-456'}
            ],
            'omics_role': 'arn:aws:iam::123456789012:role/OmicsServiceRole',
            'run_group': '123456789'
        }
        self.mock_table.put_item.assert_called_once_with(Item=expected_item)
        
        # Verify CloudFormation response
        mock_cfnresponse.send.assert_called_once_with(
            self.test_event,
            self.context,
            mock_cfnresponse.SUCCESS,
            {'WorkflowCount': '2'}
        )
        
        # Verify list_workflows was called (once, since workflows were found immediately)
        self.mock_omics_client.list_workflows.assert_called()
    
    def test_create_config_with_retry(self):
        """Test creation with retry when workflows aren't immediately available"""
        # Set up Omics responses - empty first, then partial, then complete
        omics_responses = [
            {'items': []},  # First attempt - no workflows
            {'items': [{'arn': 'arn:aws:omics:us-east-1:123:workflow/mag-123', 'name': 'nfcore-mag-4-0-0'}]},  # Second - partial
            {'items': [  # Third - all workflows found
                {'arn': 'arn:aws:omics:us-east-1:123:workflow/mag-123', 'name': 'nfcore-mag-4-0-0'},
                {'arn': 'arn:aws:omics:us-east-1:123:workflow/meta-456', 'name': 'nfcore-metatdenovo-1-2-0'}
            ]},
            {'items': [  # Fourth call for building config after retry  
                {'arn': 'arn:aws:omics:us-east-1:123:workflow/mag-123', 'name': 'nfcore-mag-4-0-0'},
                {'arn': 'arn:aws:omics:us-east-1:123:workflow/meta-456', 'name': 'nfcore-metatdenovo-1-2-0'}
            ]}
        ]
        
        # Import handler with mocks
        handler, mock_cfnresponse = self._import_handler_with_mocks()
        
        # Mock list_workflows to return different responses
        self.mock_omics_client.list_workflows.side_effect = omics_responses
        
        # Execute handler
        handler(self.test_event, self.context)
        
        # Verify DynamoDB put_item was called
        self.mock_table.put_item.assert_called_once()
        
        # Verify CloudFormation response
        mock_cfnresponse.send.assert_called_once_with(
            self.test_event,
            self.context,
            mock_cfnresponse.SUCCESS,
            {'WorkflowCount': '2'}
        )
        
        # Verify list_workflows was called 3 times during retry
        self.assertEqual(self.mock_omics_client.list_workflows.call_count, 3)
        
        # Verify time.sleep was called twice (after first two failed attempts)
        # Get the mocked time module from sys.modules
        mock_time = sys.modules.get('time')
        if mock_time and hasattr(mock_time, 'sleep') and hasattr(mock_time.sleep, 'call_count'):
            self.assertEqual(mock_time.sleep.call_count, 2)
    
    def test_retry_with_multiple_workflows(self):
        """Test retry mechanism with more than 2 workflows"""
        # Stop the default env patcher
        self.env_patcher.stop()
        
        # Set up environment with 3 workflows
        with patch.dict(os.environ, {'WORKFLOW_CONFIG': 'mag:4.0.0,metatdenovo:1.2.0,ampliseq:2.1.0'}):
            # Set up Omics responses - gradual availability
            omics_responses = [
                {'items': []},  # First attempt - no workflows
                {'items': [  # Second - only mag
                    {'arn': 'arn:aws:omics:us-east-1:123:workflow/mag-123', 'name': 'nfcore-mag-4-0-0'}
                ]},
                {'items': [  # Third - mag and metatdenovo
                    {'arn': 'arn:aws:omics:us-east-1:123:workflow/mag-123', 'name': 'nfcore-mag-4-0-0'},
                    {'arn': 'arn:aws:omics:us-east-1:123:workflow/meta-456', 'name': 'nfcore-metatdenovo-1-2-0'}
                ]},
                {'items': [  # Fourth - all workflows found
                    {'arn': 'arn:aws:omics:us-east-1:123:workflow/mag-123', 'name': 'nfcore-mag-4-0-0'},
                    {'arn': 'arn:aws:omics:us-east-1:123:workflow/meta-456', 'name': 'nfcore-metatdenovo-1-2-0'},
                    {'arn': 'arn:aws:omics:us-east-1:123:workflow/amp-789', 'name': 'nfcore-ampliseq-2-1-0'}
                ]}
            ]
            
            # Import handler with mocks
            handler, mock_cfnresponse = self._import_handler_with_mocks(use_default_env=False)
            
            # Mock list_workflows to return different responses
            self.mock_omics_client.list_workflows.side_effect = omics_responses
            
            # Execute handler
            handler(self.test_event, self.context)
            
            # Verify DynamoDB put_item was called with all 3 workflows
            self.mock_table.put_item.assert_called_once()
            put_item_args = self.mock_table.put_item.call_args[1]['Item']
            self.assertEqual(len(put_item_args['workflows']), 3)
            
            # Verify CloudFormation response
            mock_cfnresponse.send.assert_called_once_with(
                self.test_event,
                self.context,
                mock_cfnresponse.SUCCESS,
                {'WorkflowCount': '3'}
            )
            
            # Verify list_workflows was called 4 times
            self.assertEqual(self.mock_omics_client.list_workflows.call_count, 4)
        
        # Restart the env patcher for other tests
        self.env_patcher.start()
    
    def test_delete_request(self):
        """Test handling of delete requests"""
        # Set up delete event
        delete_event = self.test_event.copy()
        delete_event['RequestType'] = 'Delete'
        
        # Import and call handler
        handler, mock_cfnresponse = self._import_handler_with_mocks()
        handler(delete_event, self.context)
        
        # Verify no DynamoDB operations
        self.mock_table.put_item.assert_not_called()
        self.mock_omics_client.list_workflows.assert_not_called()
        
        # Verify success response
        mock_cfnresponse.send.assert_called_once()
        call_args = mock_cfnresponse.send.call_args[0]
        self.assertEqual(call_args[2], mock_cfnresponse.SUCCESS)
    
    def test_workflow_not_found(self):
        """Test when workflows cannot be found"""
        # For Update requests, we don't retry
        update_event = self.test_event.copy()
        update_event['RequestType'] = 'Update'
        
        # Set up empty response
        self.mock_omics_client.list_workflows.return_value = {
            'items': []
        }
        
        handler, mock_cfnresponse = self._import_handler_with_mocks()
        handler(update_event, self.context)
        
        # Verify empty workflow list was stored
        self.mock_table.put_item.assert_called_once()
        put_item_args = self.mock_table.put_item.call_args[1]['Item']
        self.assertEqual(len(put_item_args['workflows']), 0)
    
    def test_single_workflow_configuration(self):
        """Test handling of single workflow configuration"""
        # Stop the default env patcher
        self.env_patcher.stop()
        
        # Set up environment with single workflow
        with patch.dict(os.environ, {'WORKFLOW_CONFIG': 'mag:4.0.0'}):
            # Set up Omics response with single workflow
            omics_response = {
                'items': [
                    {'arn': 'arn:aws:omics:us-east-1:123:workflow/mag-123', 'name': 'nfcore-mag-4-0-0'}
                ]
            }
            
            # Import handler with mocks
            handler, mock_cfnresponse = self._import_handler_with_mocks(use_default_env=False)
            
            # Mock list_workflows to return single workflow
            self.mock_omics_client.list_workflows.return_value = omics_response
            
            # Execute handler
            handler(self.test_event, self.context)
            
            # Verify single workflow was configured
            expected_item = {
                'id': 'workflows',
                'workflows': [
                    {'name': 'mag', 'version': '4.0.0', 'arn': 'arn:aws:omics:us-east-1:123:workflow/mag-123'}
                ],
                'omics_role': 'arn:aws:iam::123456789012:role/OmicsServiceRole',
                'run_group': '123456789'
            }
            self.mock_table.put_item.assert_called_once_with(Item=expected_item)
            
            # Verify CloudFormation response
            mock_cfnresponse.send.assert_called_once_with(
                self.test_event,
                self.context,
                mock_cfnresponse.SUCCESS,
                {'WorkflowCount': '1'}
            )
        
        # Restart the env patcher for other tests
        self.env_patcher.start()
    
    def test_multiple_workflows_more_than_two(self):
        """Test handling of more than 2 workflows"""
        # Stop the default env patcher
        self.env_patcher.stop()
        
        # Set up environment with 5 workflows
        with patch.dict(os.environ, {'WORKFLOW_CONFIG': 'mag:4.0.0,metatdenovo:1.2.0,ampliseq:2.1.0,atacseq:2.0.0,rnaseq:3.14.0'}):
            # Set up Omics response with all 5 workflows
            omics_response = {
                'items': [
                    {'arn': 'arn:aws:omics:us-east-1:123:workflow/mag-123', 'name': 'nfcore-mag-4-0-0'},
                    {'arn': 'arn:aws:omics:us-east-1:123:workflow/meta-456', 'name': 'nfcore-metatdenovo-1-2-0'},
                    {'arn': 'arn:aws:omics:us-east-1:123:workflow/amp-789', 'name': 'nfcore-ampliseq-2-1-0'},
                    {'arn': 'arn:aws:omics:us-east-1:123:workflow/atac-101', 'name': 'nfcore-atacseq-2-0-0'},
                    {'arn': 'arn:aws:omics:us-east-1:123:workflow/rna-202', 'name': 'nfcore-rnaseq-3-14-0'}
                ]
            }
            
            # Import handler with mocks
            handler, mock_cfnresponse = self._import_handler_with_mocks(use_default_env=False)
            
            # Mock list_workflows to return all 5 workflows
            self.mock_omics_client.list_workflows.return_value = omics_response
            
            # Execute handler
            handler(self.test_event, self.context)
            
            # Verify all 5 workflows were configured
            expected_item = {
                'id': 'workflows',
                'workflows': [
                    {'name': 'mag', 'version': '4.0.0', 'arn': 'arn:aws:omics:us-east-1:123:workflow/mag-123'},
                    {'name': 'metatdenovo', 'version': '1.2.0', 'arn': 'arn:aws:omics:us-east-1:123:workflow/meta-456'},
                    {'name': 'ampliseq', 'version': '2.1.0', 'arn': 'arn:aws:omics:us-east-1:123:workflow/amp-789'},
                    {'name': 'atacseq', 'version': '2.0.0', 'arn': 'arn:aws:omics:us-east-1:123:workflow/atac-101'},
                    {'name': 'rnaseq', 'version': '3.14.0', 'arn': 'arn:aws:omics:us-east-1:123:workflow/rna-202'}
                ],
                'omics_role': 'arn:aws:iam::123456789012:role/OmicsServiceRole',
                'run_group': '123456789'
            }
            self.mock_table.put_item.assert_called_once_with(Item=expected_item)
            
            # Verify CloudFormation response
            mock_cfnresponse.send.assert_called_once_with(
                self.test_event,
                self.context,
                mock_cfnresponse.SUCCESS,
                {'WorkflowCount': '5'}
            )
        
        # Restart the env patcher for other tests
        self.env_patcher.start()
    
    def test_partial_workflow_match(self):
        """Test configuration when only some workflows exist"""
        # For Update requests, we don't retry
        update_event = self.test_event.copy()
        update_event['RequestType'] = 'Update'
        
        # Set up response with only one workflow
        self.mock_omics_client.list_workflows.return_value = {
            'items': [
                {'name': 'nfcore-mag-4-0-0', 'arn': 'arn:aws:omics:us-east-1:123:workflow/mag'}
            ]
        }
        
        handler, mock_cfnresponse = self._import_handler_with_mocks()
        handler(update_event, self.context)
        
        # Verify only one workflow was configured
        self.mock_table.put_item.assert_called_once()
        put_item_args = self.mock_table.put_item.call_args[1]['Item']
        self.assertEqual(len(put_item_args['workflows']), 1)
        self.assertEqual(put_item_args['workflows'][0]['name'], 'mag')
        self.assertEqual(put_item_args['workflows'][0]['arn'], 'arn:aws:omics:us-east-1:123:workflow/mag')
    
    def test_error_handling(self):
        """Test error handling when operations fail"""
        # Set up mock to raise exception
        self.mock_omics_client.list_workflows.side_effect = Exception('API error')
        
        # Import and call handler
        handler, mock_cfnresponse = self._import_handler_with_mocks()
        handler(self.test_event, self.context)
        
        # Verify failure response
        mock_cfnresponse.send.assert_called_once()
        call_args = mock_cfnresponse.send.call_args[0]
        self.assertEqual(call_args[2], mock_cfnresponse.FAILED)
    
    def test_update_request(self):
        """Test handling of update requests"""
        # Set up update event
        update_event = self.test_event.copy()
        update_event['RequestType'] = 'Update'
        
        # Set up mock responses
        self.mock_omics_client.list_workflows.return_value = {
            'items': [
                {'name': 'nfcore-mag-4-0-0', 'arn': 'arn:aws:omics:us-east-1:123:workflow/mag'}
            ]
        }
        
        # Import and call handler
        handler, mock_cfnresponse = self._import_handler_with_mocks()
        handler(update_event, self.context)
        
        # Verify no sleep for update (only for create)
        # Verify put_item was called
        self.mock_table.put_item.assert_called_once()
        
        # Verify success response
        call_args = mock_cfnresponse.send.call_args[0]
        self.assertEqual(call_args[2], mock_cfnresponse.SUCCESS)


if __name__ == '__main__':
    unittest.main() 