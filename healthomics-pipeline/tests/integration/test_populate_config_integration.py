import unittest
import boto3
import os
import sys
import json
from moto import mock_aws
from unittest.mock import patch, MagicMock

# Add lambda source to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../lambda_src'))


@mock_aws
class TestPopulateConfigIntegration(unittest.TestCase):
    """Integration tests for populate-workflow-config Lambda"""
    
    def setUp(self):
        """Set up test fixtures before each test"""
        # Create DynamoDB table
        self.dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        self.table_name = 'omics-workflow-config'
        self.table = self.dynamodb.create_table(
            TableName=self.table_name,
            KeySchema=[
                {
                    'AttributeName': 'id',
                    'KeyType': 'HASH'
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'id',
                    'AttributeType': 'S'
                }
            ],
            BillingMode='PAY_PER_REQUEST'
        )
        
        # Mock environment variables
        self.env_patcher = patch.dict(os.environ, {
            'WORKFLOW_CONFIG': 'mag:4.0.0,metatdenovo:1.2.0'
        })
        self.env_patcher.start()
        
        # CloudFormation event template
        self.base_event = {
            'RequestType': 'Create',
            'ResponseURL': 'https://cloudformation-custom-resource-response-useast1.s3.amazonaws.com/test',
            'StackId': 'arn:aws:cloudformation:us-east-1:123456789012:stack/test-stack/test-id',
            'RequestId': 'test-request-id',
            'ResourceType': 'Custom::PopulateConfig',
            'LogicalResourceId': 'PopulateConfig',
            'ResourceProperties': {
                'OmicsRoleArn': 'arn:aws:iam::123456789012:role/OmicsServiceRole',
                'RunGroupId': 'run-group-123'
            }
        }
        
        # Create mock for cfnresponse
        self.mock_cfnresponse = MagicMock()
        self.mock_cfnresponse.SUCCESS = "SUCCESS"
        self.mock_cfnresponse.FAILED = "FAILED"
        
        # Create lambda context
        self.context = MagicMock()
        self.context.log_stream_name = "test-log-stream"
    
    def tearDown(self):
        """Clean up after tests"""
        self.env_patcher.stop()
    
    def _import_handler_with_mocks(self, mock_omics_responses):
        """Import handler with mocked Omics client"""
        # Mock time to avoid sleep
        mock_time = MagicMock()
        mock_time.sleep = MagicMock()
        
        # Mock Omics client
        mock_omics_client = MagicMock()
        mock_omics_client.list_workflows.return_value = mock_omics_responses
        
        # Use the real moto DynamoDB resource
        mock_dynamodb_resource = self.dynamodb
        
        with patch.dict(sys.modules, {'cfnresponse': self.mock_cfnresponse, 'time': mock_time}), \
             patch('boto3.client') as mock_client, \
             patch('boto3.resource') as mock_resource:
            
            def client_side_effect(service_name, **kwargs):
                if service_name == 'omics':
                    return mock_omics_client
                else:
                    # For any other service, return a mock
                    return MagicMock()
            
            def resource_side_effect(service_name, **kwargs):
                if service_name == 'dynamodb':
                    return mock_dynamodb_resource
                else:
                    # For any other service, return a mock
                    return MagicMock()
            
            mock_client.side_effect = client_side_effect
            mock_resource.side_effect = resource_side_effect
            
            # Clear any previously imported module
            if 'populate_config' in sys.modules:
                del sys.modules['populate_config']
            
            # Import the module
            import importlib.util
            spec = importlib.util.spec_from_file_location(
                "populate_config", 
                os.path.join(os.path.dirname(__file__), '../../lambda_src/populate-workflow-config.py')
            )
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            
            return module.handler, mock_omics_client
    
    def test_create_config_stores_in_dynamodb(self):
        """Test that workflow configuration is stored correctly in DynamoDB"""
        # Set up mock Omics responses to return workflows immediately
        mock_omics_responses = {
            'items': [
                {'name': 'nfcore-mag-4-0-0', 'arn': 'arn:aws:omics:us-east-1:123:workflow/mag'},
                {'name': 'nfcore-metatdenovo-1-2-0', 'arn': 'arn:aws:omics:us-east-1:123:workflow/meta'}
            ]
        }
        
        handler, mock_omics = self._import_handler_with_mocks(mock_omics_responses)
        
        # Update event with TableName
        event = self.base_event.copy()
        event['ResourceProperties']['TableName'] = self.table_name
        
        handler(event, self.context)
        
        # Verify item was stored in DynamoDB
        response = self.table.get_item(Key={'id': 'workflows'})
        self.assertIn('Item', response)
        
        item = response['Item']
        self.assertEqual(len(item['workflows']), 2)
        self.assertEqual(item['workflows'][0]['name'], 'mag')
        self.assertEqual(item['workflows'][0]['version'], '4.0.0')
        self.assertEqual(item['workflows'][1]['name'], 'metatdenovo')
        self.assertEqual(item['workflows'][1]['version'], '1.2.0')
        
        # Verify CloudFormation response
        self.mock_cfnresponse.send.assert_called_once()
        cfn_args = self.mock_cfnresponse.send.call_args[0]
        self.assertEqual(cfn_args[2], self.mock_cfnresponse.SUCCESS)
    
    def test_retry_mechanism_when_workflows_not_ready(self):
        """Test retry mechanism when workflows aren't immediately available"""
        # Mock Omics responses - empty first, then with workflows
        mock_omics_responses = [
            {'items': []},  # First call - no workflows
            {'items': []},  # Second call - still no workflows
            {'items': [  # Third call - workflows found
                {'name': 'nfcore-mag-4-0-0', 'arn': 'arn:aws:omics:us-east-1:123:workflow/mag'},
                {'name': 'nfcore-metatdenovo-1-2-0', 'arn': 'arn:aws:omics:us-east-1:123:workflow/meta'}
            ]}
        ]
        
        # Import handler with mocks and configure the side_effect
        handler, mock_omics = self._import_handler_with_mocks({})
        mock_omics.list_workflows.side_effect = mock_omics_responses
        
        # Update event with TableName
        event = self.base_event.copy()
        event['ResourceProperties']['TableName'] = self.table_name
        
        handler(event, self.context)
        
        # Verify retry happened
        self.assertEqual(mock_omics.list_workflows.call_count, 3)
        
        # Verify data was eventually stored
        response = self.table.get_item(Key={'id': 'workflows'})
        self.assertIn('Item', response)
        self.assertEqual(len(response['Item']['workflows']), 2)
    
    def test_update_config_overwrites_existing(self):
        """Test that update requests overwrite existing configuration"""
        # First, create initial configuration with Update request to avoid retry
        initial_response = {
            'items': [
                {'name': 'nfcore-mag-4-0-0', 'arn': 'arn:aws:omics:us-east-1:123:workflow/mag-old'}
            ]
        }
        
        handler, _ = self._import_handler_with_mocks(initial_response)
        
        # Use Update request
        event = self.base_event.copy()
        event['RequestType'] = 'Update'
        event['ResourceProperties']['TableName'] = self.table_name
        handler(event, self.context)
        
        # Verify initial data
        response = self.table.get_item(Key={'id': 'workflows'})
        self.assertEqual(len(response['Item']['workflows']), 1)
        self.assertEqual(response['Item']['workflows'][0]['arn'], 'arn:aws:omics:us-east-1:123:workflow/mag-old')
        
        # Now update with new workflows
        update_event = self.base_event.copy()
        update_event['RequestType'] = 'Update'
        
        updated_response = {
            'items': [
                {'name': 'nfcore-mag-4-0-0', 'arn': 'arn:aws:omics:us-east-1:123:workflow/mag-new'},
                {'name': 'nfcore-metatdenovo-1-2-0', 'arn': 'arn:aws:omics:us-east-1:123:workflow/meta-new'}
            ]
        }
        
        handler, _ = self._import_handler_with_mocks(updated_response)
        handler(update_event, self.context)
        
        # Verify data was updated
        response = self.table.get_item(Key={'id': 'workflows'})
        self.assertEqual(len(response['Item']['workflows']), 2)
        
        workflows = {w['name']: w for w in response['Item']['workflows']}
        self.assertEqual(workflows['mag']['arn'], 'arn:aws:omics:us-east-1:123:workflow/mag-new')
        self.assertEqual(workflows['metatdenovo']['arn'], 'arn:aws:omics:us-east-1:123:workflow/meta-new')
    
    def test_delete_request_does_not_modify_table(self):
        """Test that delete requests don't modify the DynamoDB table"""
        # Create initial configuration
        initial_response = {
            'items': [
                {'name': 'nfcore-mag-4-0-0', 'arn': 'arn:aws:omics:us-east-1:123:workflow/mag-123'}
            ]
        }
        
        handler, _ = self._import_handler_with_mocks(initial_response)
        
        # Add TableName to event
        event = self.base_event.copy()
        event['ResourceProperties']['TableName'] = self.table_name
        handler(event, self.context)
        
        # Get initial item count
        initial_count = self.table.scan()['Count']
        
        # Send delete request
        delete_event = self.base_event.copy()
        delete_event['RequestType'] = 'Delete'
        
        handler, mock_omics = self._import_handler_with_mocks({})
        handler(delete_event, self.context)
        
        # Verify table wasn't modified
        final_count = self.table.scan()['Count']
        self.assertEqual(initial_count, final_count)
        
        # Verify Omics wasn't called for delete
        mock_omics.list_workflows.assert_not_called()
    
    def test_partial_workflow_match(self):
        """Test when only some workflows are found"""
        # Only mag workflow exists in Omics
        omics_response = {
            'items': [
                {'name': 'nfcore-mag-4-0-0', 'arn': 'arn:aws:omics:us-east-1:123:workflow/mag-123'},
                {'name': 'some-other-workflow', 'arn': 'arn:aws:omics:us-east-1:123:workflow/other'}
            ]
        }
        
        handler, _ = self._import_handler_with_mocks(omics_response)
        
        # Use Update request to avoid retry logic
        event = self.base_event.copy()
        event['RequestType'] = 'Update'
        event['ResourceProperties']['TableName'] = self.table_name
        handler(event, self.context)
        
        # Verify only matched workflows were stored
        response = self.table.get_item(Key={'id': 'workflows'})
        self.assertEqual(len(response['Item']['workflows']), 1)
        self.assertEqual(response['Item']['workflows'][0]['name'], 'mag')
        
        # Verify success response even with partial match
        self.mock_cfnresponse.send.assert_called_once()
        call_args = self.mock_cfnresponse.send.call_args[0]
        self.assertEqual(call_args[2], self.mock_cfnresponse.SUCCESS)
    
    def test_omics_api_failure_handling(self):
        """Test handling of Omics API failures"""
        # Mock Omics to raise exception
        handler, mock_omics = self._import_handler_with_mocks({})
        mock_omics.list_workflows.side_effect = Exception('Omics API throttled')
        
        # Add TableName to event
        event = self.base_event.copy()
        event['ResourceProperties']['TableName'] = self.table_name
        handler(event, self.context)
        
        # Verify no data was stored
        response = self.table.scan()
        self.assertEqual(response['Count'], 0)
        
        # Verify failure response
        self.mock_cfnresponse.send.assert_called_once()
        call_args = self.mock_cfnresponse.send.call_args[0]
        self.assertEqual(call_args[2], self.mock_cfnresponse.FAILED)
    
    def test_environment_variable_parsing(self):
        """Test parsing of WORKFLOW_CONFIG environment variable"""
        # Test with different workflow configurations
        with patch.dict(os.environ, {'WORKFLOW_CONFIG': 'workflow1:1.0.0,workflow2:2.0.0'}):
            omics_response = {
                'items': [
                    {'name': 'nfcore-workflow1-1-0-0', 'arn': 'arn:aws:omics:us-east-1:123:workflow/w1'},
                    {'name': 'nfcore-workflow2-2-0-0', 'arn': 'arn:aws:omics:us-east-1:123:workflow/w2'}
                ]
            }
            
            handler, _ = self._import_handler_with_mocks(omics_response)
            
            # Add TableName to event
            event = self.base_event.copy()
            event['ResourceProperties']['TableName'] = self.table_name
            handler(event, self.context)
            
            # Verify both workflows were processed
            response = self.table.get_item(Key={'id': 'workflows'})
            self.assertEqual(len(response['Item']['workflows']), 2)
            
            workflows = {w['name']: w for w in response['Item']['workflows']}
            self.assertIn('workflow1', workflows)
            self.assertIn('workflow2', workflows)
    
    def test_multiple_workflows_more_than_two(self):
        """Test handling of more than 2 workflows in integration"""
        # Set up environment with 4 workflows
        with patch.dict(os.environ, {'WORKFLOW_CONFIG': 'mag:4.0.0,metatdenovo:1.2.0,ampliseq:2.1.0,atacseq:2.0.0'}):
            # Set up mock Omics responses with all 4 workflows
            mock_omics_responses = {
                'items': [
                    {'name': 'nfcore-mag-4-0-0', 'arn': 'arn:aws:omics:us-east-1:123:workflow/mag'},
                    {'name': 'nfcore-metatdenovo-1-2-0', 'arn': 'arn:aws:omics:us-east-1:123:workflow/meta'},
                    {'name': 'nfcore-ampliseq-2-1-0', 'arn': 'arn:aws:omics:us-east-1:123:workflow/amp'},
                    {'name': 'nfcore-atacseq-2-0-0', 'arn': 'arn:aws:omics:us-east-1:123:workflow/atac'},
                    {'name': 'some-other-workflow', 'arn': 'arn:aws:omics:us-east-1:123:workflow/other'}
                ]
            }
            
            handler, mock_omics = self._import_handler_with_mocks(mock_omics_responses)
            
            # Update event with TableName
            event = self.base_event.copy()
            event['ResourceProperties']['TableName'] = self.table_name
            
            handler(event, self.context)
            
            # Verify all 4 workflows were stored in DynamoDB
            response = self.table.get_item(Key={'id': 'workflows'})
            self.assertIn('Item', response)
            
            item = response['Item']
            self.assertEqual(len(item['workflows']), 4)
            
            # Verify workflow names
            workflow_names = [w['name'] for w in item['workflows']]
            self.assertIn('mag', workflow_names)
            self.assertIn('metatdenovo', workflow_names)
            self.assertIn('ampliseq', workflow_names)
            self.assertIn('atacseq', workflow_names)
            
            # Verify CloudFormation response
            self.mock_cfnresponse.send.assert_called_once()
            cfn_args = self.mock_cfnresponse.send.call_args[0]
            self.assertEqual(cfn_args[2], self.mock_cfnresponse.SUCCESS)
            self.assertEqual(cfn_args[3]['WorkflowCount'], '4')


if __name__ == '__main__':
    unittest.main() 