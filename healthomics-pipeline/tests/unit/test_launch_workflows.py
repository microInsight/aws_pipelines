import unittest
from unittest.mock import Mock, patch, MagicMock, call
import sys
import os
import json
from datetime import datetime

# Add lambda source to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../lambda_src'))


class TestLaunchWorkflows(unittest.TestCase):
    """Unit tests for launch-omics-workflows Lambda"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.context = Mock()
        
        self.test_event = {
            'bucket': 'input-bucket',  # Add the missing bucket key
            'manifest': {
                'run_id': 'test-run-123',
                'samplesheet_mag.csv': 's3://input-bucket/run123/samplesheet_mag.csv',
                'samplesheet_metatdenovo.csv': 's3://input-bucket/run123/samplesheet_metatdenovo.csv',
                'sample1_R1.fastq.gz': 's3://input-bucket/run123/sample1_R1.fastq.gz',
                'sample1_R2.fastq.gz': 's3://input-bucket/run123/sample1_R2.fastq.gz'
            },
            'output_bucket': 'output-bucket'
        }
        
        # Mock environment variables
        self.env_patcher = patch.dict(os.environ, {
            'CONFIG_TABLE': 'omics-workflow-config'
        })
        
        # Create fresh mocks for each test
        self.mock_dynamodb = MagicMock()
        self.mock_table = MagicMock()
        self.mock_omics_client = MagicMock()
        self.mock_s3_client = MagicMock()
        
        # Default DynamoDB response
        self.mock_table.get_item.return_value = {
            'Item': {
                'id': 'workflows',
                'workflows': [
                    {
                        'name': 'mag',
                        'version': '4.0.0',
                        'arn': 'arn:aws:omics:us-east-1:123:workflow/mag-workflow'
                    },
                    {
                        'name': 'metatdenovo',
                        'version': '1.2.0',
                        'arn': 'arn:aws:omics:us-east-1:123:workflow/meta-workflow'
                    }
                ],
                'omics_role': 'arn:aws:iam::123456789012:role/OmicsRole',
                'run_group': 'run-group-123'
            }
        }
        
    def _import_handler_with_mocks(self):
        """Import the handler with mocked boto3 clients"""
        with patch('boto3.resource') as mock_resource, \
             patch('boto3.client') as mock_client:
            
            # Set up DynamoDB mock
            mock_resource.return_value = self.mock_dynamodb
            self.mock_dynamodb.Table.return_value = self.mock_table
            
            # Set up client mocks
            def client_side_effect(service_name):
                if service_name == 'omics':
                    return self.mock_omics_client
                elif service_name == 's3':
                    return self.mock_s3_client
                return MagicMock()
            
            mock_client.side_effect = client_side_effect
            
            # Start environment patch
            self.env_patcher.start()
            
            # Import the module
            import importlib.util
            spec = importlib.util.spec_from_file_location(
                "launch_workflows", 
                os.path.join(os.path.dirname(__file__), '../../lambda_src/launch-omics-workflows.py')
            )
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            
            return module.handler
    
    def tearDown(self):
        """Clean up after tests"""
        self.env_patcher.stop()
    
    def test_launch_all_workflows_success(self):
        """Test successful launch of all workflows"""
        # Set up mock responses
        self.mock_s3_client.put_object.return_value = {}
        self.mock_omics_client.start_run.side_effect = [
            {'id': 'omics-run-mag-123', 'arn': 'arn:aws:omics:us-east-1:123:run/mag-123'},
            {'id': 'omics-run-meta-456', 'arn': 'arn:aws:omics:us-east-1:123:run/meta-456'}
        ]
        
        # Import and call handler
        handler = self._import_handler_with_mocks()
        result = handler(self.test_event, self.context)
        
        # Verify DynamoDB was queried
        self.mock_table.get_item.assert_called_once_with(Key={'id': 'workflows'})
        
        # Verify workflow launches (no S3 uploads in the actual Lambda)
        self.assertEqual(self.mock_omics_client.start_run.call_count, 2)
        
        # Check first workflow launch
        mag_call = self.mock_omics_client.start_run.call_args_list[0][1]
        self.assertEqual(mag_call['workflowId'], 'mag-workflow')  # Lambda extracts ID from ARN
        self.assertEqual(mag_call['name'], 'mag-test-run-123')
        
        # Check second workflow launch
        meta_call = self.mock_omics_client.start_run.call_args_list[1][1]
        self.assertEqual(meta_call['workflowId'], 'meta-workflow')
        self.assertEqual(meta_call['name'], 'metatdenovo-test-run-123')
        
        # Verify response
        self.assertEqual(result['statusCode'], 200)
        self.assertEqual(result['run_id'], 'test-run-123')
        self.assertEqual(len(result['launched_workflows']), 2)
        self.assertEqual(result['launched_workflows'][0]['workflow_name'], 'mag')
        self.assertEqual(result['launched_workflows'][1]['workflow_name'], 'metatdenovo')
    
    def test_missing_samplesheet(self):
        """Test when samplesheet for a workflow is missing"""
        # Remove one samplesheet
        del self.test_event['manifest']['samplesheet_metatdenovo.csv']
        
        # Set up mock responses
        self.mock_s3_client.put_object.return_value = {}
        self.mock_omics_client.start_run.return_value = {
            'id': 'omics-run-mag-123',
            'arn': 'arn:aws:omics:us-east-1:123:run/mag-123'
        }
        
        # Import and call handler
        handler = self._import_handler_with_mocks()
        result = handler(self.test_event, self.context)
        
        # Verify only one workflow was launched
        self.assertEqual(self.mock_omics_client.start_run.call_count, 1)
        self.assertEqual(len(result['launched_workflows']), 1)
        self.assertEqual(result['launched_workflows'][0]['workflow_name'], 'mag')
    
    def test_workflow_launch_failure(self):
        """Test handling of workflow launch failure"""
        # Set up mock to fail on second workflow
        self.mock_s3_client.put_object.return_value = {}
        self.mock_omics_client.start_run.side_effect = [
            {'id': 'omics-run-mag-123', 'arn': 'arn:aws:omics:us-east-1:123:run/mag-123'},
            Exception('Insufficient permissions')
        ]
        
        # Import and call handler
        handler = self._import_handler_with_mocks()
        result = handler(self.test_event, self.context)
        
        # Verify first workflow was launched successfully
        self.assertEqual(len(result['launched_workflows']), 1)
        self.assertEqual(result['launched_workflows'][0]['workflow_name'], 'mag')
        
        # Response should still be 200 (partial success)
        self.assertEqual(result['statusCode'], 200)
    
    def test_empty_workflow_config(self):
        """Test when no workflows are configured"""
        # Set up empty workflow config
        self.mock_table.get_item.return_value = {
            'Item': {
                'id': 'workflows',
                'workflows': [],
                'omics_role': 'arn:aws:iam::123456789012:role/OmicsRole',
                'run_group': 'run-group-123'
            }
        }
        
        # Import and call handler
        handler = self._import_handler_with_mocks()
        
        # Should raise exception when no workflows are launched
        with self.assertRaises(Exception) as ctx:
            handler(self.test_event, self.context)
        
        self.assertIn('No workflows were launched', str(ctx.exception))
    
    def test_s3_upload_failure(self):
        """Test handling of S3 upload failure"""
        # This test is not applicable as the Lambda doesn't upload to S3
        # But we'll test that workflow launch continues even if parameters have issues
        
        # Set up mock responses
        self.mock_omics_client.start_run.side_effect = Exception('Parameter validation failed')
        
        # Import and call handler
        handler = self._import_handler_with_mocks()
        
        # Should raise exception when no workflows are launched
        with self.assertRaises(Exception) as ctx:
            handler(self.test_event, self.context)
        
        self.assertIn('No workflows were launched', str(ctx.exception))
    
    def test_workflow_parameters(self):
        """Test correct workflow parameter construction"""
        # Set up mocks
        self.mock_s3_client.put_object.return_value = {}
        self.mock_omics_client.start_run.return_value = {
            'id': 'omics-run-mag-123',
            'arn': 'arn:aws:omics:us-east-1:123:run/mag-123'
        }
        
        # Import and call handler
        handler = self._import_handler_with_mocks()
        result = handler(self.test_event, self.context)
        
        # Check workflow parameters
        start_run_call = self.mock_omics_client.start_run.call_args_list[0][1]
        params = start_run_call['parameters']
        
        # Verify required parameters
        self.assertIn('input', params)
        self.assertIn('outdir', params)
        
        # Verify S3 paths
        self.assertEqual(params['input'], 's3://input-bucket/samplesheet_mag.csv')
        self.assertEqual(params['outdir'], 's3://output-bucket/test-run-123/mag/')


if __name__ == '__main__':
    unittest.main() 