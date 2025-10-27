import unittest
import boto3
import os
import sys
import json
from moto import mock_aws
from unittest.mock import patch, MagicMock, Mock
from datetime import datetime

# Add lambda source to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../lambda_src'))


@mock_aws
class TestLaunchWorkflowsIntegration(unittest.TestCase):
    """Integration tests for launch-omics-workflows Lambda"""
    
    def setUp(self):
        """Set up test fixtures with AWS service mocks"""
        # Create DynamoDB table and populate it
        self.dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        self.table = self.dynamodb.create_table(
            TableName='omics-workflow-config',
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
        
        # Populate DynamoDB with workflow configuration
        self.table.put_item(Item={
            'id': 'workflows',
            'workflows': [
                {
                    'name': 'mag',
                    'version': '4.0.0',
                    'arn': 'arn:aws:omics:us-east-1:123456789012:workflow/mag-workflow-id'
                },
                {
                    'name': 'metatdenovo',
                    'version': '1.2.0',
                    'arn': 'arn:aws:omics:us-east-1:123456789012:workflow/meta-workflow-id'
                },
                {
                    'name': 'ampliseq',
                    'version': '2.5.0',
                    'arn': 'arn:aws:omics:us-east-1:123456789012:workflow/ampliseq-workflow-id'
                }
            ],
            'omics_role': 'arn:aws:iam::123456789012:role/OmicsServiceRole',
            'run_group': 'run-group-123'
        })
        
        # Create S3 bucket
        self.s3 = boto3.client('s3', region_name='us-east-1')
        self.input_bucket = 'test-input-bucket'
        self.output_bucket = 'test-output-bucket'
        self.s3.create_bucket(Bucket=self.input_bucket)
        self.s3.create_bucket(Bucket=self.output_bucket)
        
        # Upload sample samplesheets to S3
        self.s3.put_object(
            Bucket=self.input_bucket,
            Key='samplesheet_mag.csv',
            Body=b'sample,fastq_1,fastq_2\nsample1,s3://bucket/sample1_R1.fastq.gz,s3://bucket/sample1_R2.fastq.gz'
        )
        
        self.s3.put_object(
            Bucket=self.input_bucket,
            Key='samplesheet_metatdenovo.csv',
            Body=b'sample,fastq_1,fastq_2\nsample2,s3://bucket/sample2_R1.fastq.gz,s3://bucket/sample2_R2.fastq.gz'
        )
        
        # Mock environment variables
        self.env_patcher = patch.dict(os.environ, {
            'CONFIG_TABLE': 'omics-workflow-config'
        })
        self.env_patcher.start()
        
        # Create lambda context
        self.context = Mock()
        self.context.function_name = 'launch-omics-workflows'
        self.context.log_stream_name = 'test-log-stream'
        
        # Base event for testing
        self.base_event = {
            'bucket': self.input_bucket,
            'output_bucket': self.output_bucket,
            'manifest': {
                'run_id': 'test-run-20241210-123456',
                'samplesheet_mag.csv': f's3://{self.input_bucket}/samplesheet_mag.csv',
                'samplesheet_metatdenovo.csv': f's3://{self.input_bucket}/samplesheet_metatdenovo.csv',
                'sample1_R1.fastq.gz': f's3://{self.input_bucket}/sample1_R1.fastq.gz',
                'sample1_R2.fastq.gz': f's3://{self.input_bucket}/sample1_R2.fastq.gz'
            }
        }
    
    def tearDown(self):
        """Clean up after tests"""
        self.env_patcher.stop()
    
    def _import_handler_with_mocked_omics(self, omics_responses):
        """Import handler with mocked Omics client"""
        mock_omics_client = MagicMock()
        
        # Set up start_run responses
        if isinstance(omics_responses, list):
            mock_omics_client.start_run.side_effect = omics_responses
        else:
            mock_omics_client.start_run.return_value = omics_responses
        
        # Create mocks for all clients/resources
        mock_s3_client = self.s3  # Use the real moto S3 client
        mock_dynamodb_resource = self.dynamodb  # Use the real moto DynamoDB resource
        
        with patch('boto3.client') as mock_client, \
             patch('boto3.resource') as mock_resource:
            
            def client_side_effect(service_name, **kwargs):
                if service_name == 'omics':
                    return mock_omics_client
                elif service_name == 's3':
                    return mock_s3_client
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
            if 'launch_workflows' in sys.modules:
                del sys.modules['launch_workflows']
            
            # Import the module
            import importlib.util
            spec = importlib.util.spec_from_file_location(
                "launch_workflows", 
                os.path.join(os.path.dirname(__file__), '../../lambda_src/launch-omics-workflows.py')
            )
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            
            return module.handler, mock_omics_client
    
    def test_successful_workflow_launch(self):
        """Test successful launch of multiple workflows"""
        # Mock Omics responses
        omics_responses = [
            {
                'id': 'omics-run-mag-123',
                'arn': 'arn:aws:omics:us-east-1:123456789012:run/mag-run-123',
                'status': 'PENDING'
            },
            {
                'id': 'omics-run-meta-456',
                'arn': 'arn:aws:omics:us-east-1:123456789012:run/meta-run-456',
                'status': 'PENDING'
            }
        ]
        
        handler, mock_omics = self._import_handler_with_mocked_omics(omics_responses)
        result = handler(self.base_event, self.context)
        
        # Verify response
        self.assertEqual(result['statusCode'], 200)
        self.assertEqual(result['run_id'], 'test-run-20241210-123456')
        self.assertEqual(len(result['launched_workflows']), 2)
        
        # Verify workflow details
        launched = {w['workflow_name']: w for w in result['launched_workflows']}
        self.assertIn('mag', launched)
        self.assertEqual(launched['mag']['run_id'], 'omics-run-mag-123')
        
        self.assertIn('metatdenovo', launched)
        self.assertEqual(launched['metatdenovo']['run_id'], 'omics-run-meta-456')
        
        # Verify Omics calls
        self.assertEqual(mock_omics.start_run.call_count, 2)
        
        # Check first workflow launch parameters
        mag_call = mock_omics.start_run.call_args_list[0][1]
        self.assertEqual(mag_call['workflowId'], 'mag-workflow-id')
        self.assertEqual(mag_call['name'], 'mag-test-run-20241210-123456')
        self.assertEqual(mag_call['roleArn'], 'arn:aws:iam::123456789012:role/OmicsServiceRole')
        self.assertEqual(mag_call['runGroupId'], 'run-group-123')
        self.assertIn('input', mag_call['parameters'])
        self.assertIn('outdir', mag_call['parameters'])
    
    def test_missing_samplesheet_skips_workflow(self):
        """Test that workflows without samplesheets are skipped"""
        # Remove metatdenovo samplesheet from manifest
        event = self.base_event.copy()
        del event['manifest']['samplesheet_metatdenovo.csv']
        
        # Only one workflow should be launched
        omics_response = {
            'id': 'omics-run-mag-123',
            'arn': 'arn:aws:omics:us-east-1:123456789012:run/mag-run-123',
            'status': 'PENDING'
        }
        
        handler, mock_omics = self._import_handler_with_mocked_omics(omics_response)
        result = handler(event, self.context)
        
        # Verify only one workflow was launched
        self.assertEqual(len(result['launched_workflows']), 1)
        self.assertEqual(result['launched_workflows'][0]['workflow_name'], 'mag')
        
        # Verify Omics was called only once
        self.assertEqual(mock_omics.start_run.call_count, 1)
    
    def test_workflow_launch_failure_continues_others(self):
        """Test that failure of one workflow doesn't stop others"""
        # First workflow fails, second succeeds
        omics_responses = [
            Exception('InsufficientCapacityException: No compute capacity available'),
            {
                'id': 'omics-run-meta-456',
                'arn': 'arn:aws:omics:us-east-1:123456789012:run/meta-run-456',
                'status': 'PENDING'
            }
        ]
        
        handler, mock_omics = self._import_handler_with_mocked_omics(omics_responses)
        result = handler(self.base_event, self.context)
        
        # Verify partial success
        self.assertEqual(result['statusCode'], 200)
        self.assertEqual(len(result['launched_workflows']), 1)
        self.assertEqual(result['launched_workflows'][0]['workflow_name'], 'metatdenovo')
        
        # Verify both workflows were attempted
        self.assertEqual(mock_omics.start_run.call_count, 2)
    
    def test_no_workflows_launched_raises_exception(self):
        """Test that exception is raised when no workflows are launched"""
        # Remove all samplesheets
        event = self.base_event.copy()
        event['manifest'] = {'run_id': 'test-run-123'}
        
        handler, mock_omics = self._import_handler_with_mocked_omics([])
        
        # Should raise exception
        with self.assertRaises(Exception) as ctx:
            handler(event, self.context)
        
        self.assertIn('No workflows were launched', str(ctx.exception))
    
    def test_dynamodb_configuration_missing(self):
        """Test handling when DynamoDB configuration is missing"""
        # Remove the configuration from DynamoDB
        self.table.delete_item(Key={'id': 'workflows'})
        
        handler, mock_omics = self._import_handler_with_mocked_omics([])
        
        # Should raise KeyError when trying to access missing item
        with self.assertRaises(KeyError):
            handler(self.base_event, self.context)
    
    def test_workflow_parameters_include_metadata(self):
        """Test that workflow parameters include proper metadata"""
        omics_response = {
            'id': 'omics-run-mag-123',
            'arn': 'arn:aws:omics:us-east-1:123456789012:run/mag-run-123',
            'status': 'PENDING'
        }
        
        handler, mock_omics = self._import_handler_with_mocked_omics(omics_response)
        
        # Capture datetime before execution
        before_time = datetime.now()
        
        result = handler(self.base_event, self.context)
        
        # Verify tags include metadata
        mag_call = mock_omics.start_run.call_args_list[0][1]
        tags = mag_call['tags']
        
        self.assertEqual(tags['run_id'], 'test-run-20241210-123456')
        self.assertEqual(tags['workflow'], 'mag')
        self.assertIn('start_time', tags)
        
        # Verify timestamp is reasonable
        start_time = datetime.fromisoformat(tags['start_time'])
        after_time = datetime.now()
        self.assertTrue(before_time <= start_time <= after_time)
    
    def test_output_uri_formatting(self):
        """Test that output URIs are properly formatted"""
        omics_response = {
            'id': 'omics-run-mag-123',
            'arn': 'arn:aws:omics:us-east-1:123456789012:run/mag-run-123',
            'status': 'PENDING'
        }
        
        handler, mock_omics = self._import_handler_with_mocked_omics(omics_response)
        result = handler(self.base_event, self.context)
        
        # Check outputUri parameter
        mag_call = mock_omics.start_run.call_args_list[0][1]
        expected_uri = f"s3://{self.output_bucket}/test-run-20241210-123456/mag/"
        self.assertEqual(mag_call['outputUri'], expected_uri)
        
        # Check outdir in parameters
        params = mag_call['parameters']
        self.assertEqual(params['outdir'], expected_uri)


if __name__ == '__main__':
    unittest.main() 