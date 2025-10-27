import unittest
import os
import sys
from unittest.mock import patch, MagicMock, Mock
from datetime import datetime, timezone

# Add lambda source to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../lambda_src'))


class TestPollWorkflowsIntegration(unittest.TestCase):
    """Integration tests for poll-omics-workflows Lambda"""
    
    def setUp(self):
        """Set up test fixtures"""
        # Create lambda context
        self.context = Mock()
        self.context.function_name = 'poll-omics-workflows'
        self.context.log_stream_name = 'test-log-stream'
        
        # Base event for testing
        self.base_event = {
            'run_id': 'test-run-20241210-123456',
            'launched_workflows': [
                {
                    'workflow_name': 'mag',
                    'run_id': 'omics-run-mag-123',
                    'arn': 'arn:aws:omics:us-east-1:123456789012:run/mag-123'
                },
                {
                    'workflow_name': 'metatdenovo',
                    'run_id': 'omics-run-meta-456',
                    'arn': 'arn:aws:omics:us-east-1:123456789012:run/meta-456'
                },
                {
                    'workflow_name': 'ampliseq',
                    'run_id': 'omics-run-ampliseq-789',
                    'arn': 'arn:aws:omics:us-east-1:123456789012:run/ampliseq-789'
                }
            ]
        }
    
    def _import_handler_with_mocked_omics(self, get_run_responses):
        """Import handler with mocked Omics client"""
        mock_omics_client = MagicMock()
        
        # Set up get_run responses
        if isinstance(get_run_responses, list):
            mock_omics_client.get_run.side_effect = get_run_responses
        else:
            mock_omics_client.get_run.return_value = get_run_responses
        
        with patch('boto3.client') as mock_client:
            mock_client.return_value = mock_omics_client
            
            # Clear any previously imported module
            if 'poll_workflows' in sys.modules:
                del sys.modules['poll_workflows']
            
            # Import the module
            import importlib.util
            spec = importlib.util.spec_from_file_location(
                "poll_workflows", 
                os.path.join(os.path.dirname(__file__), '../../lambda_src/poll-omics-workflows.py')
            )
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            
            return module.handler, mock_omics_client
    
    def test_all_workflows_running(self):
        """Test polling when all workflows are still running"""
        # Create timestamps
        start_time = datetime.now(timezone.utc)
        
        get_run_responses = [
            {
                'id': 'omics-run-mag-123',
                'status': 'RUNNING',
                'startTime': start_time,
                'statusMessage': 'Processing step 5 of 10'
            },
            {
                'id': 'omics-run-meta-456',
                'status': 'RUNNING',
                'startTime': start_time,
                'statusMessage': 'Processing step 3 of 8'
            },
            {
                'id': 'omics-run-ampliseq-789',
                'status': 'STARTING',
                'startTime': start_time,
                'statusMessage': 'Initializing compute resources'
            }
        ]
        
        handler, mock_omics = self._import_handler_with_mocked_omics(get_run_responses)
        result = handler(self.base_event, self.context)
        
        # Verify response
        self.assertEqual(result['statusCode'], 200)
        self.assertEqual(result['run_id'], 'test-run-20241210-123456')
        self.assertFalse(result['all_complete'])
        self.assertFalse(result['any_failed'])
        
        # Verify all workflows were polled
        self.assertEqual(mock_omics.get_run.call_count, 3)
        
        # Verify workflow statuses
        statuses = {s['workflow_name']: s for s in result['workflow_statuses']}
        self.assertEqual(statuses['mag']['status'], 'RUNNING')
        self.assertEqual(statuses['metatdenovo']['status'], 'RUNNING')
        self.assertEqual(statuses['ampliseq']['status'], 'STARTING')
    
    def test_all_workflows_completed(self):
        """Test polling when all workflows have completed successfully"""
        start_time = datetime.now(timezone.utc)
        stop_time = datetime.now(timezone.utc)
        
        get_run_responses = [
            {
                'id': 'omics-run-mag-123',
                'status': 'COMPLETED',
                'startTime': start_time,
                'stopTime': stop_time,
                'statusMessage': 'Workflow completed successfully'
            },
            {
                'id': 'omics-run-meta-456',
                'status': 'COMPLETED',
                'startTime': start_time,
                'stopTime': stop_time,
                'statusMessage': 'Workflow completed successfully'
            },
            {
                'id': 'omics-run-ampliseq-789',
                'status': 'COMPLETED',
                'startTime': start_time,
                'stopTime': stop_time,
                'statusMessage': 'Workflow completed successfully'
            }
        ]
        
        handler, mock_omics = self._import_handler_with_mocked_omics(get_run_responses)
        result = handler(self.base_event, self.context)
        
        # Verify response
        self.assertEqual(result['statusCode'], 200)
        self.assertTrue(result['all_complete'])
        self.assertFalse(result['any_failed'])
        
        # Verify all workflows show completed
        for status in result['workflow_statuses']:
            self.assertEqual(status['status'], 'COMPLETED')
    
    def test_mixed_workflow_statuses(self):
        """Test polling with mixed workflow statuses"""
        start_time = datetime.now(timezone.utc)
        stop_time = datetime.now(timezone.utc)
        
        get_run_responses = [
            {
                'id': 'omics-run-mag-123',
                'status': 'COMPLETED',
                'startTime': start_time,
                'stopTime': stop_time,
                'statusMessage': 'Workflow completed successfully'
            },
            {
                'id': 'omics-run-meta-456',
                'status': 'FAILED',
                'startTime': start_time,
                'stopTime': stop_time,
                'statusMessage': 'Out of memory error in step 7',
                'failureReason': 'RESOURCE_LIMIT_EXCEEDED'
            },
            {
                'id': 'omics-run-ampliseq-789',
                'status': 'RUNNING',
                'startTime': start_time,
                'statusMessage': 'Processing step 2 of 5'
            }
        ]
        
        handler, mock_omics = self._import_handler_with_mocked_omics(get_run_responses)
        result = handler(self.base_event, self.context)
        
        # Verify response
        self.assertEqual(result['statusCode'], 200)
        self.assertFalse(result['all_complete'])  # One is still running
        self.assertTrue(result['any_failed'])      # One failed
        
        # Verify individual statuses
        statuses = {s['workflow_name']: s for s in result['workflow_statuses']}
        self.assertEqual(statuses['mag']['status'], 'COMPLETED')
        self.assertEqual(statuses['metatdenovo']['status'], 'FAILED')
        self.assertEqual(statuses['ampliseq']['status'], 'RUNNING')
        
        # Verify failure message is captured
        self.assertIn('Out of memory error', statuses['metatdenovo']['statusMessage'])
    
    def test_workflow_cancelled(self):
        """Test handling of cancelled workflows"""
        start_time = datetime.now(timezone.utc)
        stop_time = datetime.now(timezone.utc)
        
        get_run_responses = [
            {
                'id': 'omics-run-mag-123',
                'status': 'COMPLETED',
                'startTime': start_time,
                'stopTime': stop_time,
                'statusMessage': 'Workflow completed successfully'
            },
            {
                'id': 'omics-run-meta-456',
                'status': 'CANCELLED',
                'startTime': start_time,
                'stopTime': stop_time,
                'statusMessage': 'Workflow cancelled by user'
            },
            {
                'id': 'omics-run-ampliseq-789',
                'status': 'COMPLETED',
                'startTime': start_time,
                'stopTime': stop_time,
                'statusMessage': 'Workflow completed successfully'
            }
        ]
        
        handler, mock_omics = self._import_handler_with_mocked_omics(get_run_responses)
        result = handler(self.base_event, self.context)
        
        # Verify response
        self.assertTrue(result['all_complete'])   # Cancelled counts as complete
        self.assertTrue(result['any_failed'])     # Cancelled counts as failed
        
        # Verify cancelled status
        statuses = {s['workflow_name']: s for s in result['workflow_statuses']}
        self.assertEqual(statuses['metatdenovo']['status'], 'CANCELLED')
    
    def test_api_error_handling(self):
        """Test handling of API errors during polling"""
        get_run_responses = [
            {
                'id': 'omics-run-mag-123',
                'status': 'COMPLETED',
                'startTime': datetime.now(timezone.utc),
                'stopTime': datetime.now(timezone.utc),
                'statusMessage': 'Workflow completed successfully'
            },
            Exception('ResourceNotFoundException: Run not found'),
            {
                'id': 'omics-run-ampliseq-789',
                'status': 'RUNNING',
                'startTime': datetime.now(timezone.utc),
                'statusMessage': 'Processing'
            }
        ]
        
        handler, mock_omics = self._import_handler_with_mocked_omics(get_run_responses)
        result = handler(self.base_event, self.context)
        
        # Verify response continues despite error
        self.assertEqual(result['statusCode'], 200)
        self.assertFalse(result['all_complete'])
        self.assertTrue(result['any_failed'])
        
        # Verify error is captured in status
        statuses = {s['workflow_name']: s for s in result['workflow_statuses']}
        self.assertEqual(statuses['metatdenovo']['status'], 'UNKNOWN')
        self.assertIn('ResourceNotFoundException', statuses['metatdenovo']['statusMessage'])
    
    def test_empty_workflow_list(self):
        """Test handling of empty workflow list"""
        event = {
            'run_id': 'test-run-123',
            'launched_workflows': []
        }
        
        handler, mock_omics = self._import_handler_with_mocked_omics([])
        result = handler(event, self.context)
        
        # Verify response
        self.assertEqual(result['statusCode'], 200)
        self.assertTrue(result['all_complete'])
        self.assertFalse(result['any_failed'])
        self.assertEqual(len(result['workflow_statuses']), 0)
        
        # Verify no API calls were made
        mock_omics.get_run.assert_not_called()
    
    def test_timestamp_formatting(self):
        """Test proper formatting of timestamps in response"""
        # Use specific timestamps
        start_time = datetime(2024, 12, 10, 10, 30, 45, tzinfo=timezone.utc)
        stop_time = datetime(2024, 12, 10, 12, 15, 30, tzinfo=timezone.utc)
        
        get_run_responses = {
            'id': 'omics-run-mag-123',
            'status': 'COMPLETED',
            'startTime': start_time,
            'stopTime': stop_time,
            'statusMessage': 'Workflow completed successfully'
        }
        
        # Test with single workflow
        event = {
            'run_id': 'test-run-123',
            'launched_workflows': [
                {
                    'workflow_name': 'mag',
                    'run_id': 'omics-run-mag-123',
                    'arn': 'arn:aws:omics:us-east-1:123456789012:run/mag-123'
                }
            ]
        }
        
        handler, mock_omics = self._import_handler_with_mocked_omics(get_run_responses)
        result = handler(event, self.context)
        
        # Verify timestamp formatting
        status = result['workflow_statuses'][0]
        self.assertEqual(status['startTime'], '2024-12-10 10:30:45+00:00')
        self.assertEqual(status['stopTime'], '2024-12-10 12:15:30+00:00')
    
    def test_all_statuses_preserved(self):
        """Test that all status fields are preserved in response"""
        get_run_response = {
            'id': 'omics-run-mag-123',
            'status': 'FAILED',
            'startTime': datetime.now(timezone.utc),
            'stopTime': datetime.now(timezone.utc),
            'statusMessage': 'Detailed error message',
            'failureReason': 'INTERNAL_ERROR',
            'totalBases': 1234567890,
            'totalReads': 9876543,
            'outputUri': 's3://bucket/output/',
            'logLocation': 's3://bucket/logs/'
        }
        
        # Test with single workflow
        event = {
            'run_id': 'test-run-123',
            'launched_workflows': [
                {
                    'workflow_name': 'mag',
                    'run_id': 'omics-run-mag-123',
                    'arn': 'arn:aws:omics:us-east-1:123456789012:run/mag-123'
                }
            ]
        }
        
        handler, mock_omics = self._import_handler_with_mocked_omics(get_run_response)
        result = handler(event, self.context)
        
        # Verify all required fields are in response
        status = result['workflow_statuses'][0]
        self.assertEqual(status['workflow_name'], 'mag')
        self.assertEqual(status['run_id'], 'omics-run-mag-123')
        self.assertEqual(status['status'], 'FAILED')
        self.assertIn('startTime', status)
        self.assertIn('stopTime', status)
        self.assertEqual(status['statusMessage'], 'Detailed error message')


if __name__ == '__main__':
    unittest.main() 