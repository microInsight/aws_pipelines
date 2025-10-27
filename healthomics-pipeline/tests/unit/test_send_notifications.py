import unittest
from unittest.mock import Mock, patch, MagicMock
import sys
import os

# Add lambda source to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../lambda_src'))


class TestSendNotifications(unittest.TestCase):
    """Unit tests for send-omics-notifications Lambda"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.context = Mock()
        
        self.test_event = {
            'run_id': 'test-run-123',
            'any_failed': False,
            'workflow_statuses': [
                {
                    'workflow_name': 'mag',
                    'run_id': 'workflow-run-1',
                    'status': 'COMPLETED',
                    'startTime': '2024-01-01T10:00:00Z',
                    'stopTime': '2024-01-01T11:00:00Z',
                    'statusMessage': 'Workflow completed successfully'
                },
                {
                    'workflow_name': 'metatdenovo',
                    'run_id': 'workflow-run-2',
                    'status': 'COMPLETED',
                    'startTime': '2024-01-01T10:00:00Z',
                    'stopTime': '2024-01-01T11:30:00Z',
                    'statusMessage': 'Workflow completed successfully'
                }
            ]
        }
        
        # Mock environment variables
        self.env_patcher = patch.dict(os.environ, {
            'SNS_TOPIC_ARN': 'arn:aws:sns:us-east-1:123456789012:test-topic'
        })
        
        # Create a fresh mock for each test
        self.mock_sns_client = MagicMock()
        
    def _import_handler_with_mock(self):
        """Import the handler with mocked boto3 client"""
        with patch('boto3.client') as mock_boto_client:
            mock_boto_client.return_value = self.mock_sns_client
            
            # Start environment patch
            self.env_patcher.start()
            
            # Import the module
            import importlib.util
            spec = importlib.util.spec_from_file_location(
                "send_notifications", 
                os.path.join(os.path.dirname(__file__), '../../lambda_src/send-omics-notifications.py')
            )
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            
            return module.handler
    
    def tearDown(self):
        """Clean up after tests"""
        self.env_patcher.stop()
    
    def test_send_success_notification(self):
        """Test sending notification for successful runs"""
        # Import and call handler
        handler = self._import_handler_with_mock()
        result = handler(self.test_event, self.context)
        
        # Verify response
        self.assertEqual(result['statusCode'], 200)
        self.assertEqual(result['message'], 'Notification sent successfully to all subscribers')
        
        # Verify SNS publish was called
        self.mock_sns_client.publish.assert_called_once()
        call_args = self.mock_sns_client.publish.call_args[1]
        
        # Verify topic ARN
        self.assertEqual(call_args['TopicArn'], 'arn:aws:sns:us-east-1:123456789012:test-topic')
        
        # Verify subject
        self.assertEqual(call_args['Subject'], 'HealthOmics Run test-run-123 - COMPLETED')
        
        # Verify message content
        message = call_args['Message']
        self.assertIn('Run ID: test-run-123', message)
        self.assertIn('Total Workflows: 2', message)
        self.assertIn('MAG Pipeline:', message)
        self.assertIn('METATDENOVO Pipeline:', message)
        self.assertIn('Completed: 2/2', message)
        self.assertIn('Failed/Cancelled: 0/2', message)
        self.assertIn('All workflows completed successfully', message)
    
    def test_send_failure_notification(self):
        """Test sending notification for failed runs"""
        # Modify event to include a failure
        self.test_event['any_failed'] = True
        self.test_event['workflow_statuses'][1]['status'] = 'FAILED'
        self.test_event['workflow_statuses'][1]['statusMessage'] = 'Out of memory error'
        
        # Import and call handler
        handler = self._import_handler_with_mock()
        result = handler(self.test_event, self.context)
        
        # Verify response
        self.assertEqual(result['statusCode'], 200)
        
        # Verify SNS publish was called
        self.mock_sns_client.publish.assert_called_once()
        call_args = self.mock_sns_client.publish.call_args[1]
        
        # Verify subject indicates failure
        self.assertEqual(call_args['Subject'], 'HealthOmics Run test-run-123 - FAILED')
        
        # Verify message content
        message = call_args['Message']
        self.assertIn('Status: FAILED', message)
        self.assertIn('Out of memory error', message)
        self.assertIn('Completed: 1/2', message)
        self.assertIn('Failed/Cancelled: 1/2', message)
        self.assertIn('One or more workflows failed', message)
    
    def test_mixed_status_notification(self):
        """Test notification with mixed statuses (completed, failed, cancelled)"""
        # Add more workflow statuses
        self.test_event['any_failed'] = True
        self.test_event['workflow_statuses'].append({
            'workflow_name': 'ampliseq',
            'run_id': 'workflow-run-3',
            'status': 'CANCELLED',
            'startTime': '2024-01-01T10:00:00Z',
            'stopTime': '2024-01-01T10:15:00Z',
            'statusMessage': 'User cancelled workflow'
        })
        
        # Import and call handler
        handler = self._import_handler_with_mock()
        result = handler(self.test_event, self.context)
        
        # Verify response
        self.assertEqual(result['statusCode'], 200)
        
        # Verify SNS publish was called
        call_args = self.mock_sns_client.publish.call_args[1]
        message = call_args['Message']
        
        # Verify summary counts
        self.assertIn('Total Workflows: 3', message)
        self.assertIn('Completed: 2/3', message)
        self.assertIn('Failed/Cancelled: 1/3', message)
    
    def test_sns_publish_error(self):
        """Test error handling when SNS publish fails"""
        # Set up mock to raise exception
        self.mock_sns_client.publish.side_effect = Exception('SNS access denied')
        
        # Import handler
        handler = self._import_handler_with_mock()
        
        # Call should raise the exception
        with self.assertRaises(Exception) as ctx:
            handler(self.test_event, self.context)
        
        self.assertIn('SNS access denied', str(ctx.exception))
    
    def test_empty_workflow_statuses(self):
        """Test handling of empty workflow statuses"""
        # Set up event with no workflows
        empty_event = {
            'run_id': 'test-run-123',
            'any_failed': False,
            'workflow_statuses': []
        }
        
        # Import and call handler
        handler = self._import_handler_with_mock()
        result = handler(empty_event, self.context)
        
        # Verify response
        self.assertEqual(result['statusCode'], 200)
        
        # Verify SNS publish was called
        call_args = self.mock_sns_client.publish.call_args[1]
        message = call_args['Message']
        
        # Verify message handles empty list gracefully
        self.assertIn('Total Workflows: 0', message)
        self.assertIn('Completed: 0/0', message)
        self.assertIn('Failed/Cancelled: 0/0', message)


if __name__ == '__main__':
    unittest.main() 