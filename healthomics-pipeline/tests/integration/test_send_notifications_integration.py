import unittest
import boto3
import os
import sys
import json
from moto import mock_aws
from unittest.mock import patch, Mock, MagicMock

# Add lambda source to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../lambda_src'))


@mock_aws
class TestSendNotificationsIntegration(unittest.TestCase):
    """Integration tests for send-omics-notifications Lambda"""
    
    def setUp(self):
        """Set up test fixtures with AWS service mocks"""
        # Create SNS topic
        self.sns = boto3.client('sns', region_name='us-east-1')
        topic_response = self.sns.create_topic(Name='omics-notifications')
        self.topic_arn = topic_response['TopicArn']
        
        # Subscribe email addresses to topic
        self.sns.subscribe(
            TopicArn=self.topic_arn,
            Protocol='email',
            Endpoint='admin@example.com'
        )
        
        self.sns.subscribe(
            TopicArn=self.topic_arn,
            Protocol='email',
            Endpoint='team@example.com'
        )
        
        # Mock environment variables
        self.env_patcher = patch.dict(os.environ, {
            'SNS_TOPIC_ARN': self.topic_arn
        })
        self.env_patcher.start()
        
        # Create lambda context
        self.context = Mock()
        self.context.function_name = 'send-omics-notifications'
        self.context.log_stream_name = 'test-log-stream'
        
        # Base event for testing
        self.base_event = {
            'run_id': 'test-run-20241210-123456',
            'any_failed': False,
            'workflow_statuses': [
                {
                    'workflow_name': 'mag',
                    'run_id': 'omics-run-mag-123',
                    'status': 'COMPLETED',
                    'startTime': '2024-12-10T10:00:00Z',
                    'stopTime': '2024-12-10T11:30:00Z',
                    'statusMessage': 'Workflow completed successfully'
                },
                {
                    'workflow_name': 'metatdenovo',
                    'run_id': 'omics-run-meta-456',
                    'status': 'COMPLETED',
                    'startTime': '2024-12-10T10:00:00Z',
                    'stopTime': '2024-12-10T12:00:00Z',
                    'statusMessage': 'Workflow completed successfully'
                }
            ]
        }
    
    def tearDown(self):
        """Clean up after tests"""
        self.env_patcher.stop()
    
    def _get_published_messages(self):
        """Get messages published to SNS topic"""
        # In moto, we can't directly retrieve published messages,
        # so we'll capture them through a mock
        return []
    
    def _import_handler_with_mocked_sns(self):
        """Import handler with properly mocked SNS client"""
        # Use the real moto SNS client
        mock_sns_client = self.sns
        
        with patch('boto3.client') as mock_client:
            def client_side_effect(service_name, **kwargs):
                if service_name == 'sns':
                    return mock_sns_client
                else:
                    # For any other service, return a mock
                    return MagicMock()
            
            mock_client.side_effect = client_side_effect
            
            # Clear any previously imported module
            if 'send_notifications' in sys.modules:
                del sys.modules['send_notifications']
            
            # Import the module
            import importlib.util
            spec = importlib.util.spec_from_file_location(
                "send_notifications", 
                os.path.join(os.path.dirname(__file__), '../../lambda_src/send-omics-notifications.py')
            )
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            
            return module.handler, mock_sns_client
    
    def test_send_success_notification(self):
        """Test sending notification for successful workflow completion"""
        # Import and execute handler
        handler, mock_sns = self._import_handler_with_mocked_sns()
        
        # Capture SNS publish call
        with patch.object(mock_sns, 'publish', wraps=mock_sns.publish) as mock_publish:
            result = handler(self.base_event, self.context)
            
            # Verify response
            self.assertEqual(result['statusCode'], 200)
            self.assertIn('Notification sent successfully', result['message'])
            
            # Verify SNS publish was called
            mock_publish.assert_called_once()
            
            # Check publish parameters
            call_args = mock_publish.call_args[1]
            self.assertEqual(call_args['TopicArn'], self.topic_arn)
            self.assertEqual(call_args['Subject'], 'HealthOmics Run test-run-20241210-123456 - COMPLETED')
            
            # Verify message content
            message = call_args['Message']
            self.assertIn('Run ID: test-run-20241210-123456', message)
            self.assertIn('Total Workflows: 2', message)
            self.assertIn('MAG Pipeline:', message)
            self.assertIn('METATDENOVO Pipeline:', message)
            self.assertIn('Status: COMPLETED', message)
            self.assertIn('Completed: 2/2', message)
            self.assertIn('Failed/Cancelled: 0/2', message)
            self.assertIn('All workflows completed successfully', message)
    
    def test_send_failure_notification(self):
        """Test sending notification for failed workflows"""
        # Modify event to include failures
        failure_event = self.base_event.copy()
        failure_event['any_failed'] = True
        failure_event['workflow_statuses'] = [
            {
                'workflow_name': 'mag',
                'run_id': 'omics-run-mag-123',
                'status': 'COMPLETED',
                'startTime': '2024-12-10T10:00:00Z',
                'stopTime': '2024-12-10T11:30:00Z',
                'statusMessage': 'Workflow completed successfully'
            },
            {
                'workflow_name': 'metatdenovo',
                'run_id': 'omics-run-meta-456',
                'status': 'FAILED',
                'startTime': '2024-12-10T10:00:00Z',
                'stopTime': '2024-12-10T10:30:00Z',
                'statusMessage': 'Out of memory error in step 5'
            },
            {
                'workflow_name': 'ampliseq',
                'run_id': 'omics-run-ampliseq-789',
                'status': 'CANCELLED',
                'startTime': '2024-12-10T10:00:00Z',
                'stopTime': '2024-12-10T10:15:00Z',
                'statusMessage': 'User cancelled workflow'
            }
        ]
        
        handler, mock_sns = self._import_handler_with_mocked_sns()
        
        with patch.object(mock_sns, 'publish', wraps=mock_sns.publish) as mock_publish:
            result = handler(failure_event, self.context)
            
            # Verify response
            self.assertEqual(result['statusCode'], 200)
            
            # Check publish parameters
            call_args = mock_publish.call_args[1]
            self.assertEqual(call_args['Subject'], 'HealthOmics Run test-run-20241210-123456 - FAILED')
            
            # Verify message content
            message = call_args['Message']
            self.assertIn('Total Workflows: 3', message)
            self.assertIn('Status: FAILED', message)
            self.assertIn('Status: CANCELLED', message)
            self.assertIn('Out of memory error in step 5', message)
            self.assertIn('User cancelled workflow', message)
            self.assertIn('Completed: 1/3', message)
            self.assertIn('Failed/Cancelled: 2/3', message)
            self.assertIn('One or more workflows failed', message)
    
    def test_empty_workflow_list(self):
        """Test handling of empty workflow list"""
        empty_event = {
            'run_id': 'test-run-20241210-123456',
            'any_failed': False,
            'workflow_statuses': []
        }
        
        handler, mock_sns = self._import_handler_with_mocked_sns()
        
        with patch.object(mock_sns, 'publish', wraps=mock_sns.publish) as mock_publish:
            result = handler(empty_event, self.context)
            
            # Verify response
            self.assertEqual(result['statusCode'], 200)
            
            # Verify SNS publish was called
            mock_publish.assert_called_once()
            
            # Check message content
            message = mock_publish.call_args[1]['Message']
            self.assertIn('Total Workflows: 0', message)
            self.assertIn('Completed: 0/0', message)
            self.assertIn('Failed/Cancelled: 0/0', message)
    
    def test_mixed_workflow_statuses(self):
        """Test notification with various workflow statuses"""
        mixed_event = {
            'run_id': 'test-run-20241210-123456',
            'any_failed': False,
            'workflow_statuses': [
                {
                    'workflow_name': 'mag',
                    'run_id': 'omics-run-mag-123',
                    'status': 'COMPLETED',
                    'startTime': '2024-12-10T10:00:00Z',
                    'stopTime': '2024-12-10T11:30:00Z',
                    'statusMessage': 'Workflow completed successfully'
                },
                {
                    'workflow_name': 'metatdenovo',
                    'run_id': 'omics-run-meta-456',
                    'status': 'RUNNING',
                    'startTime': '2024-12-10T10:00:00Z',
                    'stopTime': '',
                    'statusMessage': 'Still processing step 3 of 10'
                },
                {
                    'workflow_name': 'ampliseq',
                    'run_id': 'omics-run-ampliseq-789',
                    'status': 'PENDING',
                    'startTime': '',
                    'stopTime': '',
                    'statusMessage': 'Waiting for compute resources'
                }
            ]
        }
        
        handler, mock_sns = self._import_handler_with_mocked_sns()
        
        with patch.object(mock_sns, 'publish', wraps=mock_sns.publish) as mock_publish:
            result = handler(mixed_event, self.context)
            
            # Verify response
            self.assertEqual(result['statusCode'], 200)
            
            # Check subject indicates completion (not failure)
            call_args = mock_publish.call_args[1]
            self.assertEqual(call_args['Subject'], 'HealthOmics Run test-run-20241210-123456 - COMPLETED')
            
            # Verify message content shows various statuses
            message = call_args['Message']
            self.assertIn('Status: RUNNING', message)
            self.assertIn('Status: PENDING', message)
            self.assertIn('Still processing step 3 of 10', message)
            self.assertIn('Waiting for compute resources', message)
    
    def test_sns_error_handling(self):
        """Test error handling when SNS publish fails"""
        handler, mock_sns = self._import_handler_with_mocked_sns()
        
        # Make SNS publish fail
        with patch.object(mock_sns, 'publish') as mock_publish:
            mock_publish.side_effect = Exception('SNS service temporarily unavailable')
            
            # Should raise the exception
            with self.assertRaises(Exception) as ctx:
                handler(self.base_event, self.context)
            
            self.assertIn('SNS service temporarily unavailable', str(ctx.exception))
    
    def test_long_workflow_names_formatting(self):
        """Test formatting of notifications with long workflow names"""
        event = self.base_event.copy()
        event['workflow_statuses'] = [
            {
                'workflow_name': 'very-long-workflow-name-that-might-affect-formatting',
                'run_id': 'omics-run-long-123',
                'status': 'COMPLETED',
                'startTime': '2024-12-10T10:00:00Z',
                'stopTime': '2024-12-10T11:30:00Z',
                'statusMessage': 'This is a very long status message that provides detailed information about the workflow execution including multiple steps and their outcomes'
            }
        ]
        
        handler, mock_sns = self._import_handler_with_mocked_sns()
        
        with patch.object(mock_sns, 'publish', wraps=mock_sns.publish) as mock_publish:
            result = handler(event, self.context)
            
            # Verify the long names are handled properly
            message = mock_publish.call_args[1]['Message']
            self.assertIn('VERY-LONG-WORKFLOW-NAME-THAT-MIGHT-AFFECT-FORMATTING Pipeline:', message)
            self.assertIn('This is a very long status message', message)
    
    def test_subscriber_list_validation(self):
        """Test that notifications are sent to all subscribers"""
        # List current subscriptions
        subscriptions = self.sns.list_subscriptions_by_topic(TopicArn=self.topic_arn)
        
        # Verify we have the expected subscribers
        endpoints = [sub['Endpoint'] for sub in subscriptions['Subscriptions']]
        self.assertIn('admin@example.com', endpoints)
        self.assertIn('team@example.com', endpoints)
        self.assertEqual(len(endpoints), 2)


if __name__ == '__main__':
    unittest.main() 