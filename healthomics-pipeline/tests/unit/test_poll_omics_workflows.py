import unittest
from unittest.mock import Mock, patch, MagicMock
import sys
import os

# Add lambda source to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../lambda_src'))


class TestPollOmicsWorkflows(unittest.TestCase):
    """Unit tests for poll-omics-workflows Lambda"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.context = Mock()
        
        self.test_event = {
            'run_id': 'test-run-123',
            'launched_workflows': [
                {
                    'workflow_name': 'mag',
                    'run_id': 'workflow-run-1'
                },
                {
                    'workflow_name': 'metatdenovo',
                    'run_id': 'workflow-run-2'
                }
            ]
        }
        
        # Create a fresh mock for each test
        self.mock_omics_client = MagicMock()
        
    def _import_handler_with_mock(self):
        """Import the handler with mocked boto3 client"""
        with patch('boto3.client') as mock_boto_client:
            mock_boto_client.return_value = self.mock_omics_client
            
            # Import the module
            import importlib.util
            spec = importlib.util.spec_from_file_location(
                "poll_omics_workflows", 
                os.path.join(os.path.dirname(__file__), '../../lambda_src/poll-omics-workflows.py')
            )
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            
            return module.handler
    
    def test_all_workflows_completed(self):
        """Test when all workflows are completed successfully"""
        # Set up mock responses
        self.mock_omics_client.get_run.side_effect = [
            {
                'status': 'COMPLETED',
                'startTime': '2024-01-01T10:00:00Z',
                'stopTime': '2024-01-01T11:00:00Z',
                'statusMessage': 'Workflow completed successfully'
            },
            {
                'status': 'COMPLETED',
                'startTime': '2024-01-01T10:00:00Z',
                'stopTime': '2024-01-01T11:30:00Z',
                'statusMessage': 'Workflow completed successfully'
            }
        ]
        
        # Import and call handler
        handler = self._import_handler_with_mock()
        result = handler(self.test_event, self.context)
        
        # Verify results
        self.assertEqual(result['statusCode'], 200)
        self.assertEqual(result['run_id'], 'test-run-123')
        self.assertTrue(result['all_complete'])
        self.assertFalse(result['any_failed'])
        
        # Verify workflow statuses
        statuses = result['workflow_statuses']
        self.assertEqual(len(statuses), 2)
        self.assertEqual(statuses[0]['status'], 'COMPLETED')
        self.assertEqual(statuses[1]['status'], 'COMPLETED')
    
    def test_workflow_still_running(self):
        """Test when some workflows are still running"""
        # Set up mock responses
        self.mock_omics_client.get_run.side_effect = [
            {
                'status': 'COMPLETED',
                'startTime': '2024-01-01T10:00:00Z',
                'stopTime': '2024-01-01T11:00:00Z',
                'statusMessage': 'Workflow completed successfully'
            },
            {
                'status': 'RUNNING',
                'startTime': '2024-01-01T10:00:00Z',
                'statusMessage': 'Workflow is running'
            }
        ]
        
        # Import and call handler
        handler = self._import_handler_with_mock()
        result = handler(self.test_event, self.context)
        
        # Verify results
        self.assertEqual(result['statusCode'], 200)
        self.assertFalse(result['all_complete'])
        self.assertFalse(result['any_failed'])
        
        # Verify workflow statuses
        statuses = result['workflow_statuses']
        self.assertEqual(statuses[0]['status'], 'COMPLETED')
        self.assertEqual(statuses[1]['status'], 'RUNNING')
    
    def test_workflow_failed(self):
        """Test when a workflow has failed"""
        # Set up mock responses
        self.mock_omics_client.get_run.side_effect = [
            {
                'status': 'COMPLETED',
                'startTime': '2024-01-01T10:00:00Z',
                'stopTime': '2024-01-01T11:00:00Z',
                'statusMessage': 'Workflow completed successfully'
            },
            {
                'status': 'FAILED',
                'startTime': '2024-01-01T10:00:00Z',
                'stopTime': '2024-01-01T10:30:00Z',
                'statusMessage': 'Workflow failed: Out of memory'
            }
        ]
        
        # Import and call handler
        handler = self._import_handler_with_mock()
        result = handler(self.test_event, self.context)
        
        # Verify results
        self.assertEqual(result['statusCode'], 200)
        self.assertTrue(result['all_complete'])
        self.assertTrue(result['any_failed'])
        
        # Verify workflow statuses
        statuses = result['workflow_statuses']
        self.assertEqual(statuses[0]['status'], 'COMPLETED')
        self.assertEqual(statuses[1]['status'], 'FAILED')
        self.assertEqual(statuses[1]['statusMessage'], 'Workflow failed: Out of memory')
    
    def test_api_error_handling(self):
        """Test error handling when API calls fail"""
        # Set up mock to raise exception
        self.mock_omics_client.get_run.side_effect = [
            {
                'status': 'COMPLETED',
                'startTime': '2024-01-01T10:00:00Z',
                'stopTime': '2024-01-01T11:00:00Z',
                'statusMessage': 'Workflow completed successfully'
            },
            Exception('API rate limit exceeded')
        ]
        
        # Import and call handler
        handler = self._import_handler_with_mock()
        result = handler(self.test_event, self.context)
        
        # Verify results
        self.assertEqual(result['statusCode'], 200)
        self.assertTrue(result['all_complete'])  # First one completed
        self.assertTrue(result['any_failed'])  # Second one failed due to error
        
        # Verify workflow statuses
        statuses = result['workflow_statuses']
        self.assertEqual(statuses[0]['status'], 'COMPLETED')
        self.assertEqual(statuses[1]['status'], 'UNKNOWN')
        self.assertIn('API rate limit exceeded', statuses[1]['statusMessage'])
    
    def test_empty_workflow_list(self):
        """Test handling of empty workflow list"""
        # Set up empty event
        empty_event = {
            'run_id': 'test-run-123',
            'launched_workflows': []
        }
        
        # Import and call handler
        handler = self._import_handler_with_mock()
        result = handler(empty_event, self.context)
        
        # Verify results
        self.assertEqual(result['statusCode'], 200)
        self.assertTrue(result['all_complete'])
        self.assertFalse(result['any_failed'])
        self.assertEqual(len(result['workflow_statuses']), 0)


if __name__ == '__main__':
    unittest.main() 