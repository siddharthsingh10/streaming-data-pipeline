"""
Tests for the dead letter handler.
"""
import pytest
import tempfile
import shutil
from src.dead_letter_handler import DeadLetterHandler, process_dead_letter_event


class TestDeadLetterHandler:
    """Test DeadLetterHandler functionality."""
    
    def setup_method(self):
        """Set up test environment."""
        self.temp_dir = tempfile.mkdtemp()
        self.handler = DeadLetterHandler()
    
    def teardown_method(self):
        """Clean up test environment."""
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_process_dead_letter_event(self):
        """Test processing a dead letter event."""
        event = {
            "original_event": {
                "user_id": "user-123",
                "event_type": "invalid_event",
                "timestamp": "2023-01-01T12:00:00"
            },
            "error_type": "ValidationError",
            "error_message": "Invalid event type",
            "failed_at": "2023-01-01T12:00:00",
            "processing_stage": "producer_validation"
        }
        
        success = self.handler.process_dead_letter_event(event)
        assert success is True
        
        # Check that error analysis was added
        assert 'error_analysis' in event
        analysis = event['error_analysis']
        assert 'error_category' in analysis
        assert 'can_retry' in analysis
        assert 'remediation_suggestion' in analysis
    
    def test_error_categorization(self):
        """Test error categorization logic."""
        # Schema validation error
        event = {
            "original_event": {"user_id": "user-123"},
            "error_type": "ValidationError",
            "error_message": "Schema validation failed",
            "failed_at": "2023-01-01T12:00:00",
            "processing_stage": "producer_validation"
        }
        
        success = self.handler.process_dead_letter_event(event)
        assert success is True
        
        analysis = event['error_analysis']
        assert analysis['error_category'] == 'schema_validation_error'
        assert analysis['can_retry'] is False
    
    def test_network_error_categorization(self):
        """Test network error categorization."""
        event = {
            "original_event": {"user_id": "user-123"},
            "error_type": "ConnectionError",
            "error_message": "Connection timeout",
            "failed_at": "2023-01-01T12:00:00",
            "processing_stage": "consumer_validation"
        }
        
        success = self.handler.process_dead_letter_event(event)
        assert success is True
        
        analysis = event['error_analysis']
        assert analysis['error_category'] == 'network_error'
        assert analysis['can_retry'] is True
    
    def test_transformation_error_categorization(self):
        """Test transformation error categorization."""
        event = {
            "original_event": {"user_id": "user-123"},
            "error_type": "TransformationError",
            "error_message": "Failed to parse user agent",
            "failed_at": "2023-01-01T12:00:00",
            "processing_stage": "transformation"
        }
        
        success = self.handler.process_dead_letter_event(event)
        assert success is True
        
        analysis = event['error_analysis']
        assert analysis['error_category'] == 'transformation_error'
        assert analysis['can_retry'] is True
    
    def test_missing_field_error_categorization(self):
        """Test missing field error categorization."""
        event = {
            "original_event": {"user_id": "user-123"},
            "error_type": "ValidationError",
            "error_message": "Missing required field: event_type",
            "failed_at": "2023-01-01T12:00:00",
            "processing_stage": "producer_validation"
        }
        
        success = self.handler.process_dead_letter_event(event)
        assert success is True
        
        analysis = event['error_analysis']
        assert analysis['error_category'] == 'missing_required_field'
        assert analysis['can_retry'] is False
    
    def test_remediation_suggestions(self):
        """Test remediation suggestion logic."""
        event = {
            "original_event": {"user_id": "user-123"},
            "error_type": "ValidationError",
            "error_message": "Invalid enum value",
            "failed_at": "2023-01-01T12:00:00",
            "processing_stage": "producer_validation"
        }
        
        success = self.handler.process_dead_letter_event(event)
        assert success is True
        
        analysis = event['error_analysis']
        assert 'remediation_suggestion' in analysis
        assert len(analysis['remediation_suggestion']) > 0
    
    def test_get_error_statistics(self):
        """Test getting error statistics."""
        events = [
            {
                "original_event": {"user_id": "user-1"},
                "error_type": "ValidationError",
                "error_message": "Invalid event type",
                "failed_at": "2023-01-01T12:00:00",
                "processing_stage": "producer_validation"
            },
            {
                "original_event": {"user_id": "user-2"},
                "error_type": "ConnectionError",
                "error_message": "Timeout",
                "failed_at": "2023-01-01T12:00:00",
                "processing_stage": "consumer_validation"
            }
        ]
        
        for event in events:
            self.handler.process_dead_letter_event(event)
        
        stats = self.handler.get_error_statistics()
        assert stats['processed_dead_letter_events'] == 2
        assert stats['failed_dead_letter_events'] == 0
        assert stats['total_dead_letter_events'] == 2
    
    def test_close_handler(self):
        """Test closing the handler."""
        event = {
            "original_event": {"user_id": "user-123"},
            "error_type": "ValidationError",
            "error_message": "Test error",
            "failed_at": "2023-01-01T12:00:00",
            "processing_stage": "producer_validation"
        }
        
        self.handler.process_dead_letter_event(event)
        self.handler.close()  # Should not raise any exceptions


def test_process_dead_letter_event_function():
    """Test the convenience function."""
    event = {
        "original_event": {"user_id": "user-123"},
        "error_type": "ValidationError",
        "error_message": "Test error",
        "failed_at": "2023-01-01T12:00:00",
        "processing_stage": "producer_validation"
    }
    
    success = process_dead_letter_event(event)
    assert success is True
    assert 'error_analysis' in event 