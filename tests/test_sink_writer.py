"""
Tests for the Parquet sink writer.
"""
import pytest
import os
import tempfile
import shutil
from datetime import datetime
from src.sink_writer import ParquetSinkWriter, DeadLetterSinkWriter


class TestParquetSinkWriter:
    """Test ParquetSinkWriter functionality."""
    
    def setup_method(self):
        """Set up test environment."""
        self.temp_dir = tempfile.mkdtemp()
        self.writer = ParquetSinkWriter(output_dir=self.temp_dir, batch_size=3)
    
    def teardown_method(self):
        """Clean up test environment."""
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_add_event(self):
        """Test adding a single event to the writer."""
        event = {
            "event_id": "test-123",
            "user_id": "user-456",
            "event_type": "page_view",
            "timestamp": "2023-01-01T12:00:00",
            "normalized_event_type": "view",
            "event_category": "navigation",
            "is_conversion": False
        }
        
        success = self.writer.add_event(event)
        assert success is True
        assert len(self.writer.current_batch) == 1
    
    def test_add_events(self):
        """Test adding multiple events."""
        events = [
            {
                "event_id": "test-1",
                "user_id": "user-1",
                "event_type": "page_view",
                "timestamp": "2023-01-01T12:00:00",
                "normalized_event_type": "view",
                "event_category": "navigation",
                "is_conversion": False
            },
            {
                "event_id": "test-2",
                "user_id": "user-2",
                "event_type": "purchase",
                "timestamp": "2023-01-01T12:00:00",
                "normalized_event_type": "conversion",
                "event_category": "commerce",
                "is_conversion": True,
                "revenue": 99.99
            }
        ]
        
        added_count = self.writer.add_events(events)
        assert added_count == 2
        assert len(self.writer.current_batch) == 2
    
    def test_flush_batch(self):
        """Test flushing a batch to Parquet file."""
        events = [
            {
                "event_id": "test-1",
                "user_id": "user-1",
                "event_type": "page_view",
                "timestamp": "2023-01-01T12:00:00",
                "normalized_event_type": "view",
                "event_category": "navigation",
                "is_conversion": False
            },
            {
                "event_id": "test-2",
                "user_id": "user-2",
                "event_type": "purchase",
                "timestamp": "2023-01-01T12:00:00",
                "normalized_event_type": "conversion",
                "event_category": "commerce",
                "is_conversion": True,
                "revenue": 99.99
            }
        ]
        
        # Add events to batch
        self.writer.add_events(events)
        
        # Flush batch
        success = self.writer.flush_batch()
        assert success is True
        
        # Check that files were created
        files = os.listdir(self.temp_dir)
        assert len(files) == 1
        assert files[0].endswith('.parquet')
        
        # Check stats
        stats = self.writer.get_stats()
        assert stats['total_events_written'] == 2
        assert stats['files_written'] == 1
    
    def test_auto_flush_on_batch_size(self):
        """Test automatic flushing when batch size is reached."""
        events = [
            {
                "event_id": f"test-{i}",
                "user_id": f"user-{i}",
                "event_type": "page_view",
                "timestamp": "2023-01-01T12:00:00",
                "normalized_event_type": "view",
                "event_category": "navigation",
                "is_conversion": False
            }
            for i in range(3)  # Batch size is 3
        ]
        
        # Add events (should trigger auto-flush)
        for event in events:
            self.writer.add_event(event)
        
        # Check that batch was flushed
        assert len(self.writer.current_batch) == 0
        
        # Check stats
        stats = self.writer.get_stats()
        assert stats['total_events_written'] == 3
        assert stats['files_written'] == 1
    
    def test_close_with_remaining_events(self):
        """Test closing writer with remaining events in batch."""
        event = {
            "event_id": "test-123",
            "user_id": "user-456",
            "event_type": "page_view",
            "timestamp": "2023-01-01T12:00:00",
            "normalized_event_type": "view",
            "event_category": "navigation",
            "is_conversion": False
        }
        
        self.writer.add_event(event)
        
        # Close writer (should flush remaining events)
        success = self.writer.close()
        assert success is True
        
        # Check that file was created
        files = os.listdir(self.temp_dir)
        assert len(files) == 1
        assert files[0].endswith('.parquet')
    
    def test_empty_batch_flush(self):
        """Test flushing an empty batch."""
        success = self.writer.flush_batch()
        assert success is True
        assert self.writer.file_count == 0
    
    def test_type_inference(self):
        """Test PyArrow type inference."""
        events = [
            {
                "event_id": "test-1",
                "user_id": "user-1",
                "event_type": "page_view",
                "timestamp": "2023-01-01T12:00:00",
                "normalized_event_type": "view",
                "event_category": "navigation",
                "is_conversion": False,
                "revenue": 99.99,  # float
                "session_count": 5,  # int
                "is_active": True,  # bool
                "country_code": "US"  # string
            }
        ]
        
        self.writer.add_events(events)
        success = self.writer.flush_batch()
        assert success is True


class TestDeadLetterSinkWriter:
    """Test DeadLetterSinkWriter functionality."""
    
    def setup_method(self):
        """Set up test environment."""
        self.temp_dir = tempfile.mkdtemp()
        self.writer = DeadLetterSinkWriter(output_dir=self.temp_dir)
    
    def teardown_method(self):
        """Clean up test environment."""
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_write_dead_letter_event(self):
        """Test writing a dead letter event."""
        event = {
            "original_event": {
                "user_id": "user-123",
                "event_type": "invalid_event"
            },
            "error_type": "ValidationError",
            "error_message": "Invalid event type",
            "failed_at": "2023-01-01T12:00:00",
            "processing_stage": "producer_validation"
        }
        
        success = self.writer.write_dead_letter_event(event)
        assert success is True
        
        # Check that file was created
        files = os.listdir(self.temp_dir)
        assert len(files) == 1
        assert files[0].endswith('.json')
        assert files[0].startswith('dead_letter_')
    
    def test_write_multiple_dead_letter_events(self):
        """Test writing multiple dead letter events."""
        events = [
            {
                "original_event": {"user_id": "user-1", "event_type": "invalid"},
                "error_type": "ValidationError",
                "error_message": "Invalid event type",
                "failed_at": "2023-01-01T12:00:00",
                "processing_stage": "producer_validation"
            },
            {
                "original_event": {"user_id": "user-2", "amount": -10.0},
                "error_type": "ValidationError",
                "error_message": "Negative amount",
                "failed_at": "2023-01-01T12:00:00",
                "processing_stage": "consumer_validation"
            }
        ]
        
        for event in events:
            success = self.writer.write_dead_letter_event(event)
            assert success is True
        
        # Check that files were created
        files = os.listdir(self.temp_dir)
        assert len(files) == 2
        assert all(f.endswith('.json') for f in files)
        assert all(f.startswith('dead_letter_') for f in files)
    
    def test_get_stats(self):
        """Test getting statistics."""
        event = {
            "original_event": {"user_id": "user-123"},
            "error_type": "ValidationError",
            "error_message": "Test error",
            "failed_at": "2023-01-01T12:00:00",
            "processing_stage": "producer_validation"
        }
        
        self.writer.write_dead_letter_event(event)
        
        stats = self.writer.get_stats()
        assert stats['dead_letter_events_written'] == 1
        assert stats['output_directory'] == self.temp_dir 