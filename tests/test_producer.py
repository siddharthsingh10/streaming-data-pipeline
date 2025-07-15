"""
Tests for the event producer service.
"""
import pytest
from unittest.mock import Mock, patch
from src.producer import EventProducer


class TestEventProducer:
    """Test EventProducer functionality."""
    
    def test_generate_user_event(self):
        """Test that user events are generated with required fields."""
        producer = EventProducer()
        event = producer.generate_user_event()
        
        # Check required fields
        assert "user_id" in event
        assert "event_type" in event
        assert "timestamp" in event
        assert "session_id" in event
        assert "source" in event
        assert "version" in event
        
        # Check event type is valid
        valid_types = ["page_view", "click", "purchase", "signup", "login", "logout"]
        assert event["event_type"] in valid_types
        
        # Check source and version defaults
        assert event["source"] == "web"
        assert event["version"] == "1.0"
    
    def test_generate_invalid_event(self):
        """Test that invalid events are generated for testing."""
        producer = EventProducer()
        event = producer.generate_invalid_event()
        
        # Should have invalid event type
        assert event["event_type"] == "invalid_event_type"
        
        # Other fields should be valid
        assert "user_id" in event
        assert "timestamp" in event
    
    def test_validate_event_valid(self):
        """Test validation of valid events."""
        producer = EventProducer()
        event = producer.generate_user_event()
        
        is_valid, error = producer.validate_event(event)
        assert is_valid is True
        assert error is None
    
    def test_validate_event_invalid(self):
        """Test validation of invalid events."""
        producer = EventProducer()
        event = producer.generate_invalid_event()
        
        is_valid, error = producer.validate_event(event)
        assert is_valid is False
        assert error is not None
        assert "invalid_event_type" in error
    
    @patch('src.producer.Producer')
    def test_publish_event_success(self, mock_producer_class):
        """Test successful event publishing."""
        mock_producer = Mock()
        mock_producer_class.return_value = mock_producer
        
        producer = EventProducer()
        event = {"user_id": "test", "event_type": "page_view"}
        
        success = producer.publish_event(event, "test-topic")
        assert success is True
        
        # Check that produce was called
        mock_producer.produce.assert_called_once()
        mock_producer.poll.assert_called_once()
    
    @patch('src.producer.Producer')
    def test_publish_event_failure(self, mock_producer_class):
        """Test event publishing failure."""
        mock_producer = Mock()
        mock_producer.produce.side_effect = Exception("Kafka error")
        mock_producer_class.return_value = mock_producer
        
        producer = EventProducer()
        event = {"user_id": "test", "event_type": "page_view"}
        
        success = producer.publish_event(event, "test-topic")
        assert success is False
    
    def test_process_event_valid(self):
        """Test processing of valid events."""
        with patch('src.producer.Producer') as mock_producer_class:
            mock_producer = Mock()
            mock_producer_class.return_value = mock_producer
            
            producer = EventProducer()
            event = producer.generate_user_event()
            
            success = producer.process_event(event)
            assert success is True
            assert producer.event_count == 1
            assert producer.error_count == 0
    
    def test_process_event_invalid(self):
        """Test processing of invalid events."""
        with patch('src.producer.Producer') as mock_producer_class:
            mock_producer = Mock()
            mock_producer_class.return_value = mock_producer
            
            producer = EventProducer()
            event = producer.generate_invalid_event()
            
            success = producer.process_event(event)
            assert success is False
            assert producer.event_count == 0
            assert producer.error_count == 1


class TestEventProducerIntegration:
    """Integration tests for event producer."""
    
    def test_event_generation_variety(self):
        """Test that different event types are generated."""
        producer = EventProducer()
        events = []
        
        # Generate multiple events
        for _ in range(10):
            event = producer.generate_user_event()
            events.append(event["event_type"])
        
        # Should have variety of event types
        unique_types = set(events)
        assert len(unique_types) > 1  # Should have multiple event types
        
        # All should be valid
        valid_types = {"page_view", "click", "purchase", "signup", "login", "logout"}
        assert all(event_type in valid_types for event_type in unique_types)
    
    def test_purchase_event_amount(self):
        """Test that purchase events have valid amounts."""
        producer = EventProducer()
        
        # Generate multiple purchase events
        for _ in range(5):
            event = producer.generate_user_event()
            if event["event_type"] == "purchase":
                assert "amount" in event
                assert "product_id" in event
                assert event["amount"] >= 10.0
                assert event["amount"] <= 500.0 