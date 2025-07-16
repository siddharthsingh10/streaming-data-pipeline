"""
Tests for the event transformation module.
"""
import pytest
from src.transform import EventTransformer, normalize_event_type, get_event_category, add_processing_metadata


class TestEventTransformer:
    """Test EventTransformer functionality."""
    
    def test_transform_user_event(self):
        """Test basic event transformation."""
        transformer = EventTransformer()
        
        # Create a test event
        event = {
            "event_id": "test-123",
            "user_id": "user-456",
            "event_type": "purchase",
            "timestamp": "2023-01-01T12:00:00",
            "session_id": "session-789",
            "amount": 99.99,
            "product_id": "prod-001"
        }
        
        transformed = transformer.transform_user_event(event)
        
        # Check basic fields are preserved
        assert transformed["event_id"] == "test-123"
        assert transformed["user_id"] == "user-456"
        assert transformed["event_type"] == "purchase"
        assert transformed["timestamp"] == "2023-01-01T12:00:00"
        assert transformed["session_id"] == "session-789"
        
        # Check transformations
        assert transformed["normalized_event_type"] == "PURCHASE"
        assert transformed["event_category"] == "conversion"
        assert "processed_at" in transformed
        assert "processing_version" in transformed
    
    def test_transform_page_view_event(self):
        """Test transformation of page view event."""
        transformer = EventTransformer()
        
        event = {
            "event_id": "test-123",
            "user_id": "user-456",
            "event_type": "page_view",
            "timestamp": "2023-01-01T12:00:00",
            "session_id": "session-789",
            "page_url": "https://example.com"
        }
        
        transformed = transformer.transform_user_event(event)
        
        assert transformed["normalized_event_type"] == "PAGE_VIEW"
        assert transformed["event_category"] == "engagement"
    
    def test_transform_login_event(self):
        """Test transformation of login event."""
        transformer = EventTransformer()
        
        event = {
            "event_id": "test-123",
            "user_id": "user-456",
            "event_type": "login",
            "timestamp": "2023-01-01T12:00:00",
            "session_id": "session-789"
        }
        
        transformed = transformer.transform_user_event(event)
        
        assert transformed["normalized_event_type"] == "LOGIN"
        assert transformed["event_category"] == "authentication"
    
    def test_transform_unknown_event(self):
        """Test transformation of unknown event type."""
        transformer = EventTransformer()
        
        event = {
            "event_id": "test-123",
            "user_id": "user-456",
            "event_type": "unknown_event",
            "timestamp": "2023-01-01T12:00:00",
            "session_id": "session-789"
        }
        
        transformed = transformer.transform_user_event(event)
        
        assert transformed["normalized_event_type"] == "UNKNOWN_EVENT"
        assert transformed["event_category"] == "other"
    
    def test_transform_batch(self):
        """Test batch transformation."""
        transformer = EventTransformer()
        
        events = [
            {
                "event_id": "test-1",
                "user_id": "user-1",
                "event_type": "page_view",
                "timestamp": "2023-01-01T12:00:00",
                "session_id": "session-1"
            },
            {
                "event_id": "test-2",
                "user_id": "user-2",
                "event_type": "purchase",
                "timestamp": "2023-01-01T12:00:00",
                "session_id": "session-2",
                "amount": 50.0
            }
        ]
        
        transformed = transformer.transform_batch(events)
        
        assert len(transformed) == 2
        assert transformed[0]["normalized_event_type"] == "PAGE_VIEW"
        assert transformed[1]["normalized_event_type"] == "PURCHASE"
        assert transformed[0]["event_category"] == "engagement"
        assert transformed[1]["event_category"] == "conversion"


class TestStatelessFunctions:
    """Test stateless transformation functions."""
    
    def test_normalize_event_type(self):
        """Test event type normalization."""
        assert normalize_event_type("page_view") == "PAGE_VIEW"
        assert normalize_event_type("purchase") == "PURCHASE"
        assert normalize_event_type("signup") == "SIGNUP"
        assert normalize_event_type("click") == "CLICK"
        assert normalize_event_type("unknown") == "UNKNOWN"
        assert normalize_event_type("") == "UNKNOWN"
        assert normalize_event_type(None) == "UNKNOWN"
    
    def test_get_event_category(self):
        """Test event categorization."""
        assert get_event_category("page_view") == "engagement"
        assert get_event_category("purchase") == "conversion"
        assert get_event_category("signup") == "conversion"
        assert get_event_category("click") == "engagement"
        assert get_event_category("login") == "authentication"
        assert get_event_category("logout") == "authentication"
        assert get_event_category("unknown") == "other"
    
    def test_add_processing_metadata(self):
        """Test processing metadata addition."""
        event = {
            "user_id": "user123",
            "event_type": "page_view"
        }
        
        result = add_processing_metadata(event)
        
        assert "processed_at" in result
        assert "processing_version" in result
        assert result["processing_version"] == "1.0"
        assert result["user_id"] == "user123"  # Original fields preserved 