"""
Tests for the event transformation module.
"""
import pytest
from src.transform import EventTransformer, normalize_event_type, categorize_event, is_conversion_event


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
            "product_id": "prod-001",
            "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "country": "United States"
        }
        
        transformed = transformer.transform_user_event(event)
        
        # Check basic fields are preserved
        assert transformed["event_id"] == "test-123"
        assert transformed["user_id"] == "user-456"
        assert transformed["event_type"] == "purchase"
        assert transformed["timestamp"] == "2023-01-01T12:00:00"
        assert transformed["session_id"] == "session-789"
        
        # Check transformations
        assert transformed["normalized_event_type"] == "conversion"
        assert transformed["event_category"] == "commerce"
        assert transformed["is_conversion"] is True
        assert transformed["revenue"] == 99.99
        assert transformed["conversion_value"] == 99.99
        
        # Check enriched fields
        assert "user_agent_parsed" in transformed
        assert "country_code" in transformed
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
            "page_url": "https://example.com",
            "user_agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 14_0 like Mac OS X)",
            "country": "Canada"
        }
        
        transformed = transformer.transform_user_event(event)
        
        assert transformed["normalized_event_type"] == "view"
        assert transformed["event_category"] == "navigation"
        assert transformed["is_conversion"] is False
        assert transformed["revenue"] == 0.0
        assert transformed["conversion_value"] == 0.0
    
    def test_transform_without_user_agent(self):
        """Test transformation when user agent is missing."""
        transformer = EventTransformer()
        
        event = {
            "event_id": "test-123",
            "user_id": "user-456",
            "event_type": "click",
            "timestamp": "2023-01-01T12:00:00",
            "session_id": "session-789"
        }
        
        transformed = transformer.transform_user_event(event)
        
        # Check that user_agent_parsed has default values when user_agent is missing
        assert transformed["user_agent_parsed"] is not None
        assert transformed["user_agent_parsed"]["browser"] == "unknown"
        assert transformed["user_agent_parsed"]["os"] == "unknown"
        assert transformed["user_agent_parsed"]["device"] == "unknown"
        assert transformed["normalized_event_type"] == "interaction"
        assert transformed["event_category"] == "interaction"
    
    def test_transform_without_country(self):
        """Test transformation when country is missing."""
        transformer = EventTransformer()
        
        event = {
            "event_id": "test-123",
            "user_id": "user-456",
            "event_type": "signup",
            "timestamp": "2023-01-01T12:00:00",
            "session_id": "session-789"
        }
        
        transformed = transformer.transform_user_event(event)
        
        assert transformed["country_code"] is None
        assert transformed["normalized_event_type"] == "conversion"
        assert transformed["event_category"] == "user_management"
        assert transformed["is_conversion"] is True
    
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
        assert transformed[0]["normalized_event_type"] == "view"
        assert transformed[1]["normalized_event_type"] == "conversion"
        assert transformed[1]["revenue"] == 50.0


class TestStatelessFunctions:
    """Test stateless transformation functions."""
    
    def test_normalize_event_type(self):
        """Test event type normalization."""
        assert normalize_event_type("page_view") == "view"
        assert normalize_event_type("purchase") == "conversion"
        assert normalize_event_type("signup") == "conversion"
        assert normalize_event_type("click") == "interaction"
        assert normalize_event_type("unknown") == "unknown"
    
    def test_categorize_event(self):
        """Test event categorization."""
        assert categorize_event("page_view") == "navigation"
        assert categorize_event("purchase") == "commerce"
        assert categorize_event("signup") == "user_management"
        assert categorize_event("click") == "interaction"
        assert categorize_event("unknown") == "other"
    
    def test_is_conversion_event(self):
        """Test conversion event detection."""
        assert is_conversion_event("purchase") is True
        assert is_conversion_event("signup") is True
        assert is_conversion_event("page_view") is False
        assert is_conversion_event("click") is False
        assert is_conversion_event("login") is False


class TestUserAgentParsing:
    """Test user agent parsing functionality."""
    
    def test_parse_user_agent_desktop(self):
        """Test parsing desktop user agent."""
        from src.transform import parse_user_agent
        
        user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        parsed = parse_user_agent(user_agent)
        
        assert parsed is not None
        assert "browser" in parsed
        assert "os" in parsed
        assert "device" in parsed
        assert "is_mobile" in parsed
        assert "is_tablet" in parsed
        assert "is_pc" in parsed
    
    def test_parse_user_agent_mobile(self):
        """Test parsing mobile user agent."""
        from src.transform import parse_user_agent
        
        user_agent = "Mozilla/5.0 (iPhone; CPU iPhone OS 14_0 like Mac OS X)"
        parsed = parse_user_agent(user_agent)
        
        assert parsed is not None
        assert parsed["is_mobile"] is True
        assert parsed["is_pc"] is False
    
    def test_parse_user_agent_none(self):
        """Test parsing when user agent is None."""
        from src.transform import parse_user_agent
        
        parsed = parse_user_agent(None)
        assert parsed is None
    
    def test_parse_user_agent_empty(self):
        """Test parsing when user agent is empty."""
        from src.transform import parse_user_agent
        
        parsed = parse_user_agent("")
        assert parsed is None


class TestBusinessMetrics:
    """Test business metrics calculation."""
    
    def test_calculate_revenue(self):
        """Test revenue calculation."""
        from src.transform import calculate_revenue
        
        assert calculate_revenue("purchase", 100.0) == 100.0
        assert calculate_revenue("page_view", 100.0) == 0.0
        assert calculate_revenue("click", 50.0) == 0.0
        assert calculate_revenue("signup", 0.0) == 0.0 