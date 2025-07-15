"""
Tests for JSON Schema-based validation.
"""
import pytest
from datetime import datetime
from schema.schema_validator import SchemaValidator, validate_user_event, validate_transformed_event


class TestSchemaValidator:
    """Test JSON Schema-based validation."""
    
    def test_valid_user_event(self):
        """Test validating a valid user event."""
        event_data = {
            "user_id": "user123",
            "event_type": "page_view",
            "page_url": "https://example.com",
            "session_id": "session456"
        }
        
        validated = validate_user_event(event_data)
        
        assert validated["user_id"] == "user123"
        assert validated["event_type"] == "page_view"
        assert "event_id" in validated  # Should be auto-generated
        assert "timestamp" in validated  # Should be auto-generated
        assert validated["source"] == "web"  # Default value
        assert validated["version"] == "1.0"  # Default value
    
    def test_invalid_event_type(self):
        """Test that invalid event types are rejected."""
        event_data = {
            "user_id": "user123",
            "event_type": "invalid_event"
        }
        
        with pytest.raises(Exception):  # jsonschema.ValidationError
            validate_user_event(event_data)
    
    def test_negative_amount(self):
        """Test that negative amounts are rejected."""
        event_data = {
            "user_id": "user123",
            "event_type": "purchase",
            "amount": -10.0
        }
        
        with pytest.raises(Exception):  # jsonschema.ValidationError
            validate_user_event(event_data)
    
    def test_valid_transformed_event(self):
        """Test validating a valid transformed event."""
        event_data = {
            "event_id": "event123",
            "user_id": "user123",
            "event_type": "purchase",
            "timestamp": datetime.now().isoformat(),
            "session_id": "session123",
            "normalized_event_type": "conversion",
            "event_category": "commerce",
            "is_conversion": True,
            "revenue": 99.99
        }
        
        validated = validate_transformed_event(event_data)
        
        assert validated["is_conversion"] is True
        assert validated["revenue"] == 99.99
        assert validated["event_category"] == "commerce"
        assert validated["processing_version"] == "1.0"  # Default value


class TestSchemaValidatorMappings:
    """Test event type mappings and transformations."""
    
    def test_event_type_mapping(self):
        """Test event type mapping functionality."""
        validator = SchemaValidator()
        
        assert validator.get_event_type_mapping("page_view") == "view"
        assert validator.get_event_type_mapping("purchase") == "conversion"
        assert validator.get_event_type_mapping("unknown") == "unknown"
    
    def test_event_categories(self):
        """Test event category mapping."""
        validator = SchemaValidator()
        
        assert validator.get_event_category("purchase") == "commerce"
        assert validator.get_event_category("page_view") == "navigation"
        assert validator.get_event_category("unknown") == "other"
    
    def test_conversion_events(self):
        """Test conversion event detection."""
        validator = SchemaValidator()
        
        assert validator.is_conversion_event("purchase") is True
        assert validator.is_conversion_event("signup") is True
        assert validator.is_conversion_event("page_view") is False
        assert validator.is_conversion_event("click") is False


class TestSchemaValidatorErrors:
    """Test schema validation error handling."""
    
    def test_schema_errors(self):
        """Test getting detailed validation errors."""
        validator = SchemaValidator()
        
        invalid_data = {
            "user_id": "user123",
            "event_type": "invalid_event"
        }
        
        errors = validator.get_schema_errors(invalid_data, "user_event")
        assert len(errors) > 0
        assert "invalid_event" in str(errors[0])  # Should mention invalid enum value
    
    def test_unknown_schema(self):
        """Test handling of unknown schema names."""
        validator = SchemaValidator()
        
        errors = validator.get_schema_errors({}, "unknown_schema")
        assert errors == ["Unknown schema: unknown_schema"]


class TestSchemaValidatorDefaults:
    """Test default value application."""
    
    def test_auto_generated_fields(self):
        """Test that required fields are auto-generated."""
        event_data = {
            "user_id": "user123",
            "event_type": "page_view"
        }
        
        validated = validate_user_event(event_data)
        
        # Should have auto-generated fields
        assert "event_id" in validated
        assert "timestamp" in validated
        
        # Should have default values
        assert validated["source"] == "web"
        assert validated["version"] == "1.0"
    
    def test_existing_fields_not_overwritten(self):
        """Test that existing fields are not overwritten by defaults."""
        event_data = {
            "user_id": "user123",
            "event_type": "page_view",
            "source": "mobile",
            "version": "2.0"
        }
        
        validated = validate_user_event(event_data)
        
        # Should preserve existing values
        assert validated["source"] == "mobile"
        assert validated["version"] == "2.0" 