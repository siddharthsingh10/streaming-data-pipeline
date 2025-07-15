"""
Schema validator using JSON Schema for language-agnostic validation.
"""
import yaml
import json
from typing import Dict, Any, Optional, List
from jsonschema import validate, ValidationError
from datetime import datetime
import uuid


class SchemaValidator:
    """
    Validator for event schemas using JSON Schema.
    
    This provides language-agnostic schema validation that can be used
    across different components and languages.
    """
    
    def __init__(self, schema_file: str = "schema/event_schema.yaml"):
        """Initialize validator with schema file."""
        self.schema_file = schema_file
        self.schemas = self._load_schemas()
        self.mappings = self._load_mappings()
    
    def _load_schemas(self) -> Dict[str, Any]:
        """Load schemas from YAML file."""
        try:
            with open(self.schema_file, 'r') as file:
                data = yaml.safe_load(file)
                return data.get('schemas', {})
        except Exception as e:
            raise ValueError(f"Failed to load schema file: {e}")
    
    def _load_mappings(self) -> Dict[str, Any]:
        """Load event type mappings from YAML file."""
        try:
            with open(self.schema_file, 'r') as file:
                data = yaml.safe_load(file)
                return data.get('mappings', {})
        except Exception as e:
            raise ValueError(f"Failed to load mappings: {e}")
    
    def validate_user_event(self, event_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate a user event against the schema.
        
        Args:
            event_data: Event data to validate
            
        Returns:
            Validated event data with defaults applied
            
        Raises:
            ValidationError: If validation fails
        """
        schema = self.schemas['user_event']
        
        # Apply defaults
        validated_data = self._apply_defaults(event_data, schema)
        
        # Validate against schema
        validate(instance=validated_data, schema=schema)
        
        return validated_data
    
    def validate_transformed_event(self, event_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate a transformed event against the schema.
        
        Args:
            event_data: Transformed event data to validate
            
        Returns:
            Validated event data with defaults applied
            
        Raises:
            ValidationError: If validation fails
        """
        schema = self.schemas['transformed_event']
        
        # Apply defaults
        validated_data = self._apply_defaults(event_data, schema)
        
        # Validate against schema
        validate(instance=validated_data, schema=schema)
        
        return validated_data
    
    def validate_dead_letter_event(self, event_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate a dead letter event against the schema.
        
        Args:
            event_data: Dead letter event data to validate
            
        Returns:
            Validated event data with defaults applied
            
        Raises:
            ValidationError: If validation fails
        """
        schema = self.schemas['dead_letter_event']
        
        # Apply defaults
        validated_data = self._apply_defaults(event_data, schema)
        
        # Validate against schema
        validate(instance=validated_data, schema=schema)
        
        return validated_data
    
    def _apply_defaults(self, data: Dict[str, Any], schema: Dict[str, Any]) -> Dict[str, Any]:
        """Apply schema defaults to data."""
        result = data.copy()
        
        # Apply defaults from schema
        if 'properties' in schema:
            for field_name, field_schema in schema['properties'].items():
                if field_name not in result and 'default' in field_schema:
                    result[field_name] = field_schema['default']
        
        # Apply common defaults
        if 'event_id' not in result:
            result['event_id'] = str(uuid.uuid4())
        
        if 'timestamp' not in result:
            result['timestamp'] = datetime.now().isoformat()
        
        return result
    
    def get_event_type_mapping(self, event_type: str) -> str:
        """Get normalized event type from mapping."""
        mappings = self.mappings.get('event_type_mapping', {})
        return mappings.get(event_type, 'unknown')
    
    def get_event_category(self, event_type: str) -> str:
        """Get event category from mapping."""
        categories = self.mappings.get('event_categories', {})
        return categories.get(event_type, 'other')
    
    def is_conversion_event(self, event_type: str) -> bool:
        """Check if event type is a conversion event."""
        conversion_events = set(self.mappings.get('conversion_events', []))
        return event_type in conversion_events
    
    def get_schema_errors(self, data: Dict[str, Any], schema_name: str) -> List[str]:
        """
        Get detailed validation errors.
        
        Args:
            data: Data to validate
            schema_name: Name of schema to validate against
            
        Returns:
            List of validation error messages
        """
        if schema_name not in self.schemas:
            return [f"Unknown schema: {schema_name}"]
        
        schema = self.schemas[schema_name]
        errors = []
        
        try:
            validate(instance=data, schema=schema)
        except ValidationError as e:
            errors.append(str(e))
        
        return errors


# Convenience functions for common validations
def validate_user_event(data: Dict[str, Any]) -> Dict[str, Any]:
    """Validate user event data."""
    validator = SchemaValidator()
    return validator.validate_user_event(data)


def validate_transformed_event(data: Dict[str, Any]) -> Dict[str, Any]:
    """Validate transformed event data."""
    validator = SchemaValidator()
    return validator.validate_transformed_event(data)


def validate_dead_letter_event(data: Dict[str, Any]) -> Dict[str, Any]:
    """Validate dead letter event data."""
    validator = SchemaValidator()
    return validator.validate_dead_letter_event(data) 