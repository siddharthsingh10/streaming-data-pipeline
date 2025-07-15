"""
Event transformation module with stateless functions for data processing.
"""
import json
import logging
from typing import Dict, Any, Optional
from datetime import datetime
from user_agents import parse

from schema.schema_validator import SchemaValidator
from src.utils import setup_logging

logger = setup_logging(__name__)


class EventTransformer:
    """
    Event transformer that applies business logic and data enrichment.
    
    Features:
    - Event type normalization
    - User agent parsing
    - Business metrics calculation
    - Data enrichment and cleaning
    """
    
    def __init__(self):
        """Initialize the event transformer."""
        self.validator = SchemaValidator()
        logger.info("Event transformer initialized")
    
    def transform_user_event(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform a user event into a processed event.
        
        Args:
            event: Raw user event data
            
        Returns:
            Transformed event data
        """
        try:
            # Extract basic fields
            transformed = {
                "event_id": event.get("event_id"),
                "user_id": event.get("user_id"),
                "event_type": event.get("event_type"),
                "timestamp": event.get("timestamp"),
                "session_id": event.get("session_id")
            }
            
            # Apply minimal transformations
            transformed.update(self._normalize_event_type(event))
            transformed.update(self._parse_user_agent(event))
            transformed.update(self._add_processing_metadata())
            
            # Validate transformed event
            validated = self.validator.validate_transformed_event(transformed)
            
            logger.debug(f"Transformed event: {event.get('event_id', 'unknown')}")
            return validated
            
        except Exception as e:
            logger.error(f"Failed to transform event: {e}")
            raise
    
    def _normalize_event_type(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize event type and categorize."""
        event_type = event.get("event_type", "unknown")
        
        return {
            "normalized_event_type": self.validator.get_event_type_mapping(event_type),
            "event_category": self.validator.get_event_category(event_type),
            "is_conversion": self.validator.is_conversion_event(event_type)
        }
    
    def _parse_user_agent(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Parse user agent string for device/browser information."""
        user_agent_str = event.get("user_agent")
        default_ua = {
            "browser": "unknown",
            "browser_version": "unknown",
            "os": "unknown",
            "os_version": "unknown",
            "device": "unknown",
            "is_mobile": False,
            "is_tablet": False,
            "is_pc": False
        }
        if not user_agent_str:
            return {"user_agent_parsed": default_ua}
        try:
            ua = parse(user_agent_str)
            return {
                "user_agent_parsed": {
                    "browser": ua.browser.family or "unknown",
                    "browser_version": ua.browser.version_string or "unknown",
                    "os": ua.os.family or "unknown",
                    "os_version": ua.os.version_string or "unknown",
                    "device": ua.device.family or "unknown",
                    "is_mobile": ua.is_mobile,
                    "is_tablet": ua.is_tablet,
                    "is_pc": ua.is_pc
                }
            }
        except Exception as e:
            logger.warning(f"Failed to parse user agent: {e}")
            return {"user_agent_parsed": default_ua}
    
    def _calculate_business_metrics(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate business metrics from event data."""
        event_type = event.get("event_type")
        amount = event.get("amount", 0.0)
        
        # Revenue calculation
        revenue = amount if event_type == "purchase" else 0.0
        
        # Conversion value (could be different from revenue)
        conversion_value = revenue if self.validator.is_conversion_event(event_type) else 0.0
        
        return {
            "revenue": revenue,
            "conversion_value": conversion_value
        }
    
    def _enrich_location_data(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Enrich location data with country code."""
        country = event.get("country")
        
        if not country:
            return {"country_code": None}
        
        # Simple country to code mapping (in production, use a proper library)
        country_mapping = {
            "United States": "US",
            "Canada": "CA",
            "United Kingdom": "GB",
            "Germany": "DE",
            "France": "FR",
            "Japan": "JP",
            "China": "CN",
            "India": "IN"
        }
        
        country_code = country_mapping.get(country, country[:2].upper() if len(country) >= 2 else None)
        
        return {"country_code": country_code}
    
    def _add_processing_metadata(self) -> Dict[str, Any]:
        """Add processing metadata to transformed event."""
        return {
            "processed_at": datetime.now().isoformat(),
            "processing_version": "1.0"
        }
    
    def transform_batch(self, events: list[Dict[str, Any]]) -> list[Dict[str, Any]]:
        """
        Transform a batch of events.
        
        Args:
            events: List of raw events
            
        Returns:
            List of transformed events
        """
        transformed_events = []
        
        for event in events:
            try:
                transformed = self.transform_user_event(event)
                transformed_events.append(transformed)
            except Exception as e:
                logger.error(f"Failed to transform event in batch: {e}")
                # Continue processing other events
                continue
        
        logger.info(f"Transformed {len(transformed_events)} out of {len(events)} events")
        return transformed_events


# Convenience functions for stateless transformations
def normalize_event_type(event_type: str) -> str:
    """Normalize event type using schema mappings."""
    validator = SchemaValidator()
    return validator.get_event_type_mapping(event_type)


def categorize_event(event_type: str) -> str:
    """Categorize event type."""
    validator = SchemaValidator()
    return validator.get_event_category(event_type)


def is_conversion_event(event_type: str) -> bool:
    """Check if event is a conversion event."""
    validator = SchemaValidator()
    return validator.is_conversion_event(event_type)


def parse_user_agent(user_agent_str: str) -> Optional[Dict[str, Any]]:
    """Parse user agent string."""
    if not user_agent_str:
        return None
    
    try:
        ua = parse(user_agent_str)
        return {
            "browser": ua.browser.family,
            "browser_version": ua.browser.version_string,
            "os": ua.os.family,
            "os_version": ua.os.version_string,
            "device": ua.device.family,
            "is_mobile": ua.is_mobile,
            "is_tablet": ua.is_tablet,
            "is_pc": ua.is_pc
        }
    except Exception:
        return None


def calculate_revenue(event_type: str, amount: float = 0.0) -> float:
    """Calculate revenue from event."""
    if event_type == "purchase":
        return amount
    return 0.0


def enrich_event_data(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Apply all transformations to an event.
    
    Args:
        event: Raw event data
        
    Returns:
        Enriched event data
    """
    transformer = EventTransformer()
    return transformer.transform_user_event(event) 