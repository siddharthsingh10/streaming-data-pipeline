"""
Event transformation module with simple transformations for the streaming pipeline.
"""
import logging
from typing import Dict, Any, Optional
from datetime import datetime

from src.utils import setup_logging

logger = setup_logging(__name__)


class EventTransformer:
    """
    Simple event transformer that applies basic transformations.
    
    Features:
    - Event type normalization
    - Processing metadata addition
    - Simple data enrichment
    """
    
    def __init__(self):
        """Initialize the event transformer."""
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
            # Start with original event
            transformed = event.copy()
            
            # Apply simple transformations
            transformed.update(self._normalize_event_type(event))
            transformed.update(self._add_processing_metadata())
            
            logger.debug(f"Transformed event: {event.get('event_type', 'unknown')}")
            return transformed
            
        except Exception as e:
            logger.error(f"Failed to transform event: {e}")
            raise
    
    def _normalize_event_type(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize event type to uppercase."""
        event_type = event.get("event_type", "unknown")
        
        return {
            "normalized_event_type": event_type.upper(),
            "event_category": self._get_event_category(event_type)
        }
    
    def _get_event_category(self, event_type: str) -> str:
        """Simple event categorization."""
        if event_type in ["purchase", "signup"]:
            return "conversion"
        elif event_type in ["page_view", "click"]:
            return "engagement"
        elif event_type in ["login", "logout"]:
            return "authentication"
        else:
            return "other"
    
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


# Simple convenience functions
def normalize_event_type(event_type: str) -> str:
    """Normalize event type to uppercase."""
    return event_type.upper() if event_type else "UNKNOWN"


def get_event_category(event_type: str) -> str:
    """Get event category."""
    if event_type in ["purchase", "signup"]:
        return "conversion"
    elif event_type in ["page_view", "click"]:
        return "engagement"
    elif event_type in ["login", "logout"]:
        return "authentication"
    else:
        return "other"


def add_processing_metadata(event: Dict[str, Any]) -> Dict[str, Any]:
    """Add processing metadata to event."""
    event["processed_at"] = datetime.now().isoformat()
    event["processing_version"] = "1.0"
    return event 