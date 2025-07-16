"""
Dead letter handler for processing failed events.
"""
import json
import logging
from typing import Dict, Any, Optional
from datetime import datetime

from src.utils import setup_logging, format_error_message
from src.sink_writer import DeadLetterSinkWriter

logger = setup_logging(__name__)


class DeadLetterHandler:
    """
    Handler for dead letter events (failed events).
    
    Features:
    - Validates dead letter event schema
    - Writes to JSON files for debugging
    - Provides error analysis and categorization
    """
    
    def __init__(self):
        """Initialize the dead letter handler."""
        self.sink_writer = DeadLetterSinkWriter()
        self.processed_count = 0
        self.error_count = 0
        
        logger.info("Dead letter handler initialized")
    
    def process_dead_letter_event(self, event: Dict[str, Any]) -> bool:
        """
        Process a dead letter event.
        
        Args:
            event: Dead letter event to process
            
        Returns:
            bool: True if processed successfully
        """
        try:
            # Analyze the error
            error_analysis = self._analyze_error(event)
            
            # Add analysis to event
            event['error_analysis'] = error_analysis
            
            # Write to sink
            success = self.sink_writer.write_dead_letter_event(event)
            
            if success:
                self.processed_count += 1
                logger.warning(f"Processed dead letter event: {error_analysis['error_category']}")
            else:
                self.error_count += 1
                logger.error("Failed to write dead letter event to sink")
            
            return success
            
        except Exception as e:
            self.error_count += 1
            logger.error(f"Failed to process dead letter event: {e}")
            return False
    
    def _analyze_error(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """
        Analyze the error in a dead letter event.
        
        Args:
            event: Dead letter event
            
        Returns:
            Dict containing error analysis
        """
        error_type = event.get('error_type', 'Unknown')
        error_message = event.get('error_message', '')
        processing_stage = event.get('processing_stage', 'Unknown')
        
        # Categorize error
        error_category = self._categorize_error(error_type, error_message, processing_stage)
        
        # Determine if event can be retried
        can_retry = self._can_retry_event(error_type, processing_stage)
        
        # Suggest remediation
        remediation = self._suggest_remediation(error_category, processing_stage)
        
        return {
            'error_category': error_category,
            'can_retry': can_retry,
            'remediation_suggestion': remediation,
            'analyzed_at': datetime.now().isoformat()
        }
    
    def _categorize_error(self, error_type: str, error_message: str, stage: str) -> str:
        """
        Categorize the error based on type, message, and processing stage.
        
        Args:
            error_type: Type of error
            error_message: Error message
            stage: Processing stage where error occurred
            
        Returns:
            str: Error category
        """
        # Missing required fields (check this first)
        if 'required' in error_message.lower() or 'missing' in error_message.lower():
            return 'missing_required_field'
        
        # Invalid enum values
        if 'enum' in error_message.lower() or 'not one of' in error_message.lower():
            return 'invalid_enum_value'
        
        # Data type errors
        if 'type' in error_message.lower() or 'TypeError' in error_type:
            return 'data_type_error'
        
        # Network/connection errors
        if 'connection' in error_message.lower() or 'timeout' in error_message.lower():
            return 'network_error'
        
        # Disk/storage errors
        if 'disk' in error_message.lower() or 'storage' in error_message.lower():
            return 'storage_error'
        
        # Schema validation errors (check this after more specific errors)
        if 'ValidationError' in error_type or 'schema' in error_message.lower():
            return 'schema_validation_error'
        
        # Processing stage specific errors
        if stage == 'producer_validation':
            return 'producer_validation_error'
        elif stage == 'consumer_validation':
            return 'consumer_validation_error'
        elif stage == 'transformation':
            return 'transformation_error'
        elif stage == 'sink_write':
            return 'sink_write_error'
        
        return 'unknown_error'
    
    def _can_retry_event(self, error_type: str, stage: str) -> bool:
        """
        Determine if the event can be retried.
        
        Args:
            error_type: Type of error
            stage: Processing stage where error occurred
            
        Returns:
            bool: True if event can be retried
        """
        # Schema validation errors are usually not retryable
        if 'ValidationError' in error_type:
            return False
        
        # Data type errors are usually not retryable
        if 'TypeError' in error_type:
            return False
        
        # Missing required fields are usually not retryable
        if 'required' in error_type:
            return False
        
        # Network errors are usually retryable
        if 'connection' in error_type.lower() or 'timeout' in error_type.lower():
            return True
        
        # Storage errors might be retryable
        if 'storage' in error_type.lower() or 'disk' in error_type.lower():
            return True
        
        # Transformation errors might be retryable
        if stage == 'transformation':
            return True
        
        # Default to not retryable for safety
        return False
    
    def _suggest_remediation(self, error_category: str, stage: str) -> str:
        """
        Suggest remediation for the error.
        
        Args:
            error_category: Category of error
            stage: Processing stage where error occurred
            
        Returns:
            str: Remediation suggestion
        """
        if error_category == 'missing_required_field':
            return 'Add missing required fields to event data'
        elif error_category == 'invalid_enum_value':
            return 'Use valid enum values from schema definition'
        elif error_category == 'data_type_error':
            return 'Ensure data types match schema requirements'
        elif error_category == 'network_error':
            return 'Check network connectivity and retry'
        elif error_category == 'storage_error':
            return 'Check disk space and file permissions'
        elif error_category == 'schema_validation_error':
            return 'Validate event against schema before processing'
        else:
            return 'Review error details and fix underlying issue'
    
    def get_error_statistics(self) -> Dict[str, Any]:
        """
        Get error statistics.
        
        Returns:
            Dict containing error statistics
        """
        return {
            'processed_dead_letter_events': self.processed_count,
            'failed_dead_letter_events': self.error_count,
            'total_dead_letter_events': self.processed_count + self.error_count
        }
    
    def close(self):
        """Close the dead letter handler."""
        try:
            if self.sink_writer:
                self.sink_writer.close()
            logger.info("Dead letter handler closed")
        except Exception as e:
            logger.error(f"Error closing dead letter handler: {e}")


# Convenience functions
def process_dead_letter_event(event: Dict[str, Any]) -> bool:
    """
    Process a dead letter event using the default handler.
    
    Args:
        event: Dead letter event to process
        
    Returns:
        bool: True if processed successfully
    """
    handler = DeadLetterHandler()
    try:
        return handler.process_dead_letter_event(event)
    finally:
        handler.close() 