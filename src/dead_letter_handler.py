"""
Dead letter handler for processing failed events.
"""
import json
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
from collections import defaultdict, Counter

from src.utils import setup_logging, format_error_message
from src.sink_writer import DeadLetterSinkWriter
from schema.schema_validator import SchemaValidator

logger = setup_logging(__name__)


class DeadLetterHandler:
    """
    Handler for dead letter events (failed events).
    
    Features:
    - Validates dead letter event schema
    - Writes to JSON files for debugging
    - Provides error analysis and categorization
    - Supports potential reprocessing
    """
    
    def __init__(self):
        """Initialize the dead letter handler."""
        self.validator = SchemaValidator()
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
            # Validate dead letter event schema
            validated_event = self.validator.validate_dead_letter_event(event)
            
            # Analyze the error
            error_analysis = self._analyze_error(validated_event)
            
            # Add analysis to event
            validated_event['error_analysis'] = error_analysis
            
            # Write to sink
            success = self.sink_writer.write_dead_letter_event(validated_event)
            
            if success:
                self.processed_count += 1
                logger.warning(f"Processed dead letter event: {error_analysis['error_category']}")
                
                # Also add analysis to original event for testing
                event['error_analysis'] = error_analysis
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
        suggestions = {
            'schema_validation_error': 'Review and update event schema or fix data format',
            'data_type_error': 'Check data types and ensure proper type conversion',
            'missing_required_field': 'Add missing required fields to event data',
            'invalid_enum_value': 'Use valid enum values as defined in schema',
            'network_error': 'Check network connectivity and retry',
            'storage_error': 'Check disk space and file permissions',
            'producer_validation_error': 'Fix event data before sending to producer',
            'consumer_validation_error': 'Review event format and schema compliance',
            'transformation_error': 'Check transformation logic and data compatibility',
            'sink_write_error': 'Verify sink configuration and storage availability',
            'unknown_error': 'Review error logs for specific issue'
        }
        
        return suggestions.get(error_category, 'Review error logs for specific issue')
    
    def get_error_statistics(self) -> Dict[str, Any]:
        """
        Get statistics about processed dead letter events.
        
        Returns:
            Dict containing error statistics
        """
        return {
            'processed_dead_letter_events': self.processed_count,
            'failed_dead_letter_events': self.error_count,
            'success_rate': self.processed_count / (self.processed_count + self.error_count) if (self.processed_count + self.error_count) > 0 else 0
        }
    
    def close(self):
        """Close the dead letter handler."""
        try:
            if self.sink_writer:
                self.sink_writer.close()
            
            stats = self.get_error_statistics()
            logger.info(f"Dead letter handler closed. Stats: {stats}")
            
        except Exception as e:
            logger.error(f"Error closing dead letter handler: {e}")


class DeadLetterReprocessor:
    """
    Reprocessor for dead letter events.
    
    Attempts to reprocess failed events after fixing issues.
    """
    
    def __init__(self):
        """Initialize the dead letter reprocessor."""
        self.validator = SchemaValidator()
        self.reprocessed_count = 0
        self.failed_reprocess_count = 0
        
        logger.info("Dead letter reprocessor initialized")
    
    def can_reprocess_event(self, dead_letter_event: Dict[str, Any]) -> bool:
        """
        Check if a dead letter event can be reprocessed.
        
        Args:
            dead_letter_event: Dead letter event to check
            
        Returns:
            bool: True if event can be reprocessed
        """
        error_analysis = dead_letter_event.get('error_analysis', {})
        return error_analysis.get('can_retry', False)
    
    def reprocess_event(self, dead_letter_event: Dict[str, Any]) -> bool:
        """
        Attempt to reprocess a dead letter event.
        
        Args:
            dead_letter_event: Dead letter event to reprocess
            
        Returns:
            bool: True if reprocessed successfully
        """
        try:
            original_event = dead_letter_event.get('original_event', {})
            
            # Try to validate the original event
            self.validator.validate_user_event(original_event)
            
            # If validation passes, the event can be reprocessed
            self.reprocessed_count += 1
            logger.info(f"Successfully validated event for reprocessing: {original_event.get('event_id', 'unknown')}")
            
            return True
            
        except Exception as e:
            self.failed_reprocess_count += 1
            logger.warning(f"Failed to reprocess event: {e}")
            return False
    
    def get_reprocessing_stats(self) -> Dict[str, Any]:
        """
        Get reprocessing statistics.
        
        Returns:
            Dict containing reprocessing statistics
        """
        total_attempts = self.reprocessed_count + self.failed_reprocess_count
        return {
            'reprocessed_events': self.reprocessed_count,
            'failed_reprocess_attempts': self.failed_reprocess_count,
            'total_attempts': total_attempts,
            'reprocessing_success_rate': self.reprocessed_count / total_attempts if total_attempts > 0 else 0
        }


class DeadLetterAnalyzer:
    """
    Analyzer for dead letter events to identify patterns and trends.
    
    Features:
    - Batch analysis of dead letter events
    - Error pattern detection
    - Trend analysis
    - Automated remediation suggestions
    """
    
    def __init__(self):
        """Initialize the dead letter analyzer."""
        self.analysis_count = 0
        logger.info("Dead letter analyzer initialized")
    
    def analyze_batch(self, dead_letter_events: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Analyze a batch of dead letter events.
        
        Args:
            dead_letter_events: List of dead letter events to analyze
            
        Returns:
            Dict containing batch analysis results
        """
        if not dead_letter_events:
            return {
                'total_events': 0,
                'error_categories': {},
                'processing_stages': {},
                'retryable_events': 0,
                'non_retryable_events': 0,
                'common_patterns': [],
                'recommendations': []
            }
        
        # Initialize counters
        error_categories = Counter()
        processing_stages = Counter()
        retryable_count = 0
        non_retryable_count = 0
        error_messages = []
        
        # Analyze each event
        for event in dead_letter_events:
            error_analysis = event.get('error_analysis', {})
            
            # Count error categories
            category = error_analysis.get('error_category', 'unknown')
            error_categories[category] += 1
            
            # Count processing stages
            stage = event.get('processing_stage', 'unknown')
            processing_stages[stage] += 1
            
            # Count retryable vs non-retryable
            if error_analysis.get('can_retry', False):
                retryable_count += 1
            else:
                non_retryable_count += 1
            
            # Collect error messages for pattern analysis
            error_messages.append(event.get('error_message', ''))
        
        # Identify common patterns
        common_patterns = self._identify_patterns(error_messages)
        
        # Generate recommendations
        recommendations = self._generate_recommendations(error_categories, processing_stages)
        
        self.analysis_count += 1
        
        return {
            'total_events': len(dead_letter_events),
            'error_categories': dict(error_categories),
            'processing_stages': dict(processing_stages),
            'retryable_events': retryable_count,
            'non_retryable_events': non_retryable_count,
            'retry_rate': retryable_count / len(dead_letter_events) if dead_letter_events else 0,
            'common_patterns': common_patterns,
            'recommendations': recommendations,
            'analyzed_at': datetime.now().isoformat()
        }
    
    def _identify_patterns(self, error_messages: List[str]) -> List[str]:
        """
        Identify common patterns in error messages.
        
        Args:
            error_messages: List of error messages
            
        Returns:
            List of identified patterns
        """
        patterns = []
        
        # Check for common patterns
        if any('missing' in msg.lower() for msg in error_messages):
            patterns.append("Missing required fields")
        
        if any('enum' in msg.lower() for msg in error_messages):
            patterns.append("Invalid enum values")
        
        if any('type' in msg.lower() for msg in error_messages):
            patterns.append("Data type mismatches")
        
        if any('connection' in msg.lower() for msg in error_messages):
            patterns.append("Network connectivity issues")
        
        if any('timeout' in msg.lower() for msg in error_messages):
            patterns.append("Timeout issues")
        
        if any('schema' in msg.lower() for msg in error_messages):
            patterns.append("Schema validation failures")
        
        return patterns
    
    def _generate_recommendations(self, error_categories: Counter, processing_stages: Counter) -> List[str]:
        """
        Generate recommendations based on error analysis.
        
        Args:
            error_categories: Counter of error categories
            processing_stages: Counter of processing stages
            
        Returns:
            List of recommendations
        """
        recommendations = []
        
        # Most common error category
        if error_categories:
            most_common_error = error_categories.most_common(1)[0][0]
            if most_common_error == 'missing_required_field':
                recommendations.append("Review event generation to ensure all required fields are included")
            elif most_common_error == 'invalid_enum_value':
                recommendations.append("Update event generation to use valid enum values")
            elif most_common_error == 'data_type_error':
                recommendations.append("Check data type handling in transformation logic")
            elif most_common_error == 'network_error':
                recommendations.append("Investigate network connectivity and retry mechanisms")
        
        # Processing stage analysis
        if processing_stages:
            most_common_stage = processing_stages.most_common(1)[0][0]
            if most_common_stage == 'producer_validation':
                recommendations.append("Improve event validation at producer level")
            elif most_common_stage == 'consumer_validation':
                recommendations.append("Review consumer validation logic")
            elif most_common_stage == 'transformation':
                recommendations.append("Enhance transformation error handling")
            elif most_common_stage == 'sink_write':
                recommendations.append("Check sink configuration and storage")
        
        # General recommendations
        if len(error_categories) > 3:
            recommendations.append("Consider implementing more comprehensive error handling")
        
        if not recommendations:
            recommendations.append("Monitor error patterns and adjust accordingly")
        
        return recommendations


# Convenience functions
def process_dead_letter_event(event: Dict[str, Any]) -> bool:
    """
    Process a dead letter event.
    
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


def analyze_dead_letter_events(events: list[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Analyze a list of dead letter events.
    
    Args:
        events: List of dead letter events
        
    Returns:
        Dict containing analysis results
    """
    handler = DeadLetterHandler()
    
    try:
        processed = 0
        failed = 0
        
        for event in events:
            if handler.process_dead_letter_event(event):
                processed += 1
            else:
                failed += 1
        
        return {
            'total_events': len(events),
            'processed': processed,
            'failed': failed,
            'success_rate': processed / len(events) if events else 0
        }
        
    finally:
        handler.close() 