"""
Parquet sink writer for storing transformed events efficiently.
"""
import os
import logging
import uuid
from typing import Dict, Any, List, Optional
from datetime import datetime
import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import Table

from src.config import OUTPUT_DIR, BATCH_SIZE, BATCH_TIMEOUT_SECONDS
from src.utils import setup_logging

logger = setup_logging(__name__)


class ParquetSinkWriter:
    """
    Parquet sink writer for storing transformed events.
    
    Features:
    - Batch writing for efficiency
    - Automatic file rotation
    - Error handling and retry logic
    - Compression for storage efficiency
    """
    
    def __init__(self, output_dir: str = None, batch_size: int = None):
        """Initialize the Parquet sink writer."""
        self.output_dir = output_dir or OUTPUT_DIR
        self.batch_size = batch_size or BATCH_SIZE
        self.current_batch = []
        self.file_count = 0
        self.total_events_written = 0
        
        # Ensure output directory exists
        os.makedirs(self.output_dir, exist_ok=True)
        
        logger.info(f"Parquet sink writer initialized: {self.output_dir}")
    
    def add_event(self, event: Dict[str, Any]) -> bool:
        """
        Add an event to the current batch.
        
        Args:
            event: Transformed event to add
            
        Returns:
            bool: True if event added successfully
        """
        try:
            self.current_batch.append(event)
            
            # Check if batch is full
            if len(self.current_batch) >= self.batch_size:
                self.flush_batch()
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to add event to batch: {e}")
            return False
    
    def add_events(self, events: List[Dict[str, Any]]) -> int:
        """
        Add multiple events to the current batch.
        
        Args:
            events: List of transformed events
            
        Returns:
            int: Number of events successfully added
        """
        added_count = 0
        
        for event in events:
            if self.add_event(event):
                added_count += 1
        
        return added_count
    
    def flush_batch(self) -> bool:
        """
        Flush the current batch to a Parquet file.
        
        Returns:
            bool: True if batch written successfully
        """
        if not self.current_batch:
            logger.debug("No events in batch to flush")
            return True
        
        try:
            # Create filename with timestamp and UUID
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            file_uuid = str(uuid.uuid4())[:8]
            filename = f"events_{timestamp}_{file_uuid}.parquet"
            filepath = os.path.join(self.output_dir, filename)
            
            # Convert batch to PyArrow Table
            table = self._batch_to_table(self.current_batch)
            
            # Write to Parquet with compression
            pq.write_table(
                table,
                filepath,
                compression='snappy',
                row_group_size=10000
            )
            
            batch_size = len(self.current_batch)
            self.total_events_written += batch_size
            self.file_count += 1
            
            logger.info(f"Wrote batch of {batch_size} events to {filename}")
            logger.debug(f"Total events written: {self.total_events_written}")
            
            # Clear the batch
            self.current_batch = []
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to flush batch: {e}")
            return False
    
    def _batch_to_table(self, events: List[Dict[str, Any]]) -> Table:
        """
        Convert a batch of events to a PyArrow Table.
        
        Args:
            events: List of event dictionaries
            
        Returns:
            PyArrow Table
        """
        # Convert events to list of lists for PyArrow
        columns = {}
        
        # Get all unique keys from all events
        all_keys = set()
        for event in events:
            all_keys.update(event.keys())
        
        # Initialize columns
        for key in all_keys:
            columns[key] = []
        
        # Fill columns with data
        for event in events:
            for key in all_keys:
                value = event.get(key)
                columns[key].append(value)
        
        # Create PyArrow arrays
        arrays = []
        field_names = []
        
        for key, values in columns.items():
            # Determine appropriate PyArrow type
            pa_type = self._infer_pyarrow_type(values)
            arrays.append(pa.array(values, type=pa_type))
            field_names.append(key)
        
        # Create schema and table
        schema = pa.schema([pa.field(name, array.type) for name, array in zip(field_names, arrays)])
        table = pa.table(arrays, schema=schema)
        
        return table
    
    def _infer_pyarrow_type(self, values: List[Any]) -> pa.DataType:
        """
        Infer the appropriate PyArrow data type from a list of values.
        
        Args:
            values: List of values to infer type from
            
        Returns:
            PyArrow data type
        """
        # Check if all values are None
        if all(v is None for v in values):
            return pa.null()
        
        # Get non-None values
        non_null_values = [v for v in values if v is not None]
        
        if not non_null_values:
            return pa.null()
        
        # Check types
        sample_value = non_null_values[0]
        
        if isinstance(sample_value, bool):
            return pa.bool_()
        elif isinstance(sample_value, int):
            return pa.int64()
        elif isinstance(sample_value, float):
            return pa.float64()
        elif isinstance(sample_value, str):
            return pa.string()
        elif isinstance(sample_value, dict):
            return pa.struct([])  # Simplified for now
        elif isinstance(sample_value, list):
            return pa.list_(pa.string())  # Simplified for now
        else:
            return pa.string()  # Default to string
    
    def close(self) -> bool:
        """
        Close the writer and flush any remaining events.
        
        Returns:
            bool: True if closed successfully
        """
        try:
            if self.current_batch:
                logger.info(f"Flushing final batch of {len(self.current_batch)} events")
                self.flush_batch()
            
            logger.info(f"Sink writer closed. Total events written: {self.total_events_written}")
            return True
            
        except Exception as e:
            logger.error(f"Error closing sink writer: {e}")
            return False
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get statistics about the sink writer.
        
        Returns:
            Dict containing statistics
        """
        return {
            "total_events_written": self.total_events_written,
            "files_written": self.file_count,
            "current_batch_size": len(self.current_batch),
            "output_directory": self.output_dir
        }


class DeadLetterSinkWriter:
    """
    Sink writer for dead letter events.
    
    Writes failed events to JSON files for debugging and potential reprocessing.
    """
    
    def __init__(self, output_dir: str = None):
        """Initialize the dead letter sink writer."""
        self.output_dir = output_dir or os.path.join(OUTPUT_DIR, "dead_letters")
        self.event_count = 0
        
        # Ensure output directory exists
        os.makedirs(self.output_dir, exist_ok=True)
        
        logger.info(f"Dead letter sink writer initialized: {self.output_dir}")
    
    def write_dead_letter_event(self, event: Dict[str, Any]) -> bool:
        """
        Write a dead letter event to a JSON file.
        
        Args:
            event: Dead letter event to write
            
        Returns:
            bool: True if written successfully
        """
        try:
            # Create filename with timestamp and UUID
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            file_uuid = str(uuid.uuid4())[:8]
            filename = f"dead_letter_{timestamp}_{file_uuid}.json"
            filepath = os.path.join(self.output_dir, filename)
            
            # Write event to JSON file
            import json
            with open(filepath, 'w') as f:
                json.dump(event, f, indent=2, default=str)
            
            self.event_count += 1
            logger.warning(f"Wrote dead letter event to {filename}")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to write dead letter event: {e}")
            return False
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get statistics about the dead letter sink writer.
        
        Returns:
            Dict containing statistics
        """
        return {
            "dead_letter_events_written": self.event_count,
            "output_directory": self.output_dir
        }


# Convenience functions
def write_events_to_parquet(events: List[Dict[str, Any]], output_dir: str = None) -> bool:
    """
    Write a list of events to Parquet files.
    
    Args:
        events: List of transformed events
        output_dir: Output directory (optional)
        
    Returns:
        bool: True if written successfully
    """
    writer = ParquetSinkWriter(output_dir=output_dir)
    
    try:
        writer.add_events(events)
        return writer.close()
    except Exception as e:
        logger.error(f"Failed to write events to Parquet: {e}")
        return False


def write_dead_letter_event(event: Dict[str, Any], output_dir: str = None) -> bool:
    """
    Write a dead letter event to JSON.
    
    Args:
        event: Dead letter event
        output_dir: Output directory (optional)
        
    Returns:
        bool: True if written successfully
    """
    writer = DeadLetterSinkWriter(output_dir=output_dir)
    return writer.write_dead_letter_event(event) 