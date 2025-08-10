import json
import logging
from datetime import datetime
from typing import Any, Dict, Iterator, List, Optional

import pyarrow as pa

from ray.data._internal.util import _check_import
from ray.data.block import Block, BlockMetadata
from ray.data.datasource import ReadTask
from ray.data.datasource.streaming_datasource import (
    StreamingDatasource,
    StreamingMetrics,
    create_streaming_read_task,
)
from ray.util.annotations import PublicAPI

logger = logging.getLogger(__name__)

# Global state for tracking current positions across shards
_kinesis_current_positions = {}


def _parse_kinesis_position(position: str) -> Optional[str]:
    """Parse Kinesis sequence number from position string.
    
    Args:
        position: Position string in format "sequence:123" or just "123".
    
    Returns:
        Parsed sequence number as string, or None if invalid.
    """
    if position:
        try:
            if position.startswith("sequence:"):
                return position.split(":", 1)[1]
            else:
                return position
        except (ValueError, IndexError):
            logger.warning(f"Invalid Kinesis sequence number: {position}")
    return None


def _read_kinesis_shard(
    stream_name: str,
    shard_id: str,
    kinesis_config: Dict[str, Any],
    max_records: int = 1000,
) -> Iterator[Dict[str, Any]]:
    """Read records from a Kinesis shard.
    
    Args:
        stream_name: Kinesis stream name.
        shard_id: Shard identifier.
        kinesis_config: Kinesis client configuration.
        max_records: Maximum records to read per call.
    
    Yields:
        Dictionary records from Kinesis.
    """
    _check_import("kinesis_datasource", module="boto3", package="boto3")
    
    import boto3
    from botocore.exceptions import BotoCoreError, ClientError
    
    # Track position
    position_key = f"{stream_name}-{shard_id}"
    
    # Set up metrics
    metrics = StreamingMetrics()
    
    try:
        kinesis = boto3.client("kinesis", **kinesis_config)
        
        # Get shard iterator
        iterator_response = kinesis.get_shard_iterator(
            StreamName=stream_name,
            ShardId=shard_id,
            ShardIteratorType="TRIM_HORIZON"
        )
        
        shard_iterator = iterator_response["ShardIterator"]
        records_read = 0
        
        while shard_iterator and records_read < max_records:
            try:
                # Get records from shard
                response = kinesis.get_records(
                    ShardIterator=shard_iterator,
                    Limit=min(100, max_records - records_read)  # AWS limit is 10k
                )
                
                records = response.get("Records", [])
                shard_iterator = response.get("NextShardIterator")
                
                for record in records:
                    if records_read >= max_records:
                        break
                    
                    try:
                        # Decode record data
                        data_bytes = record["Data"]
                        data_str = data_bytes.decode("utf-8") if isinstance(data_bytes, bytes) else str(data_bytes)
                        
                        # Convert record to dictionary
                        processed_record = {
                            "stream_name": stream_name,
                            "shard_id": shard_id,
                            "sequence_number": record["SequenceNumber"],
                            "partition_key": record["PartitionKey"],
                            "data": data_str,
                            "approximate_arrival_timestamp": record["ApproximateArrivalTimestamp"],
                        }
                        
                        # Try to parse JSON data
                        try:
                            processed_record["data_json"] = json.loads(data_str)
                        except (json.JSONDecodeError, TypeError):
                            processed_record["data_json"] = None
                        
                        # Update position tracking
                        _kinesis_current_positions[position_key] = record["SequenceNumber"]
                        
                        # Record metrics
                        data_size = len(data_bytes) if isinstance(data_bytes, bytes) else len(data_str)
                        metrics.record_read(1, data_size)
                        
                        records_read += 1
                        yield processed_record
                        
                    except Exception as e:
                        logger.warning(f"Error processing Kinesis record: {e}")
                        metrics.record_error()
                        continue
                
                # Stop if no more data
                if not records:
                    break
                    
            except (BotoCoreError, ClientError) as e:
                logger.error(f"Error reading from Kinesis shard {shard_id}: {e}")
                metrics.record_error()
                break
                
    except Exception as e:
        logger.error(f"Error setting up Kinesis client: {e}")
        raise RuntimeError(f"Kinesis read error: {e}") from e


def _get_kinesis_position(stream_name: str, shard_id: str) -> str:
    """Get current position for a Kinesis shard.
    
    Args:
        stream_name: Kinesis stream name.
        shard_id: Shard identifier.
    
    Returns:
        Current position string in format "sequence:123".
    """
    position_key = f"{stream_name}-{shard_id}"
    sequence = _kinesis_current_positions.get(position_key, "TRIM_HORIZON")
    return f"sequence:{sequence}"


@PublicAPI(stability="alpha")
class KinesisDatasource(StreamingDatasource):
    """Kinesis datasource for reading from Amazon Kinesis Data Streams.
    
    This datasource supports reading from Kinesis streams in both batch and streaming
    modes. It handles multiple shards automatically.
    
    Examples:
        Basic usage:
        
        .. testcode::
            :skipif: True
            
            from ray.data._internal.datasource.kinesis_datasource import KinesisDatasource
            
            # Create Kinesis datasource
            datasource = KinesisDatasource(
                stream_name="my-stream",
                kinesis_config={
                    "region_name": "us-west-2"
                }
            )
            
        With credentials:
        
        .. testcode::
            :skipif: True
            
            from ray.data._internal.datasource.kinesis_datasource import KinesisDatasource
            
            # Create datasource with explicit credentials
            datasource = KinesisDatasource(
                stream_name="secure-stream",
                kinesis_config={
                    "region_name": "us-west-2",
                    "aws_access_key_id": "your-key",
                    "aws_secret_access_key": "your-secret"
                }
            )
            
        With specific sequence numbers:
        
        .. testcode::
            :skipif: True
            
            from ray.data._internal.datasource.kinesis_datasource import KinesisDatasource
            
            # Read from specific sequence range
            datasource = KinesisDatasource(
                stream_name="events",
                kinesis_config={"region_name": "us-west-2"},
                start_sequence="sequence:12345",
                end_sequence="sequence:67890"
            )
    """
    
    def __init__(
        self,
        stream_name: str,
        kinesis_config: Dict[str, Any],
        max_records_per_task: int = 1000,
        start_sequence: Optional[str] = None,
        end_sequence: Optional[str] = None,
        **kwargs
    ):
        """Initialize Kinesis datasource.
        
        Args:
            stream_name: Kinesis stream name.
            kinesis_config: Kinesis client configuration dictionary.
            max_records_per_task: Maximum records per task per batch.
            start_sequence: Starting sequence number position.
            end_sequence: Ending sequence number position.
            **kwargs: Additional arguments passed to StreamingDatasource.
        """
        self.stream_name = stream_name
        self.kinesis_config = kinesis_config
        
        # Create streaming config
        streaming_config = {
            "stream_name": self.stream_name,
            "kinesis_config": self.kinesis_config,
            "source_identifier": f"kinesis://{kinesis_config.get('region_name', 'unknown')}/{stream_name}",
        }
        
        super().__init__(
            max_records_per_task=max_records_per_task,
            start_position=start_sequence,
            end_position=end_sequence,
            streaming_config=streaming_config,
            **kwargs
        )
    
    def _validate_config(self) -> None:
        """Validate Kinesis configuration.
        
        Raises:
            ValueError: If configuration is invalid.
            ImportError: If boto3 is not available.
        """
        _check_import(self, module="boto3", package="boto3")
        
        if not self.stream_name:
            raise ValueError("stream_name must be provided and non-empty")
            
        if not self.kinesis_config:
            raise ValueError("kinesis_config must be provided")
            
        if "region_name" not in self.kinesis_config:
            raise ValueError("region_name is required in kinesis_config")
    
    def get_name(self) -> str:
        """Get datasource name.
        
        Returns:
            String identifier for this datasource.
        """
        return f"kinesis({self.stream_name})"
    
    def get_streaming_partitions(self) -> List[Dict[str, Any]]:
        """Get Kinesis shards to read from.
        
        Returns:
            List of shard metadata dictionaries.
        """
        _check_import(self, module="boto3", package="boto3")
        
        import boto3
        
        partitions = []
        
        try:
            kinesis = boto3.client("kinesis", **self.kinesis_config)
            
            # Describe stream to get shards
            response = kinesis.describe_stream(StreamName=self.stream_name)
            shards = response["StreamDescription"]["Shards"]
            
            for shard in shards:
                shard_id = shard["ShardId"]
                partition_info = {
                    "partition_id": f"{self.stream_name}-{shard_id}",
                    "stream_name": self.stream_name,
                    "shard_id": shard_id,
                    "start_sequence": _parse_kinesis_position(self.start_position) if self.start_position else None,
                    "end_sequence": _parse_kinesis_position(self.end_position) if self.end_position else None,
                }
                partitions.append(partition_info)
                
        except Exception as e:
            logger.error(f"Error discovering Kinesis shards: {e}")
            raise
        
        return partitions
    
    def _create_streaming_read_task(self, partition_info: Dict[str, Any]) -> ReadTask:
        """Create read task for a Kinesis shard.
        
        Args:
            partition_info: Shard metadata from get_streaming_partitions.
            
        Returns:
            ReadTask for this shard.
        """
        stream_name = partition_info["stream_name"]
        shard_id = partition_info["shard_id"]
        partition_id = partition_info["partition_id"]
        
        def read_kinesis_fn():
            """Read from Kinesis shard."""
            return _read_kinesis_shard(
                stream_name=stream_name,
                shard_id=shard_id,
                kinesis_config=self.kinesis_config,
                max_records=self.max_records_per_task,
            )
        
        def get_position_fn():
            """Get current Kinesis position."""
            return _get_kinesis_position(stream_name, shard_id)
        
        def get_schema_fn():
            """Get Kinesis record schema."""
            return pa.schema([
                ("stream_name", pa.string()),
                ("shard_id", pa.string()),
                ("sequence_number", pa.string()),
                ("partition_key", pa.string()),
                ("data", pa.string()),
                ("approximate_arrival_timestamp", pa.timestamp("ms")),
                ("data_json", pa.string()),  # JSON will be stored as string
            ])
        
        return create_streaming_read_task(
            partition_id=partition_id,
            streaming_config=self.streaming_config,
            read_source_fn=read_kinesis_fn,
            get_position_fn=get_position_fn,
            get_schema_fn=get_schema_fn,
            start_position=self.start_position,
            end_position=self.end_position,
            max_records=self.max_records_per_task,
        )
    
    def get_streaming_schema(self) -> Optional[pa.Schema]:
        """Get schema for Kinesis records.
        
        Returns:
            PyArrow schema for Kinesis record structure.
        """
        return pa.schema([
            ("stream_name", pa.string()),
            ("shard_id", pa.string()),
            ("sequence_number", pa.string()),
            ("partition_key", pa.string()),
            ("data", pa.string()),
            ("approximate_arrival_timestamp", pa.timestamp("ms")),
            ("data_json", pa.string()),
        ]) 