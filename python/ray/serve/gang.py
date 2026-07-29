from dataclasses import dataclass
from typing import List

from ray.util.annotations import DeveloperAPI


@DeveloperAPI
@dataclass
class GangContext:
    """Context information for a replica that is part of a gang."""

    gang_id: str
    """Unique identifier for this gang."""

    rank: int
    """This replica's rank within the gang (0-indexed)."""

    world_size: int
    """Total number of replicas in this gang."""

    member_replica_ids: List[str]
    """List of replica IDs in this gang, ordered by rank."""

    pg_name: str = ""
    """Name of the gang placement group. Used to recover the PG reference
    after controller restart and during placement group leak detection."""
