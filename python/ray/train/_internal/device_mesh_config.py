from dataclasses import dataclass, field
from functools import cached_property
from typing import Dict, Literal, Tuple, Union

from ray.util.annotations import DeveloperAPI, PublicAPI

# Axis names for parallel dimensions in device mesh
Axis = Literal["pp", "dp_rep", "dp_shard", "ep", "cp", "tp"]


@PublicAPI(stability="alpha")
@dataclass(frozen=True)
class DPConfig:
    """Data-parallel configuration expressed as replicate × shard.

    This configuration defines how data parallelism is structured across workers,
    supporting both classic DDP (Data Distributed Parallel) and sharded approaches
    like FSDP (Fully Sharded Data Parallel) or HSDP (Hybrid Sharded Data Parallel).

    Args:
        replicate: Number of data-parallel replicas (classic DDP replicas or
            HSDP replicate dimension). Set to "auto" to automatically determine
            based on world size and shard degree. Defaults to 1.
        shard: Shard degree for FSDP/HSDP. Each shard group shares model
            parameters. Defaults to 1 (no sharding, pure DDP).

    Example:
        For a 16-GPU setup with HSDP (4 replicas × 4 shards):

        >>> config = DPConfig(replicate=4, shard=4)

        For pure FSDP across 8 GPUs:

        >>> config = DPConfig(replicate=1, shard=8)

        For classic DDP across 8 GPUs:

        >>> config = DPConfig(replicate=8, shard=1)
    """

    replicate: Union[int, Literal["auto"]] = 1
    shard: int = 1

    def __post_init__(self):
        if self.replicate != "auto" and self.replicate < 1:
            raise ValueError(f"replicate must be >= 1 or 'auto', got {self.replicate}")
        if self.shard < 1:
            raise ValueError(f"shard must be >= 1, got {self.shard}")


@PublicAPI(stability="alpha")
@dataclass(frozen=True)
class DeviceMeshConfig:
    """Configuration for parallel dimensions in distributed training.

    This configuration defines the parallelism strategy across multiple dimensions
    commonly used in distributed training: tensor parallelism (TP), pipeline
    parallelism (PP), context parallelism (CP), expert parallelism (EP), and
    data parallelism (DP).

    The axis_order defines the nesting of parallel dimensions from outermost to
    innermost loop, where the rightmost axis varies fastest. This ordering affects
    how ranks are assigned to different parallel groups.

    Args:
        tp: Tensor parallelism degree. Splits individual tensors across devices.
            Defaults to 1 (no tensor parallelism).
        pp: Pipeline parallelism degree. Splits model layers across devices.
            Defaults to 1 (no pipeline parallelism).
        cp: Context parallelism degree. Splits sequence/context across devices.
            Defaults to 1 (no context parallelism).
        ep: Expert parallelism degree. For mixture-of-experts models.
            Defaults to 1 (no expert parallelism).
        dp: Data parallelism configuration. See :class:`DPConfig`.
            Defaults to DPConfig() (single replica, no sharding).
        axis_order: Order of parallel axes from outermost to innermost.
            Defaults to ("pp", "dp_rep", "dp_shard", "ep", "cp", "tp").

    Example:
        For a 32-GPU setup with 2-way PP, 4-way DP (2 replicas × 2 shards), and 4-way TP:

        >>> config = DeviceMeshConfig(
        ...     pp=2,
        ...     dp=DPConfig(replicate=2, shard=2),
        ...     tp=4,
        ... )
        >>> # Total: 2 * 2 * 2 * 4 = 32 GPUs
    """

    tp: int = 1
    pp: int = 1
    cp: int = 1
    ep: int = 1
    dp: DPConfig = field(default_factory=DPConfig)
    axis_order: Tuple[Axis, ...] = ("pp", "dp_rep", "dp_shard", "ep", "cp", "tp")

    def __post_init__(self):
        if self.tp < 1:
            raise ValueError(f"tp must be >= 1, got {self.tp}")
        if self.pp < 1:
            raise ValueError(f"pp must be >= 1, got {self.pp}")
        if self.cp < 1:
            raise ValueError(f"cp must be >= 1, got {self.cp}")
        if self.ep < 1:
            raise ValueError(f"ep must be >= 1, got {self.ep}")

        valid_axes = {"pp", "dp_rep", "dp_shard", "ep", "cp", "tp"}
        for axis in self.axis_order:
            if axis not in valid_axes:
                raise ValueError(
                    f"Invalid axis '{axis}' in axis_order. "
                    f"Valid axes are: {valid_axes}"
                )

        if len(self.axis_order) != len(set(self.axis_order)):
            duplicates = [a for a in self.axis_order if self.axis_order.count(a) > 1]
            raise ValueError(
                f"Duplicate axes in axis_order: {set(duplicates)}. "
                "Each axis can only appear once."
            )

    @cached_property
    def _other_dims_product(self) -> int:
        """Product of all parallel dimensions except dp.replicate."""
        return self.tp * self.pp * self.cp * self.ep * self.dp.shard

    def validate_world_size(self, world_size: int) -> None:
        """Validate that world_size is consistent with the device mesh configuration.

        Args:
            world_size: Total number of workers.

        Raises:
            ValueError: If world_size doesn't match the expected total from config.
        """
        other_dims = self._other_dims_product()

        if self.dp.replicate == "auto":
            if world_size % other_dims != 0:
                raise ValueError(
                    f"world_size ({world_size}) must be divisible by "
                    f"tp*pp*cp*ep*dp.shard ({other_dims})"
                )
        else:
            expected = other_dims * self.dp.replicate
            if world_size != expected:
                raise ValueError(
                    f"world_size ({world_size}) must equal "
                    f"tp*pp*cp*ep*dp.replicate*dp.shard ({expected})"
                )

    def get_dp_replicate(self, world_size: int) -> int:
        """Get the data parallel replicate value.

        If dp.replicate is "auto", computes the value based on world_size.

        Args:
            world_size: Total number of workers.

        Returns:
            The resolved replicate value.
        """
        if self.dp.replicate != "auto":
            return self.dp.replicate

        return world_size // self._other_dims_product()

    def get_axis_size(self, axis: Axis, world_size: int) -> int:
        """Get the size of a given axis.

        Args:
            axis: The axis name.
            world_size: Total number of workers.

        Returns:
            The size of the axis.
        """
        if axis == "tp":
            return self.tp
        elif axis == "pp":
            return self.pp
        elif axis == "cp":
            return self.cp
        elif axis == "ep":
            return self.ep
        elif axis == "dp_rep":
            return self.get_dp_replicate(world_size)
        elif axis == "dp_shard":
            return self.dp.shard
        else:
            raise ValueError(f"Unknown axis: {axis}")


@DeveloperAPI
def get_data_shard_mapping(
    device_mesh_config: DeviceMeshConfig,
    world_size: int,
) -> Dict[int, Tuple[int, int]]:
    """Map all world ranks to their data shard ID and rank within that shard.

    This function determines which data shard each worker belongs to and its
    position within that shard, based on the device mesh configuration.
    Workers in the same data shard receive the same data, while workers
    in different shards receive different data partitions.

    The mapping is computed based on the axis_order in the DeviceMeshConfig,
    which determines how ranks are assigned across parallel dimensions.

    Args:
        device_mesh_config: The device mesh configuration.
        world_size: Total number of workers.

    Returns:
        A dict mapping world_rank to (data_shard_id, rank_in_shard) where:
        - data_shard_id: The ID of the data shard this rank belongs to
            (0 to num_data_shards-1). Workers with the same data_shard_id
            should receive the same data.
        - rank_in_shard: The rank within the data shard (0 to shard_size-1).

    Raises:
        ValueError: If world_size doesn't match the device mesh config.

    Example:
        For a 16-GPU setup with 2-way DP (replicate) and 8-way model parallel:

        >>> config = DeviceMeshConfig(
        ...     dp=DPConfig(replicate=2, shard=1),
        ...     tp=8,
        ... )
        >>> mapping = get_data_shard_mapping(config, world_size=16)
        >>> # Rank 0: data_shard_id=0, rank_in_shard=0
        >>> mapping[0]
        (0, 0)
        >>> # Rank 8: data_shard_id=1, rank_in_shard=0
        >>> mapping[8]
        (1, 0)
    """
    # Validate inputs
    device_mesh_config.validate_world_size(world_size)

    # Axes that define different data shards (workers with different values
    # on these axes get different data)
    data_shard_axes = {"dp_rep", "dp_shard"}

    # Build the sizes for each axis in order (outermost to innermost)
    axis_sizes = []
    for axis in device_mesh_config.axis_order:
        size = device_mesh_config.get_axis_size(axis, world_size)
        axis_sizes.append((axis, size))

    # Build the mapping for all world ranks
    mapping = {}
    for world_rank in range(world_size):
        # Compute coordinates for each axis from world_rank
        # Using column-major ordering (rightmost varies fastest)
        coords = {}
        remaining = world_rank
        for axis, size in reversed(axis_sizes):
            coords[axis] = remaining % size
            remaining //= size

        # Compute data_shard_id: combine coordinates of data-shard axes
        data_shard_id = 0
        data_shard_multiplier = 1

        for axis, size in reversed(axis_sizes):
            if axis in data_shard_axes and axis in coords:
                data_shard_id += coords[axis] * data_shard_multiplier
                data_shard_multiplier *= size

        # Compute rank_in_shard: the position within workers that share the same data
        # This includes all non-data-shard axes
        rank_in_shard = 0
        rank_multiplier = 1

        for axis, size in reversed(axis_sizes):
            if axis not in data_shard_axes:
                rank_in_shard += coords[axis] * rank_multiplier
                rank_multiplier *= size

        mapping[world_rank] = (data_shard_id, rank_in_shard)

    return mapping
