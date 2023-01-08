from typing import Callable, Optional, Iterator, Dict, Any

from ray.data.block import Block


class MapOperatorActorsImpl:
    def __init__(
        self,
        transform_fn: Callable[[Iterator[Block]], Iterator[Block]],
        ray_remote_args: Optional[Dict[str, Any]],
        min_rows_per_bundle: Optional[int],
    ):
        pass
