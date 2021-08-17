from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Union, Dict, Tuple

from ray.util.sgd.v2.constants import TIMESTAMP

MAX = "max"
MIN = "min"


@dataclass
class CheckpointConfig:
    # The checkpoint data that should be loaded onto each worker and accessed
    # by the training function via ``sgd.load_checkpoint()``. If this is a
    # ``str`` or ``Path`` then the value is expected to be a path to a file
    # that contains a serialized checkpoint dict. If this is ``None`` then no
    # checkpoint will be loaded.
    checkpoint_to_load: Optional[Union[Dict, str, Path]] = None
    # The directory that checkpoints should be stored in. This can be be an
    # absolute path or a path relative to ``Trainer.latest_run_dir``. If this
    # is ``None`` then checkpoints will be stored in
    # ``<Trainer.latest_run_dir>/checkpoints``.
    checkpoint_dir: Optional[Union[str, Path]] = None
    # How often checkpoints should be persisted to disk - every
    # ``persist_checkpoint_frequency``th call to `sgd.checkpoint()` will be
    # persisted. If this is ``None`` then checkpoints will not be persisted to
    # disk and only one checkpoint will be accessible by
    # ``Trainer.latest_checkpoint``.
    # TODO(matt): Implement the usage of this attribute.
    persist_checkpoint_frequency: Optional[int] = 1
    # The number of checkpoints to keep on disk for this run. If a checkpoint
    # is persisted to disk after there are already this many checkpoints,
    # then an existing checkpoint will be deleted. If this is ``None``
    # then checkpoints will not be deleted.
    # TODO(matt): Implement the usage of this attribute.
    keep_checkpoints_num: Optional[int] = None
    # The strategy for choosing which checkpoints to keep if
    # ``keep_checkpoints_num`` is set. The first argument is the key from the
    # checkpoint dictionary which points to a numerical value. The second
    # argument is either "max" or "min" and determines if the checkpoints
    # with the max or min values will be kept. Default behavior is to retain
    # the checkpoints with maximum timestamp, i.e. the most recent checkpoints.
    # TODO(matt): Implement the usage of this attribute.
    checkpoint_retention_strategy: Tuple[str, str] = (TIMESTAMP, MAX)

    def __post_init__(self):
        if self.persist_checkpoint_frequency is not None:
            if (self.persist_checkpoint_frequency < 1):
                raise ValueError("The value of persist_checkpoint_frequency "
                                 "must be at least 1, or None if checkpoints "
                                 "should not be persisted.")
        if self.keep_checkpoints_num is not None:
            raise NotImplementedError("This attribute is not yet supported.")
