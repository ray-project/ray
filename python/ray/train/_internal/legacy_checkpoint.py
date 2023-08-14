from typing import Optional

import ray.cloudpickle as ray_pickle
from ray._private.utils import binary_to_hex, hex_to_binary
from ray.data.preprocessor import Preprocessor
from ray.train._checkpoint import Checkpoint

PREPROCESSOR_KEY = "preprocessor"


class LegacyFrameworkCheckpoint(Checkpoint):
    """A checkpoint to preserve the functionality of legacy
    framework-specific checkpoints.

    Example:

        >>> import tempfile
        >>> checkpoint = LegacyFrameworkCheckpoint(tempfile.mkdtemp())
        >>> preprocessor = Preprocessor()
        >>> preprocessor._attr = 1234
        >>> checkpoint.set_preprocessor(preprocessor)
        >>> checkpoint.get_preprocessor()._attr
        1234
    """

    def get_preprocessor(self) -> Optional[Preprocessor]:
        metadata = self.get_metadata()
        preprocessor_bytes = metadata.get(PREPROCESSOR_KEY)
        if preprocessor_bytes is None:
            return None
        return ray_pickle.loads(hex_to_binary(preprocessor_bytes))

    def set_preprocessor(self, preprocessor: Preprocessor):
        self.update_metadata(
            {PREPROCESSOR_KEY: binary_to_hex(ray_pickle.dumps(preprocessor))}
        )
