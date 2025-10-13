from enum import Enum

from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class CheckpointUploadMode(Enum):
    """The manner in which we want to upload the checkpoint.

    Args:
        ASYNC: Upload checkpoint asynchronously.
        SYNC: Upload checkpoint synchronously.
        NO_UPLOAD: Do not upload checkpoint.
    """

    ASYNC = "ASYNC"
    SYNC = "SYNC"
    NO_UPLOAD = "NO_UPLOAD"

    def _default_delete_local_checkpoint_after_upload(self) -> bool:
        return self == CheckpointUploadMode.ASYNC
