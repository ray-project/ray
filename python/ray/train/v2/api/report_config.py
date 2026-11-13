from enum import Enum

from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class CheckpointUploadMode(Enum):
    """The manner in which we want to upload the checkpoint.

    Members:
        ASYNC: Upload checkpoint asynchronously.
        SYNC: Upload checkpoint synchronously.
        NO_UPLOAD: Do not upload checkpoint.
    """

    ASYNC = "ASYNC"
    SYNC = "SYNC"
    NO_UPLOAD = "NO_UPLOAD"

    def default_delete_local_checkpoint_after_upload(self) -> bool:
        """Return whether to delete the local checkpoint after upload by default.

        :func:`ray.train.report` uses this value when its
        ``delete_local_checkpoint_after_upload`` argument is ``None``.

        Returns:
            ``True`` for ``ASYNC``, ``False`` for the other modes.
        """
        return self == CheckpointUploadMode.ASYNC


@PublicAPI(stability="alpha")
class CheckpointConsistencyMode(Enum):
    """Read semantics for checkpoint retrieval during an ongoing run.

    Members:
        COMMITTED: Block until the checkpoint from the latest ray.train.report
            has been uploaded and committed.
        VALIDATED: Block until the checkpoint from the latest ray.train.report
            has been uploaded and validated.
    """

    COMMITTED = "COMMITTED"
    VALIDATED = "VALIDATED"
