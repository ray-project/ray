import os
import shutil
import tempfile
import warnings
from typing import Optional

from ray import logger
from ray.ml.checkpoint import (
    Checkpoint,
    _get_local_path,
    _get_external_path,
)
from ray.util import log_once

from ray.util.annotations import Deprecated
from ray.util.ml_utils.cloud import (
    download_from_bucket,
    clear_bucket,
    upload_to_bucket,
    is_cloud_target,
)


@Deprecated
class _TrialCheckpoint(os.PathLike):
    def __init__(
        self, local_path: Optional[str] = None, cloud_path: Optional[str] = None
    ):
        self._local_path = local_path
        self._cloud_path_tcp = cloud_path

    @property
    def local_path(self):
        return self._local_path

    @local_path.setter
    def local_path(self, path: str):
        self._local_path = path

    @property
    def cloud_path(self):
        return self._cloud_path_tcp

    @cloud_path.setter
    def cloud_path(self, path: str):
        self._cloud_path_tcp = path

    # The following magic methods are implemented to keep backwards
    # compatibility with the old path-based return values.
    def __str__(self):
        return self.local_path or self.cloud_path

    def __fspath__(self):
        return self.local_path

    def __eq__(self, other):
        if isinstance(other, str):
            return self.local_path == other
        elif isinstance(other, TrialCheckpoint):
            return (
                self.local_path == other.local_path
                and self.cloud_path == other.cloud_path
            )

    def __add__(self, other):
        if isinstance(other, str):
            return self.local_path + other
        raise NotImplementedError

    def __radd__(self, other):
        if isinstance(other, str):
            return other + self.local_path
        raise NotImplementedError

    def __repr__(self):
        return (
            f"<TrialCheckpoint "
            f"local_path={self.local_path}, "
            f"cloud_path={self.cloud_path}"
            f">"
        )

    def download(
        self,
        cloud_path: Optional[str] = None,
        local_path: Optional[str] = None,
        overwrite: bool = False,
    ) -> str:
        """Download checkpoint from cloud.

        This will fetch the checkpoint directory from cloud storage
        and save it to ``local_path``.

        If a ``local_path`` argument is provided and ``self.local_path``
        is unset, it will be set to ``local_path``.

        Args:
            cloud_path (Optional[str]): Cloud path to load checkpoint from.
                Defaults to ``self.cloud_path``.
            local_path (Optional[str]): Local path to save checkpoint at.
                Defaults to ``self.local_path``.
            overwrite (bool): If True, overwrites potential existing local
                checkpoint. If False, exits if ``self.local_dir`` already
                exists and has files in it.

        """
        cloud_path = cloud_path or self.cloud_path
        if not cloud_path:
            raise RuntimeError(
                "Could not download trial checkpoint: No cloud "
                "path is set. Fix this by either passing a "
                "`cloud_path` to your call to `download()` or by "
                "passing a `cloud_path` into the constructor. The latter "
                "should automatically be done if you pass the correct "
                "`tune.SyncConfig`."
            )

        local_path = local_path or self.local_path

        if not local_path:
            raise RuntimeError(
                "Could not download trial checkpoint: No local "
                "path is set. Fix this by either passing a "
                "`local_path` to your call to `download()` or by "
                "passing a `local_path` into the constructor."
            )

        # Only update local path if unset
        if not self.local_path:
            self.local_path = local_path

        if (
            not overwrite
            and os.path.exists(local_path)
            and len(os.listdir(local_path)) > 0
        ):
            # Local path already exists and we should not overwrite,
            # so return.
            return local_path

        # Else: Actually download

        # Delete existing dir
        shutil.rmtree(local_path, ignore_errors=True)
        # Re-create
        os.makedirs(local_path, 0o755, exist_ok=True)

        # Here we trigger the actual download
        download_from_bucket(cloud_path, local_path)

        # Local dir exists and is not empty
        return local_path

    def upload(
        self,
        cloud_path: Optional[str] = None,
        local_path: Optional[str] = None,
        clean_before: bool = False,
    ):
        """Upload checkpoint to cloud.

        This will push the checkpoint directory from local storage
        to ``cloud_path``.

        If a ``cloud_path`` argument is provided and ``self.cloud_path``
        is unset, it will be set to ``cloud_path``.

        Args:
            cloud_path (Optional[str]): Cloud path to load checkpoint from.
                Defaults to ``self.cloud_path``.
            local_path (Optional[str]): Local path to save checkpoint at.
                Defaults to ``self.local_path``.
            clean_before (bool): If True, deletes potentially existing
                cloud bucket before storing new data.

        """
        local_path = local_path or self.local_path
        if not local_path:
            raise RuntimeError(
                "Could not upload trial checkpoint: No local "
                "path is set. Fix this by either passing a "
                "`local_path` to your call to `upload()` or by "
                "passing a `local_path` into the constructor."
            )

        cloud_path = cloud_path or self.cloud_path
        if not cloud_path:
            raise RuntimeError(
                "Could not download trial checkpoint: No cloud "
                "path is set. Fix this by either passing a "
                "`cloud_path` to your call to `download()` or by "
                "passing a `cloud_path` into the constructor. The latter "
                "should automatically be done if you pass the correct "
                "`tune.SyncConfig`."
            )

        if not self.cloud_path:
            self.cloud_path = cloud_path

        if clean_before:
            logger.info(f"Clearing bucket contents before upload: {cloud_path}")
            clear_bucket(cloud_path)

        # Actually upload
        upload_to_bucket(cloud_path, local_path)

        return cloud_path

    def save(self, path: Optional[str] = None, force_download: bool = False):
        """Save trial checkpoint to directory or cloud storage.

        If the ``path`` is a local target and the checkpoint already exists
        on local storage, the local directory is copied. Else, the checkpoint
        is downloaded from cloud storage.

        If the ``path`` is a cloud target and the checkpoint does not already
        exist on local storage, it is downloaded from cloud storage before.
        That way checkpoints can be transferred across cloud storage providers.

        Args:
            path (Optional[str]): Path to save checkpoint at. If empty,
                the default cloud storage path is saved to the default
                local directory.
            force_download (bool): If ``True``, forces (re-)download of
                the checkpoint. Defaults to ``False``.
        """
        temp_dirs = set()
        # Per default, save cloud checkpoint
        if not path:
            if self.cloud_path and self.local_path:
                path = self.local_path
            elif not self.cloud_path:
                raise RuntimeError(
                    "Cannot save trial checkpoint: No cloud path "
                    "found. If the checkpoint is already on the node, "
                    "you can pass a `path` argument to save it at another "
                    "location."
                )
            else:
                # No self.local_path
                raise RuntimeError(
                    "Cannot save trial checkpoint: No target path "
                    "specified and no default local directory available. "
                    "Please pass a `path` argument to `save()`."
                )
        elif not self.local_path and not self.cloud_path:
            raise RuntimeError(
                f"Cannot save trial checkpoint to cloud target "
                f"`{path}`: No existing local or cloud path was "
                f"found. This indicates an error when loading "
                f"the checkpoints. Please report this issue."
            )

        if is_cloud_target(path):
            # Storing on cloud
            if not self.local_path:
                # No local copy, yet. Download to temp dir
                local_path = tempfile.mkdtemp(prefix="tune_checkpoint_")
                temp_dirs.add(local_path)
            else:
                local_path = self.local_path

            if self.cloud_path:
                # Do not update local path as it might be a temp file
                local_path = self.download(
                    local_path=local_path, overwrite=force_download
                )

                # Remove pointer to a temporary directory
                if self.local_path in temp_dirs:
                    self.local_path = None

            # We should now have a checkpoint available locally
            if not os.path.exists(local_path) or len(os.listdir(local_path)) == 0:
                raise RuntimeError(
                    f"No checkpoint found in directory `{local_path}` after "
                    f"download - maybe the bucket is empty or downloading "
                    f"failed?"
                )

            # Only update cloud path if it wasn't set before
            cloud_path = self.upload(
                cloud_path=path, local_path=local_path, clean_before=True
            )

            # Clean up temporary directories
            for temp_dir in temp_dirs:
                shutil.rmtree(temp_dir)

            return cloud_path

        local_path_exists = (
            self.local_path
            and os.path.exists(self.local_path)
            and len(os.listdir(self.local_path)) > 0
        )

        # Else: path is a local target
        if self.local_path and local_path_exists and not force_download:
            # If we have a local copy, use it

            if path == self.local_path:
                # Nothing to do
                return self.local_path

            # Both local, just copy tree
            if os.path.exists(path):
                shutil.rmtree(path)

            shutil.copytree(self.local_path, path)
            return path

        # Else: Download
        try:
            return self.download(local_path=path, overwrite=force_download)
        except Exception as e:
            raise RuntimeError(
                "Cannot save trial checkpoint to local target as downloading "
                "from cloud failed. Did you pass the correct `SyncConfig`?"
            ) from e


@Deprecated
class TrialCheckpoint(Checkpoint, _TrialCheckpoint):
    def __init__(
        self,
        local_path: Optional[str] = None,
        cloud_path: Optional[str] = None,
    ):
        _TrialCheckpoint.__init__(self)

        # Checkpoint does not allow empty data, but TrialCheckpoint
        # did. To keep backwards compatibility, we use a placeholder URI
        # here, and manually set self._uri and self._local_dir later.
        PLACEHOLDER = "s3://placeholder"
        Checkpoint.__init__(self, uri=PLACEHOLDER)

        # Reset local variables
        self._uri = None
        self._local_path = None

        self._cloud_path_tcp = None
        self._local_path_tcp = None

        locations = set()
        if local_path:
            # Add _tcp to not conflict with Checkpoint._local_path
            self._local_path_tcp = local_path
            if os.path.exists(local_path):
                self._local_path = local_path
            locations.add(local_path)
        if cloud_path:
            self._cloud_path_tcp = cloud_path
            self._uri = cloud_path
            locations.add(cloud_path)
        self._locations = locations

    @property
    def local_path(self):
        local_path = _get_local_path(self._local_path)
        if not local_path:
            for candidate in self._locations:
                local_path = _get_local_path(candidate)
                if local_path:
                    break
        return local_path or self._local_path_tcp

    @local_path.setter
    def local_path(self, path: str):
        self._local_path = path
        if not path or not os.path.exists(path):
            return
        self._locations.add(path)

    @property
    def cloud_path(self):
        cloud_path = _get_external_path(self._uri)
        if not cloud_path:
            for candidate in self._locations:
                cloud_path = _get_external_path(candidate)
                if cloud_path:
                    break
        return cloud_path or self._cloud_path_tcp

    @cloud_path.setter
    def cloud_path(self, path: str):
        self._cloud_path_tcp = path
        if not self._uri:
            self._uri = path
        self._locations.add(path)

    def download(
        self,
        cloud_path: Optional[str] = None,
        local_path: Optional[str] = None,
        overwrite: bool = False,
    ) -> str:
        if log_once("trial_checkpoint_download_deprecated"):
            warnings.warn(
                "`checkpoint.download()` is deprecated and will be removed in "
                "the future. Please use `checkpoint.to_directory()` instead.",
                DeprecationWarning,
            )
        return _TrialCheckpoint.download(self, cloud_path, local_path, overwrite)

    def upload(
        self,
        cloud_path: Optional[str] = None,
        local_path: Optional[str] = None,
        clean_before: bool = False,
    ):
        if log_once("trial_checkpoint_upload_deprecated"):
            warnings.warn(
                "`checkpoint.upload()` is deprecated and will be removed in "
                "the future. Please use `checkpoint.to_uri()` instead.",
                DeprecationWarning,
            )
        return _TrialCheckpoint.upload(self, cloud_path, local_path, clean_before)

    def save(self, path: Optional[str] = None, force_download: bool = False):
        if log_once("trial_checkpoint_save_deprecated"):
            warnings.warn(
                "`checkpoint.save()` is deprecated and will be removed in "
                "the future. Please use `checkpoint.to_directory()` or"
                "`checkpoint.to_uri()` instead.",
                DeprecationWarning,
            )
        return _TrialCheckpoint.save(self, path, force_download)
