import os
from typing import Optional

from ray.ml.checkpoint import (
    Checkpoint,
    _get_local_path,
    _get_external_path,
)
from ray.util.annotations import Deprecated


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


# Deprecated: Remove in Ray > 1.13
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
        # Deprecated: Remove whole class in Ray > 1.13
        raise DeprecationWarning(
            "`checkpoint.download()` is deprecated and will be removed in "
            "the future. Please use `checkpoint.to_directory()` instead."
        )

    def upload(
        self,
        cloud_path: Optional[str] = None,
        local_path: Optional[str] = None,
        clean_before: bool = False,
    ):
        # Deprecated: Remove whole class in Ray > 1.13
        raise DeprecationWarning(
            "`checkpoint.upload()` is deprecated and will be removed in "
            "the future. Please use `checkpoint.to_uri()` instead."
        )

    def save(self, path: Optional[str] = None, force_download: bool = False):
        # Deprecated: Remove whole class in Ray > 1.13
        raise DeprecationWarning(
            "`checkpoint.save()` is deprecated and will be removed in "
            "the future. Please use `checkpoint.to_directory()` or"
            "`checkpoint.to_uri()` instead."
        )
