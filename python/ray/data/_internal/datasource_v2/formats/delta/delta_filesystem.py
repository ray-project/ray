"""The filesystem the Delta reader uses when the caller passes no ``filesystem``.

It is ``deltalake``'s own storage (``DeltaStorageHandler``), the one
``DeltaTable.to_pyarrow_dataset`` uses for the V1 reader. So it takes
credentials from ``storage_options`` and supports every URI ``deltalake`` does,
including Azure's ``az://``, which PyArrow can't parse. This module also tells
it each file's size from the Delta log, which saves a HEAD request per file on
S3.
"""

import threading
from typing import TYPE_CHECKING, Dict, Mapping, Optional

import pyarrow.fs as pa_fs

from ray.data._internal.util import RetryingPyFileSystem

if TYPE_CHECKING:
    from deltalake.fs import DeltaStorageHandler


class DeltaTableFileSystemHandler(pa_fs.FileSystemHandler):
    """``deltalake``'s storage for one table, told the sizes of the files it opens.

    ``DeltaStorageHandler`` asks the storage for a file's size each time it
    opens a file whose size it wasn't given when it was built: a HEAD request
    on S3. The Delta log records every file's size, so the listing and the
    reader pass them in (:func:`add_known_file_sizes`), and the
    ``DeltaStorageHandler`` is built, with the sizes known so far, when it's
    first used after new ones arrive. It isn't pickled: each task builds its
    own on first use.
    """

    def __init__(
        self, table_uri: str, storage_options: Optional[Dict[str, str]] = None
    ):
        self._table_uri = table_uri
        self._storage_options = storage_options
        self._known_sizes: Dict[str, int] = {}
        self._storage: Optional["DeltaStorageHandler"] = None
        self._lock = threading.Lock()

    def __getstate__(self):
        return {
            "table_uri": self._table_uri,
            "storage_options": self._storage_options,
            "known_sizes": self._known_sizes,
        }

    def __setstate__(self, state):
        self.__init__(state["table_uri"], state["storage_options"])
        self._known_sizes = state["known_sizes"]

    def add_known_sizes(self, sizes: Mapping[str, int]) -> None:
        """Remember the sizes of these files, by path relative to the table."""
        with self._lock:
            new = {p: s for p, s in sizes.items() if self._known_sizes.get(p) != s}
            if new:
                self._known_sizes.update(new)
                self._storage = None

    def _delta_storage(self) -> "DeltaStorageHandler":
        with self._lock:
            if self._storage is None:
                from deltalake.fs import DeltaStorageHandler

                self._storage = DeltaStorageHandler(
                    self._table_uri,
                    options=self._storage_options,
                    known_sizes=dict(self._known_sizes) or None,
                )
            return self._storage

    def equals(self, other) -> bool:
        return (
            isinstance(other, DeltaTableFileSystemHandler)
            and self._table_uri == other._table_uri
            and self._storage_options == other._storage_options
        )

    def get_type_name(self):
        return "delta"

    def normalize_path(self, path):
        return self._delta_storage().normalize_path(path)

    def get_file_info(self, paths):
        return self._delta_storage().get_file_info(paths)

    def get_file_info_selector(self, selector):
        return self._delta_storage().get_file_info_selector(selector)

    def open_input_file(self, path):
        return self._delta_storage().open_input_file(path)

    def open_input_stream(self, path):
        return self._delta_storage().open_input_stream(path)

    def create_dir(self, path, recursive):
        return self._delta_storage().create_dir(path, recursive)

    def delete_dir(self, path):
        return self._delta_storage().delete_dir(path)

    def delete_dir_contents(self, path, missing_dir_ok=False):
        return self._delta_storage().delete_dir_contents(
            path, missing_dir_ok=missing_dir_ok
        )

    def delete_root_dir_contents(self):
        return self._delta_storage().delete_root_dir_contents()

    def delete_file(self, path):
        return self._delta_storage().delete_file(path)

    def move(self, src, dest):
        return self._delta_storage().move(src, dest)

    def copy_file(self, src, dest):
        return self._delta_storage().copy_file(src, dest)

    def open_output_stream(self, path, metadata):
        return self._delta_storage().open_output_stream(path, metadata)

    def open_append_stream(self, path, metadata):
        return self._delta_storage().open_append_stream(path, metadata)


def make_table_filesystem(
    table_uri: str, storage_options: Optional[Dict[str, str]]
) -> pa_fs.FileSystem:
    """A filesystem rooted at the table directory, through ``deltalake``."""
    return pa_fs.PyFileSystem(DeltaTableFileSystemHandler(table_uri, storage_options))


def add_known_file_sizes(
    filesystem: Optional[pa_fs.FileSystem], sizes: Mapping[str, Optional[int]]
) -> None:
    """Tell ``filesystem`` the sizes of files it is about to open.

    Only a filesystem from :func:`make_table_filesystem` (possibly wrapped in
    a ``RetryingPyFileSystem``) uses them. Other filesystems open with the
    size the reader passes along.
    """
    if isinstance(filesystem, RetryingPyFileSystem):
        filesystem = filesystem.unwrap()
    handler = getattr(filesystem, "handler", None)
    if isinstance(handler, DeltaTableFileSystemHandler):
        handler.add_known_sizes({p: s for p, s in sizes.items() if s is not None})
