import os
import tempfile
import zipfile

from azure.common import AzureMissingResourceHttpError
from azure.storage.blob import BlobService
from shutil import unpack_archive
from threading import Event

"""TODOS:
   - use Azure snapshots instead of hacky backups
"""


def fixed_list_blobs(service, *args, **kwargs):
    """By defualt list_containers only returns a subset of results.

    This function attempts to fix this.
    """
    res = []
    next_marker = None
    while next_marker is None or len(next_marker) > 0:
        kwargs['marker'] = next_marker
        gen = service.list_blobs(*args, **kwargs)
        for b in gen:
            res.append(b.name)
        next_marker = gen.next_marker
    return res


def make_archive(source_path, dest_path):
    if source_path.endswith(os.path.sep):
        source_path = source_path.rstrip(os.path.sep)
    prefix_path = os.path.dirname(source_path)
    with zipfile.ZipFile(dest_path, "w", compression=zipfile.ZIP_STORED) as zf:
        if os.path.isdir(source_path):
            for dirname, subdirs, files in os.walk(source_path):
                zf.write(dirname, os.path.relpath(dirname, prefix_path))
                for filename in files:
                    filepath = os.path.join(dirname, filename)
                    zf.write(filepath, os.path.relpath(filepath, prefix_path))
        else:
            zf.write(source_path, os.path.relpath(source_path, prefix_path))


class Container(object):
    services = {}

    def __init__(self, account_name, account_key, container_name, maybe_create=False):
        self._account_name = account_name
        self._container_name = container_name
        if account_name not in Container.services:
            Container.services[account_name] = BlobService(account_name, account_key)
        self._service = Container.services[account_name]
        if maybe_create:
            self._service.create_container(self._container_name, fail_on_exist=False)

    def put(self, source_path, blob_name, callback=None):
        """Upload a file or directory from `source_path` to azure blob `blob_name`.

        Upload progress can be traced by an optional callback.
        """
        upload_done = Event()

        def progress_callback(current, total):
            if callback:
                callback(current, total)
            if current >= total:
                upload_done.set()

        # Attempt to make backup if an existing version is already available
        try:
            x_ms_copy_source = "https://{}.blob.core.windows.net/{}/{}".format(
                self._account_name,
                self._container_name,
                blob_name
            )
            self._service.copy_blob(
                container_name=self._container_name,
                blob_name=blob_name + ".backup",
                x_ms_copy_source=x_ms_copy_source
            )
        except AzureMissingResourceHttpError:
            pass

        with tempfile.TemporaryDirectory() as td:
            arcpath = os.path.join(td, "archive.zip")
            make_archive(source_path, arcpath)
            self._service.put_block_blob_from_path(
                container_name=self._container_name,
                blob_name=blob_name,
                file_path=arcpath,
                max_connections=4,
                progress_callback=progress_callback,
                max_retries=10)
            upload_done.wait()

    def get(self, dest_path, blob_name, callback=None):
        """Download a file or directory to `dest_path` to azure blob `blob_name`.

        Warning! If directory is downloaded the `dest_path` is the parent directory.

        Upload progress can be traced by an optional callback.
        """
        download_done = Event()

        def progress_callback(current, total):
            if callback:
                callback(current, total)
            if current >= total:
                download_done.set()

        with tempfile.TemporaryDirectory() as td:
            arcpath = os.path.join(td, "archive.zip")
            for backup_blob_name in [blob_name, blob_name + '.backup']:
                try:
                    blob_size = self._service.get_blob_properties(
                        blob_name=backup_blob_name,
                        container_name=self._container_name
                    )['content-length']
                    if int(blob_size) > 0:
                        self._service.get_blob_to_path(
                            container_name=self._container_name,
                            blob_name=backup_blob_name,
                            file_path=arcpath,
                            max_connections=4,
                            progress_callback=progress_callback,
                            max_retries=10)
                        unpack_archive(arcpath, dest_path)
                        download_done.wait()
                        return True
                except AzureMissingResourceHttpError:
                    pass
        return False

    def list(self, prefix=None):
        """List all blobs in the container."""
        return fixed_list_blobs(self._service, self._container_name, prefix=prefix)

    def exists(self, blob_name):
        """Returns true if `blob_name` exists in container."""
        try:
            self._service.get_blob_properties(
                blob_name=blob_name,
                container_name=self._container_name
            )
            return True
        except AzureMissingResourceHttpError:
            return False
