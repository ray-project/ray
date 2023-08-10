# Copyright NVIDIA Corporation 2023
# SPDX-License-Identifier: Apache-2.0

import fnmatch
import io
import re
import tarfile
import time
import uuid
from functools import partial
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Union

from ray.data.block import BlockAccessor
from ray.data.datasource.file_based_datasource import FileBasedDatasource
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    import pyarrow


def _base_plus_ext(path: str):
    """Split off all file extensions.

    Returns base, allext.

    Args:
        path: path with extensions

    Returns:
        str: path with all extensions removed
    """
    match = re.match(r"^((?:.*/|)[^.]+)[.]([^/]*)$", path)
    if not match:
        return None, None
    return match.group(1), match.group(2)


def _valid_sample(sample: Dict[str, Any]):
    """Check whether a sample is valid.

    Args:
        sample: sample to be checked
    """
    return (
        sample is not None
        and isinstance(sample, dict)
        and len(list(sample.keys())) > 0
        and not sample.get("__bad__", False)
    )


def _apply_list(
    f: Union[Callable, List[Callable]], sample: Dict[str, Any], default: Callable = None
):
    """Apply a list of functions to a sample.

    Args:
        f: function or list of functions
        sample: sample to be modified
        default: default function to be applied to all keys.
            Defaults to None.

    Returns:
        modified sample
    """
    if f is None:
        return sample
    if not isinstance(f, list):
        f = [f]
    for g in f:
        if default is not None and not callable(g):
            g = partial(default, format=g)
        sample = g(sample)
    return sample


def _check_suffix(suffix: str, suffixes: Union[list, callable]):
    """Check whether a suffix is valid.

    Suffixes can be either None (=accept everything), a callable,
    or a list of patterns. If the pattern contains */? it is treated
    as a glob pattern, otherwise it is treated as a literal.

    Args:
        suffix: suffix to be checked
        suffixes: list of valid suffixes
    """
    if suffixes is None:
        return True
    if callable(suffixes):
        return suffixes(suffix)
    for pattern in suffixes:
        if "*" in pattern or "?" in pattern:
            if fnmatch.fnmatch("." + suffix, pattern):
                return True
        elif suffix == pattern or "." + suffix == pattern:
            return True
    return False


def _tar_file_iterator(
    fileobj: Any,
    fileselect: Optional[Union[bool, callable, list]] = None,
    filerename: Optional[Union[bool, callable, list]] = None,
    verbose_open: bool = False,
    meta: dict = None,
):
    """Iterate over tar file, yielding filename, content pairs for the given tar stream.

    Args:
        fileobj: file object
        fileselect: patterns or function selecting
            files to be selected
        meta: metadata to be added to each sample
    """
    meta = meta or {}
    stream = tarfile.open(fileobj=fileobj, mode="r|*")
    if verbose_open:
        print(f"start {meta}")
    for tarinfo in stream:
        fname = tarinfo.name
        if not tarinfo.isreg() or fname is None:
            continue
        data = stream.extractfile(tarinfo).read()
        fname = _apply_list(filerename, fname)
        assert isinstance(fname, str)
        if not _check_suffix(fname, fileselect):
            continue
        result = dict(fname=fname, data=data)
        yield result
    if verbose_open:
        print(f"done {meta}")


def _group_by_keys(
    data: List[Dict[str, Any]],
    keys: callable = _base_plus_ext,
    suffixes: Optional[Union[list, callable]] = None,
    meta: dict = None,
):
    """Return function over iterator that groups key, value pairs into samples.

    Args:
        data: iterator over key, value pairs
        keys: function that returns key, suffix for a given key
        suffixes: list of suffixes to be included in the sample
        meta: metadata to be added to each sample
    """
    meta = meta or {}
    current_sample = None
    for filesample in data:
        assert isinstance(filesample, dict)
        fname, value = filesample["fname"], filesample["data"]
        prefix, suffix = keys(fname)
        if prefix is None:
            continue
        if current_sample is None or prefix != current_sample["__key__"]:
            if _valid_sample(current_sample):
                current_sample.update(meta)
                yield current_sample
            current_sample = dict(__key__=prefix)
            if "__url__" in filesample:
                current_sample["__url__"] = filesample["__url__"]
        if suffix in current_sample:
            raise ValueError(
                f"{fname}: duplicate file name in tar file "
                + f"{suffix} {current_sample.keys()}"
            )
        if suffixes is None or _check_suffix(suffix, suffixes):
            current_sample[suffix] = value
    if _valid_sample(current_sample):
        current_sample.update(meta)
        yield current_sample


def _default_decoder(sample: Dict[str, Any], format: Optional[Union[bool, str]] = True):
    """A default decoder for webdataset.

    This handles common file extensions: .txt, .cls, .cls2,
        .jpg, .png, .json, .npy, .mp, .pt, .pth, .pickle, .pkl.
    These are the most common extensions used in webdataset.
    For other extensions, users can provide their own decoder.

    Args:
        sample: sample, modified in place
    """
    sample = dict(sample)
    for key, value in sample.items():
        extension = key.split(".")[-1]
        if key.startswith("__"):
            continue
        elif extension in ["txt", "text"]:
            sample[key] = value.decode("utf-8")
        elif extension in ["cls", "cls2"]:
            sample[key] = int(value.decode("utf-8"))
        elif extension in ["jpg", "png", "ppm", "pgm", "pbm", "pnm"]:
            import numpy as np
            import PIL.Image

            if format == "PIL":
                sample[key] = PIL.Image.open(io.BytesIO(value))
            else:
                sample[key] = np.asarray(PIL.Image.open(io.BytesIO(value)))
        elif extension == "json":
            import json

            sample[key] = json.loads(value)
        elif extension == "npy":
            import numpy as np

            sample[key] = np.load(io.BytesIO(value))
        elif extension == "mp":
            import msgpack

            sample[key] = msgpack.unpackb(value, raw=False)
        elif extension in ["pt", "pth"]:
            import torch

            sample[key] = torch.load(io.BytesIO(value))
        elif extension in ["pickle", "pkl"]:
            import pickle

            sample[key] = pickle.loads(value)
    return sample


extension_to_format = {"jpg": "jpeg"}


def _default_encoder(sample: Dict[str, Any], format: Optional[Union[str, bool]] = True):
    """A default encoder for webdataset.

    This handles common file extensions: .txt, .cls, .cls2, .jpg,
        .png, .json, .npy, .mp, .pt, .pth, .pickle, .pkl
    These are the most common extensions used in webdataset.
    For other extensions, users can provide their own encoder.

    Args:
        sample (Dict[str, Any]): sample
    """
    sample = dict(sample)
    for key, value in sample.items():
        extension = key.split(".")[-1]
        if key.startswith("__"):
            continue
        elif extension in ["txt"]:
            sample[key] = value.encode("utf-8")
        elif extension in ["cls", "cls2"]:
            sample[key] = str(value).encode("utf-8")
        elif extension in ["jpg", "jpeg", "png", "ppm", "pgm", "pbm", "pnm"]:
            import numpy as np
            import PIL.Image

            if isinstance(value, np.ndarray):
                value = PIL.Image.fromarray(value)
            assert isinstance(value, PIL.Image.Image)
            stream = io.BytesIO()
            value.save(
                stream, format=extension_to_format.get(extension.lower(), extension)
            )
            sample[key] = stream.getvalue()
        elif extension == "json":
            import json

            sample[key] = json.dumps(value).encode("utf-8")
        elif extension == "npy":
            import numpy as np

            stream = io.BytesIO()
            np.save(stream, value)
            sample[key] = stream.getvalue()
        elif extension == "mp":
            import msgpack

            sample[key] = msgpack.dumps(value)
        elif extension in ["pt", "pth"]:
            import torch

            stream = io.BytesIO()
            torch.save(value, stream)
            sample[key] = stream.getvalue()
        elif extension in ["pickle", "pkl"]:
            import pickle

            stream = io.BytesIO()
            pickle.dump(value, stream)
            sample[key] = stream.getvalue()
    return sample


def _make_iterable(block: BlockAccessor):
    """Make a block iterable.

    This is a placeholder for dealing with more complex blocks.

    Args:
        block: Ray Dataset block

    Returns:
        Iterable[Dict[str,Any]]: Iterable of samples
    """
    return block.iter_rows(public_row_format=False)


@PublicAPI(stability="alpha")
class WebDatasetDatasource(FileBasedDatasource):
    """A Datasource for WebDataset datasets (tar format with naming conventions)."""

    _FILE_EXTENSION = "tar"

    def _read_stream(
        self,
        stream: "pyarrow.NativeFile",
        path: str,
        decoder: Optional[Union[bool, str, callable, list]] = True,
        fileselect: Optional[Union[bool, callable, list]] = None,
        filerename: Optional[Union[bool, callable, list]] = None,
        suffixes: Optional[Union[bool, callable, list]] = None,
        verbose_open: bool = False,
        **kw,
    ):
        """Read and decode samples from a stream.

        Note that fileselect selects files during reading, while suffixes
        selects files during the grouping step.

        Args:
            stream: File descriptor to read from.
            path: Path to the data.
            decoder: decoder or list of decoders to be applied to samples
            fileselect: Predicate for skipping files in tar decoder.
                Defaults to lambda_:False.
            suffixes: List of suffixes to be extracted. Defaults to None.
            verbose_open: Print message when opening files. Defaults to False.

        Yields:
            List[Dict[str, Any]]: List of sample (list of length 1).
        """
        import pandas as pd

        files = _tar_file_iterator(
            stream,
            fileselect=fileselect,
            filerename=filerename,
            verbose_open=verbose_open,
        )
        samples = _group_by_keys(files, meta=dict(__url__=path), suffixes=suffixes)
        for sample in samples:
            if decoder is not None:
                sample = _apply_list(decoder, sample, default=_default_decoder)
            yield pd.DataFrame({k: [v] for k, v in sample.items()})

    def _write_block(
        self,
        f: "pyarrow.NativeFile",
        block: BlockAccessor,
        writer_args_fn: Callable[[], Dict[str, Any]] = lambda: {},
        encoder: Optional[Union[bool, str, callable, list]] = True,
        **kw,
    ):
        """Encode and write samples to a stream.

        Args:
            f: File descriptor to write to.
            block: Data to be written.
            writer_args_fn: Ignored. Defaults to lambda:{}.
            encoder: (List of) encoder(s) to be applied to samples. Defaults to True.
        """

        stream = tarfile.open(fileobj=f, mode="w|")
        samples = _make_iterable(block)
        for sample in samples:
            if not isinstance(sample, dict):
                sample = sample.as_pydict()
            if encoder is not None:
                sample = _apply_list(encoder, sample, default=_default_encoder)
            if "__key__" not in sample:
                sample["__key__"] = uuid.uuid4().hex
            key = sample["__key__"]
            for k, v in sample.items():
                if v is None or k.startswith("__"):
                    continue
                assert isinstance(v, bytes) or isinstance(v, str)
                if not isinstance(v, bytes):
                    v = v.encode("utf-8")
                ti = tarfile.TarInfo(f"{key}.{k}")
                ti.size = len(v)
                ti.mtime = time.time()
                ti.mode, ti.uname, ti.gname = 0o644, "data", "data"
                stream.addfile(ti, io.BytesIO(v))
        stream.close()
