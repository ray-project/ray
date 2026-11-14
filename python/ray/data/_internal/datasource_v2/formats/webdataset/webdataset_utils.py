# Copyright NVIDIA Corporation 2023
# SPDX-License-Identifier: Apache-2.0
"""WebDataset read helpers shared by the V2 WebDataset datasource and reader."""

import fnmatch
import io
import json
import re
import tarfile
from functools import partial
from typing import Any, Callable, Dict, Iterable, Iterator, Optional, Union

from ray.data._internal.datasource_v2.common.image_utils import _ALLOWED_PIL_FORMATS

ALLOW_UNSAFE_DESERIALIZATION_ENV_VAR = (
    "RAY_DATA_WEBDATASET_ALLOW_UNSAFE_DESERIALIZATION"
)

# Number of samples accumulated into each emitted block.
WEBDATASET_READ_CHUNK_SIZE = 512

# Allowlist of PIL plugins the default decoder may decode image members with.
# PIL selects a plugin by file signature rather than extension, so this prevents
# untrusted bytes carried in an image-extensioned member from reaching a
# dangerous parser (e.g. the EPS plugin, which invokes Ghostscript). Covers the
# jpg/png/ppm/pgm/pbm/pnm extensions the decoder handles.
_ALLOWED_WEBDATASET_PIL_FORMATS = _ALLOWED_PIL_FORMATS + ["PPM"]


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


def _valid_sample(sample: Optional[Dict[str, Any]]):
    """Check whether a sample is valid.

    Args:
        sample: sample to be checked

    Returns:
        ``True`` if the sample is a non-empty dict without the ``__bad__`` flag.
    """
    return (
        sample is not None
        and isinstance(sample, dict)
        and len(list(sample.keys())) > 0
        and not sample.get("__bad__", False)
    )


def _apply_list(
    f: Optional[Union[bool, str, Callable, list]],
    sample: Any,
    default: Optional[Callable] = None,
) -> Any:
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


def _check_suffix(suffix: str, suffixes: Optional[Union[list, Callable]]):
    """Check whether a suffix is valid.

    Suffixes can be either None (=accept everything), a callable,
    or a list of patterns. If the pattern contains */? it is treated
    as a glob pattern, otherwise it is treated as a literal.

    Args:
        suffix: suffix to be checked
        suffixes: list of valid suffixes

    Returns:
        ``True`` if the suffix matches the allowed patterns.
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
    fileselect: Optional[Union[list, Callable]] = None,
    filerename: Optional[Union[list, Callable]] = None,
    verbose_open: bool = False,
    meta: Optional[dict] = None,
) -> Iterator[Dict[str, Any]]:
    """Iterate over tar file, yielding filename, content pairs for the given tar stream.

    Args:
        fileobj: file object
        fileselect: patterns or function selecting
            files to be selected
        filerename: patterns or function used to rename selected files
            before yielding them.
        verbose_open: if ``True``, print progress messages when starting
            and finishing iteration over the tar stream.
        meta: metadata to be added to each sample

    Yields:
        Dict[str, Any]: Dictionaries with ``fname`` and ``data`` keys for each
        selected file in the tar stream.
    """
    meta = meta or {}
    with tarfile.open(fileobj=fileobj, mode="r|*") as stream:
        if verbose_open:
            print(f"start {meta}")
        for tarinfo in stream:
            fname = tarinfo.name
            if not tarinfo.isreg() or fname is None:
                continue
            member = stream.extractfile(tarinfo)
            assert member is not None  # Regular files always have contents.
            data = member.read()
            fname = _apply_list(filerename, fname)
            assert isinstance(fname, str)
            if not _check_suffix(fname, fileselect):
                continue
            result = dict(fname=fname, data=data)
            yield result
        if verbose_open:
            print(f"done {meta}")


def _group_by_keys(
    data: Iterable[Dict[str, Any]],
    keys: Callable = _base_plus_ext,
    suffixes: Optional[Union[list, Callable]] = None,
    meta: Optional[dict] = None,
) -> Iterator[Dict[str, Any]]:
    """Return function over iterator that groups key, value pairs into samples.

    The WebDataset specification requires entries that share a sample key to be
    contiguous in the tar archive. This function relies on that invariant: it
    flushes the in-progress sample whenever the key changes. If a key is seen
    again after its run has ended, the resulting samples would be silently
    fragmented (e.g. `001.jpg, 002.jpg, 001.txt` would yield three partial
    samples instead of two complete ones), so we detect that case and raise a
    clear, actionable error. See GH #44068.

    Args:
        data: iterator over key, value pairs
        keys: function that returns key, suffix for a given key
        suffixes: list of suffixes to be included in the sample
        meta: metadata to be added to each sample

    Yields:
        Dict[str, Any]: Grouped samples, where files sharing the same key prefix are
        combined into a single dictionary.
    """
    meta = meta or {}
    current_sample: Optional[Dict[str, Any]] = None
    seen_keys: set = set()
    for filesample in data:
        assert isinstance(filesample, dict)
        fname, value = filesample["fname"], filesample["data"]
        prefix, suffix = keys(fname)
        if prefix is None:
            continue
        if current_sample is None or prefix != current_sample["__key__"]:
            if current_sample is not None:
                # Capture the key before yielding: the consumer (which may
                # include user-provided decoders) could mutate or clear the
                # dict before this generator resumes, which would otherwise
                # KeyError on `current_sample["__key__"]` below.
                last_key = current_sample["__key__"]
                if _valid_sample(current_sample):
                    current_sample.update(meta)
                    yield current_sample
                seen_keys.add(last_key)
            if prefix in seen_keys:
                raise ValueError(
                    f"WebDataset tar entries for key '{prefix}' are not "
                    f"contiguous in the archive (tar is "
                    f"{meta.get('__url__')}). `read_webdataset` requires "
                    "files sharing a sample key (the base name before the "
                    "first '.') to be adjacent in the tar, per the "
                    "WebDataset specification. Re-create the archive with "
                    "entries sorted by name, e.g. via `tar --sort=name ...` "
                    "or `find ... | sort | tar -T - ...`."
                )
            current_sample = dict(__key__=prefix)
            if "__url__" in filesample:
                current_sample["__url__"] = filesample["__url__"]
        if suffix in current_sample:
            raise ValueError(
                f"{fname}: duplicate file name in tar file "
                + f"{suffix} {current_sample.keys()}, tar is {meta.get('__url__')}"
            )
        if suffixes is None or _check_suffix(suffix, suffixes):
            current_sample[suffix] = value
    if current_sample is not None and _valid_sample(current_sample):
        current_sample.update(meta)
        yield current_sample


def _default_decoder(
    sample: Dict[str, Any],
    format: Optional[Union[bool, str]] = True,
    allow_unsafe: bool = False,
    expand_grayscale: bool = False,
):
    """A default decoder for webdataset.

    This handles common file extensions: .txt, .cls, .cls2,
        .jpg, .png, .json, .npy, .mp, .pt, .pth, .pickle, .pkl.
    These are the most common extensions used in webdataset.
    For other extensions, users can provide their own decoder.

    Args:
        sample: sample, modified in place
        format: optional image format hint (e.g. ``"PIL"`` to return PIL
            images instead of numpy arrays).
        allow_unsafe: if True, allow pickle/torch deserialization
        expand_grayscale: if True, decode grayscale images as (H, W, 1)
            instead of (H, W), so every image has three dimensions.

    Returns:
        The sample with values decoded according to their key extension.
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

            # Restrict PIL to raster plugins matching these extensions. PIL picks
            # a plugin by file signature, not extension, so without an allowlist a
            # member named ``0001.jpg`` carrying e.g. EPS bytes would be routed to
            # the EPS plugin, which shells out to Ghostscript.
            image = PIL.Image.open(
                io.BytesIO(value), formats=_ALLOWED_WEBDATASET_PIL_FORMATS
            )
            if format == "PIL":
                sample[key] = image
            else:
                image = np.asarray(image)
                if expand_grayscale and image.ndim == 2:
                    image = image[:, :, np.newaxis]
                sample[key] = image
        elif extension == "json":
            sample[key] = json.loads(value)
        elif extension == "npy":
            import numpy as np

            sample[key] = np.load(io.BytesIO(value))
        elif extension == "mp":
            import msgpack  # pyrefly: ignore[missing-import]

            sample[key] = msgpack.unpackb(value, raw=False)
        elif extension in ["pt", "pth"]:
            if not allow_unsafe:
                raise ValueError(
                    f"Refusing to load .{extension} member {key!r} from "
                    f"WebDataset with weights_only=False (arbitrary code "
                    f"execution risk). Provide a custom decoder or set "
                    f"{ALLOW_UNSAFE_DESERIALIZATION_ENV_VAR}=1 "
                    f"for trusted sources."
                )
            import torch

            sample[key] = torch.load(io.BytesIO(value), weights_only=False)
        elif extension in ["pickle", "pkl"]:
            if not allow_unsafe:
                raise ValueError(
                    f"Refusing to unpickle WebDataset member {key!r} "
                    f"(arbitrary code execution risk). Provide a custom "
                    f"decoder or set "
                    f"{ALLOW_UNSAFE_DESERIALIZATION_ENV_VAR}=1 "
                    f"for trusted sources."
                )
            import pickle

            sample[key] = pickle.loads(value)
    return sample


def iter_webdataset_rows(
    stream: Any,
    path: str,
    *,
    decoder: Optional[Union[bool, str, Callable, list]],
    fileselect: Optional[Union[list, Callable]],
    filerename: Optional[Union[list, Callable]],
    suffixes: Optional[Union[list, Callable]],
    verbose_open: bool,
    expand_json: bool,
    allow_unsafe_deserialization: bool,
    expand_grayscale: bool = False,
) -> Iterator[Dict[str, Any]]:
    """Yield one decoded row dict per WebDataset sample in a tar stream.

    Note that ``fileselect`` selects files during reading, while ``suffixes``
    selects files during the grouping step.

    Args:
        stream: File object for the tar archive.
        path: Path of the tar archive, stored in each sample's ``__url__``.
        decoder: Decoder spec applied to each sample, or ``None`` to skip.
        fileselect: Patterns or function selecting files while reading.
        filerename: Patterns or function renaming files while reading.
        suffixes: Patterns or function selecting suffixes while grouping.
        verbose_open: Print progress when starting and finishing the tar.
        expand_json: Expand the ``json`` member's fields into columns. Samples
            without a ``json`` member are left as is, and a field whose name
            matches an existing column raises ``ValueError``.
        allow_unsafe_deserialization: Allow pickle/torch deserialization in
            the default decoder.
        expand_grayscale: Make the default decoder return grayscale images
            as (H, W, 1).

    Yields:
        Dict[str, Any]: One row per sample.
    """

    files = _tar_file_iterator(
        stream,
        fileselect=fileselect,
        filerename=filerename,
        verbose_open=verbose_open,
    )
    samples = _group_by_keys(files, meta=dict(__url__=path), suffixes=suffixes)
    default_decoder = partial(
        _default_decoder,
        allow_unsafe=allow_unsafe_deserialization,
        expand_grayscale=expand_grayscale,
    )

    for sample in samples:
        if decoder is not None:
            sample = _apply_list(decoder, sample, default=default_decoder)
        if expand_json and "json" in sample:
            if isinstance(sample["json"], bytes):
                parsed_json = json.loads(sample["json"].decode("utf-8"))
            elif isinstance(sample["json"], str):
                parsed_json = json.loads(sample["json"])
            elif isinstance(sample["json"], dict):
                parsed_json = sample["json"]
            else:
                raise TypeError(
                    f"Unsupported data type" f" {type(sample['json'])} for sample"
                )
            for k, v in parsed_json.items():
                if k in sample:
                    raise ValueError(
                        f"expand_json can't add JSON field {k!r} to sample "
                        f"{sample.get('__key__')!r} in {path}: the sample already "
                        f"has a column named {k!r}."
                    )
                sample[k] = v
        yield sample
