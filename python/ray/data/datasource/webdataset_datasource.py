from typing import Any, Callable, Dict, TYPE_CHECKING
import tarfile
import io
import time
import re
import uuid
import os

from ray.util.annotations import PublicAPI
from ray.data.block import BlockAccessor
from ray.data.datasource.file_based_datasource import FileBasedDatasource


if TYPE_CHECKING:
    import pyarrow

verbose_open = False


def base_plus_ext(path):
    """Split off all file extensions.

    Returns base, allext.

    Args:
        path (str): path with extensions

    Returns:
        str: path with all extensions removed
    """
    match = re.match(r"^((?:.*/|)[^.]+)[.]([^/]*)$", path)
    if not match:
        return None, None
    return match.group(1), match.group(2)


def valid_sample(sample):
    """Check whether a sample is valid.

    Args:
        sample (dict): sample to be checked
    """
    return (
        sample is not None
        and isinstance(sample, dict)
        and len(list(sample.keys())) > 0
        and not sample.get("__bad__", False)
    )


def tar_file_iterator(fileobj, skipfn=lambda fname: False):
    """Iterate over tar file, yielding filename, content pairs for the given tar stream.

    Args:
        fileobj (file): file object
        skipfn (function): function indicating that files should be skipped
        meta (dict): metadata to be added to each sample
    """
    global verbose_open
    stream = tarfile.open(fileobj=fileobj, mode="r|*")
    if verbose_open:
        print(f"start {meta}")
    for tarinfo in stream:
        fname = tarinfo.name
        if not tarinfo.isreg() or fname is None or skipfn(fname):
            continue
        data = stream.extractfile(tarinfo).read()
        result = dict(fname=fname, data=data)
        yield result
    if verbose_open:
        print(f"done {meta}")


def group_by_keys(data, keys=base_plus_ext, suffixes=None, meta={}):
    """Return function over iterator that groups key, value pairs into samples.

    Args:
        data (iterator): iterator over key, value pairs
        keys (function): function that returns key, suffix for a given key
        suffixes (list): list of suffixes to be included in the sample
        meta (dict): metadata to be added to each sample
    """
    current_sample = None
    for filesample in data:
        assert isinstance(filesample, dict)
        fname, value = filesample["fname"], filesample["data"]
        prefix, suffix = keys(fname)
        if prefix is None:
            continue
        if current_sample is None or prefix != current_sample["__key__"]:
            if valid_sample(current_sample):
                current_sample.update(meta)
                yield current_sample
            current_sample = dict(__key__=prefix)
            if "__url__" in filesample:
                current_sample["__url__"] = filesample["__url__"]
        if suffix in current_sample:
            raise ValueError(
                f"{fname}: duplicate file name in tar file {suffix} {current_sample.keys()}"
            )
        if suffixes is None or suffix in suffixes:
            current_sample[suffix] = value
    if valid_sample(current_sample):
        current_sample.update(meta)
        yield current_sample


def table_to_list(table):
    """Convert a pyarrow table to a list of dictionaries.

    Args:
        table (pyarrow table): pyarrow table
    """
    result = []
    for i in range(table.num_rows):
        row = {}
        for name in table.column_names:
            row[name] = table[name][i].as_py()
        result.append(row)
    return result


def default_decoder(sample: Dict[str, Any]):
    """A default decoder for webdataset.

    This handles common file extensions: .txt, .cls, .cls2, .jpg, .png, .json, .npy, .mp, .pt, .pth, .pickle, .pkl.

    Args:
        sample (Dict[str, Any]): sample, modified in place
    """
    sample = dict(sample)
    for key, value in sample.items():
        extension = os.path.splitext(key)[1]
        if key.startswith("__"):
            continue
        elif extension in [".txt"]:
            sample[key] = value.decode("utf-8")
        elif extension in [".cls", ".cls2"]:
            sample[key] = int(value.decode("utf-8"))
        elif extension in [".jpg", ".png", ".ppm", ".pgm", ".pbm", ".pnm"]:
            import PIL.Image

            sample[key] = PIL.Image.open(io.BytesIO(value))
        elif extension == ".json":
            import json

            sample[key] = json.loads(value)
        elif extension == ".npy":
            import numpy as np

            sample[key] = np.load(io.BytesIO(value))
        elif extension == ".mp":
            import msgpack

            sample[key] = msgpack.unpackb(value, raw=False)
        elif extension in [".pt", ".pth"]:
            import torch

            sample[key] = torch.load(io.BytesIO(value))
        elif extension in [".pickle", ".pkl"]:
            import pickle

            sample[key] = pickle.loads(value)
    return sample


def default_encoder(sample: Dict[str, Any]):
    """A default encoder for webdataset.

    This handles common file extensions: .txt, .cls, .cls2, .jpg, .png, .json, .npy, .mp, .pt, .pth, .pickle, .pkl

    Args:
        sample (Dict[str, Any]): sample
    """
    sample = dict(sample)
    for key, value in sample.items():
        extension = os.path.splitext(key)[1]
        if key.startswith("__"):
            continue
        elif extension in [".txt"]:
            sample[key] = value.encode("utf-8")
        elif extension in [".cls", ".cls2"]:
            sample[key] = str(value).encode("utf-8")
        elif extension in [".jpg", ".png", ".ppm", ".pgm", ".pbm", ".pnm"]:
            import PIL.Image
            import numpy as np

            if isinstance(value, np.ndarray):
                value = PIL.Image.fromarray(value)
            assert isinstance(value, PIL.Image.Image)
            stream = io.BytesIO()
            value.save(stream, format=extension[1:])
            sample[key] = stream.getvalue()
        elif extension == ".json":
            import json

            sample[key] = json.dumps(value).encode("utf-8")
        elif extension == ".npy":
            import numpy as np

            stream = io.BytesIO()
            np.save(stream, value)
            sample[key] = stream.getvalue()
        elif extension == ".mp":
            import msgpack

            sample[key] = msgpack.dumps(value)
        elif extension in [".pt", ".pth"]:
            import torch

            stream = io.BytesIO()
            torch.save(value, stream)
            sample[key] = stream.getvalue()
        elif extension in [".pickle", ".pkl"]:
            import pickle

            stream = io.BytesIO()
            pickle.dump(value, stream)
            sample[key] = stream.getvalue()
        return sample


@PublicAPI(stability="alpha")
class WebDatasetDatasource(FileBasedDatasource):
    """A Datasource for WebDataset datasets (tar format with naming conventions)."""

    _FILE_EXTENSION = "tar"

    def _read_stream(
        self,
        stream: "pyarrow.NativeFile",
        path: str,
        decoder=default_decoder,
        skipfn=lambda _: False,
        suffixes=None,
        **kw,
    ):
        import pyarrow as pa

        files = tar_file_iterator(stream, skipfn=skipfn)
        samples = group_by_keys(files, meta=dict(__url__=path), suffixes=suffixes)
        for sample in samples:
            if decoder is not None:
                sample = decoder(sample)
            sample = {k: [v] for k, v in sample.items()}
            yield pa.Table.from_pydict(sample)

    def _write_block(
        self,
        f: "pyarrow.NativeFile",
        block: BlockAccessor,
        writer_args_fn: Callable[[], Dict[str, Any]] = lambda: {},
        encoder=default_encoder,
        **kw,
    ):
        import pyarrow

        table = block.to_arrow()
        # to_pylist is missing from these tables for some reason
        # samples = table.to_pylist()
        samples = table_to_list(table)
        stream = tarfile.open(fileobj=f, mode="w|")
        for sample in samples:
            if encoder is not None:
                sample = encoder(sample)
            if not "__key__" in sample:
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
