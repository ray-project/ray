from typing import TYPE_CHECKING, Iterable, Optional, Union

from .native_file_reader import NativeFileReader
from ray.data._internal.datasource.webdataset_datasource import (
    _apply_list,
    _default_decoder,
    _group_by_keys,
    _tar_file_iterator,
)
from ray.data.block import DataBatch

if TYPE_CHECKING:
    import pyarrow


class WebDatasetReader(NativeFileReader):
    def __init__(
        self,
        decoder: Optional[Union[bool, str, callable, list]] = True,
        fileselect: Optional[Union[bool, callable, list]] = None,
        filerename: Optional[Union[bool, callable, list]] = None,
        suffixes: Optional[Union[bool, callable, list]] = None,
        verbose_open: bool = False,
        **file_reader_kwargs,
    ):
        super().__init__(**file_reader_kwargs)

        self.decoder = decoder
        self.fileselect = fileselect
        self.filerename = filerename
        self.suffixes = suffixes
        self.verbose_open = verbose_open

    def read_stream(self, file: "pyarrow.NativeFile", path: str) -> Iterable[DataBatch]:
        import pandas as pd

        files = _tar_file_iterator(
            file,
            fileselect=self.fileselect,
            filerename=self.filerename,
            verbose_open=self.verbose_open,
        )
        samples = _group_by_keys(files, meta=dict(__url__=path), suffixes=self.suffixes)
        for sample in samples:
            if self.decoder is not None:
                sample = _apply_list(self.decoder, sample, default=_default_decoder)
            yield pd.DataFrame({k: [v] for k, v in sample.items()})
