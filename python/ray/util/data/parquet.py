import random
from typing import Iterable
from typing import List, Optional, Union

import pyarrow.parquet as pq
from pandas import DataFrame

import ray.util.iter as para_iter
from .dataset import MLDataset
from .source import SourceShard


class ParquetSourceShard(SourceShard):
    def __init__(self,
                 data_pieces: List[pq.ParquetDatasetPiece],
                 columns: Optional[List[str]],
                 partitions: Optional[pq.ParquetPartitions],
                 shard_id: int):
        self._data_pieces = data_pieces
        self._columns = columns
        self._partitions = partitions
        self._shard_id = shard_id

    @property
    def shard_id(self) -> int:
        return self._shard_id

    def __iter__(self) -> Iterable[DataFrame]:
        for piece in self._data_pieces:
            yield piece.read(columns=self._columns, use_threads=False,
                             partitions=self._partitions).to_pandas()


def read_parquet(paths: Union[str, List[str]],
                 num_shards: int,
                 rowgroup_split: bool = True,
                 shuffle: bool = False,
                 shuffle_seed: int = None,
                 columns: Optional[List[str]] = None,
                 **kwargs) -> MLDataset:
    """
    Read parquet format data from hdfs filesystem into a MLDataset.
    Args:
        paths (Union[str, List[str]): a single file path or a list of file path
        rowgroup_split (bool): whether split the files into shards based on
            rowgroup. If set False, each shard will have a list of files.
        shuffle (bool): whether shuffle the ParquetDatasetPiece order when
            divide into shards
        shuffle_seed (int): the shuffle seed
        columns (Optional[List[str]]): a list of column names to read
        kwargs: the other parquet read options
    Returns:
        A MLDataset
    """
    pq_ds = pq.ParquetDataset(paths, **kwargs)
    pieces = pq_ds.pieces
    data_pieces = []
    if rowgroup_split:
        # split base on rowgroup
        for piece in pieces:
            for number in pieces.get_metadata().to_dict()["num_row_groups"]:
                data_pieces.append(
                    pq.ParquetDatasetPiece(
                        piece.path, piece.open_file_func, piece.file_options,
                        number, piece.partition_keys))
    else:
        # split base on file pieces
        data_pieces = pieces.copy()

    if len(data_pieces) < num_shards:
        raise ValueError(
            f"number of data pieces: {len(data_pieces)} should "
            f"larger than num_shards: {num_shards}")

    if shuffle:
        random_shuffle = random.Random(shuffle_seed)
        random_shuffle.shuffle(data_pieces)
    shards = [[] for _ in range(num_shards)]
    for i, item in enumerate(data_pieces):
        shard = shards[i % num_shards]
        if item.row_group is None:
            for number in pieces.get_metadata().to_dict()["num_row_groups"]:
                shard.append(pq.ParquetDatasetPiece(
                    item.path, item.open_file_func, item.file_options,
                    number, item.partition_keys))
        else:
            shard.append(item)

    for i, shard in enumerate(shards):
        shards[i] = ParquetSourceShard(shard, columns, pq_ds.partitions, i)
    it = para_iter.from_iterators(shards, False, "parquet")
    return MLDataset.from_parallel_it(it, batch_size=0)
