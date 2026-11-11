"""List ORC stripes as read units."""

import secrets
from dataclasses import replace

import pyarrow as pa

from ray.data._internal.datasource_v2.common.non_sampling_file_indexer import (
    NonSamplingFileIndexer,
    _shuffle_file_infos,
)
from ray.data._internal.datasource_v2.formats.orc.orc_metadata import (
    orc_read_unit_id,
    read_orc_metadata,
)
from ray.data._internal.datasource_v2.interfaces.file_manifest import (
    FILE_CHUNK_METADATA_COLUMN_NAME,
    FILE_SIZE_COLUMN_NAME,
    PATH_COLUMN_NAME,
    FileChunk,
    FileManifest,
)
from ray.data._internal.datasource_v2.interfaces.file_partitioner import FilePartitioner
from ray.data._internal.weighted_round_robin import WeightedRoundRobinPartitioner

_ORC_CHUNK_TYPE = pa.struct(
    [
        pa.field("unit_ids", pa.list_(pa.int64())),
        pa.field("num_rows", pa.int64()),
        pa.field("size_bytes", pa.int64()),
        pa.field("fully_matched", pa.bool_()),
        pa.field("unit_sizes", pa.list_(pa.int64())),
        pa.field("unit_rows", pa.list_(pa.int64())),
    ]
)


def _orc_manifest(paths, sizes, chunk_metadatas):
    """Keep whole-file and stripe rows compatible across ORC manifests."""
    return FileManifest(
        pa.table(
            {
                PATH_COLUMN_NAME: pa.array(paths, type=pa.string()),
                FILE_SIZE_COLUMN_NAME: pa.array(sizes, type=pa.int64()),
                FILE_CHUNK_METADATA_COLUMN_NAME: pa.array(
                    chunk_metadatas, type=_ORC_CHUNK_TYPE
                ),
            }
        )
    )


class OrcStripeIndexer(NonSamplingFileIndexer):
    """Read each file's layout while listing and emit its stripe chunks."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._shuffle_seed = secrets.randbits(64)

    def _replayable_shuffle(self, config):
        if config is not None and config.seed is None:
            return replace(config, seed=self._shuffle_seed, reseed_after_execution=True)
        return config

    def list_files(
        self,
        paths,
        *,
        filesystem,
        pruners=None,
        preserve_order=False,
        predicate=None,
        limit=None,
        projected_columns=None,
        shuffle_config=None,
        execution_idx=0,
        excluded_read_unit_ids=None,
    ):
        files = self._iter_file_infos_for_list(
            paths,
            filesystem=filesystem,
            pruners=pruners,
            preserve_order=True,
            shuffle_config=self._replayable_shuffle(shuffle_config),
            execution_idx=execution_idx,
            excluded_read_unit_ids=excluded_read_unit_ids,
        )
        yield from self._list_chunks(
            files, filesystem, predicate, limit, excluded_read_unit_ids
        )

    def list_files_from_file_infos(
        self,
        file_infos,
        *,
        filesystem,
        pruners=None,
        preserve_order=False,
        predicate=None,
        limit=None,
        projected_columns=None,
        shuffle_config=None,
        execution_idx=0,
        excluded_read_unit_ids=None,
    ):
        if pruners:
            file_infos = (
                info
                for info in file_infos
                if all(p.should_include(info.path) for p in pruners)
            )
        shuffle_config = self._replayable_shuffle(shuffle_config)
        if shuffle_config is not None:
            file_infos = _shuffle_file_infos(
                list(file_infos), shuffle_config.get_seed(execution_idx)
            )
        yield from self._list_chunks(
            file_infos, filesystem, predicate, limit, excluded_read_unit_ids
        )

    def _list_chunks(self, files, filesystem, predicate, limit, excluded):
        if limit is not None and limit <= 0:
            return
        excluded = excluded or frozenset()
        matched = 0
        for file in files:
            if file.size is None or file.path in excluded:
                continue
            layout = read_orc_metadata(file.path, filesystem)
            chunks = []
            stripes = layout.stripes
            if stripes is not None and len(stripes) > 1:
                for stripe in stripes:
                    if (
                        not stripe.num_rows
                        or orc_read_unit_id(file.path, stripe.index) in excluded
                    ):
                        continue
                    if predicate is None and limit is not None and matched >= limit:
                        break
                    chunks.append(
                        FileChunk(
                            (stripe.index,),
                            stripe.num_rows,
                            stripe.size_bytes,
                            fully_matched=predicate is None,
                        ).to_metadata()
                    )
                    if predicate is None:
                        matched += stripe.num_rows
            else:
                chunks.append(None)
                if predicate is None:
                    matched += layout.num_rows
            if chunks:
                yield _orc_manifest(
                    paths=[file.path] * len(chunks),
                    sizes=[file.size] * len(chunks),
                    chunk_metadatas=chunks,
                )
            if predicate is None and limit is not None and matched >= limit:
                return


class OrcStripePartitioner(FilePartitioner):
    """Group consecutive stripes by their encoded size in listing order."""

    def __init__(self, target_bytes: int):
        self._packer = WeightedRoundRobinPartitioner(
            min_bucket_size=target_bytes,
            max_bucket_size=target_bytes,
            num_buckets=1,
            emit_before_overflow=True,
        )

    @property
    def requires_global_input(self) -> bool:
        return True

    def add_input(self, input_manifest: FileManifest):
        for path, size, metadata in zip(
            input_manifest.paths,
            input_manifest.file_sizes,
            input_manifest.file_chunk_metadatas,
        ):
            weight = int(size if metadata is None else metadata["size_bytes"])
            self._packer.add_item((str(path), int(size), metadata), weight)

    def has_partition(self) -> bool:
        return self._packer.has_partition()

    def next_partition(self) -> FileManifest:
        paths, sizes, chunks = zip(*self._packer.next_partition())
        return _orc_manifest(list(paths), list(sizes), list(chunks))

    def finalize(self):
        self._packer.finalize()
