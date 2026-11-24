"""List ORC stripes as read units."""

import secrets
from dataclasses import replace

import pyarrow as pa

from ray.data._internal.datasource_v2.common.non_sampling_file_indexer import (
    NonSamplingFileIndexer,
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
from ray.data._internal.planner.plan_expression.expression_visitors import (
    get_column_references,
)

ORC_TARGET_BYTES = 128 * 1024 * 1024

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


def _has_excluded_stripe(path, excluded):
    prefix = path + "#stripe"
    return any(
        unit.startswith(prefix) and unit[len(prefix) :].isdigit() for unit in excluded
    )


class OrcStripeIndexer(NonSamplingFileIndexer):
    """Read each file's layout while listing and emit its stripe chunks."""

    def __init__(
        self,
        *,
        min_split_size=ORC_TARGET_BYTES,
        max_decoded_stripe_bytes=ORC_TARGET_BYTES,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._min_split_size = min_split_size
        self._max_decoded_stripe_bytes = max_decoded_stripe_bytes
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
        files = super().list_files(
            paths,
            filesystem=filesystem,
            pruners=pruners,
            # Listing tasks can be replayed after partially yielding manifests.
            preserve_order=True,
            shuffle_config=self._replayable_shuffle(shuffle_config),
            execution_idx=execution_idx,
            excluded_read_unit_ids=excluded_read_unit_ids,
        )
        yield from self._list_chunks(
            files,
            filesystem,
            predicate,
            limit,
            excluded_read_unit_ids,
            projected_columns,
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
        files = super().list_files_from_file_infos(
            file_infos,
            filesystem=filesystem,
            pruners=pruners,
            preserve_order=True,
            shuffle_config=self._replayable_shuffle(shuffle_config),
            execution_idx=execution_idx,
            excluded_read_unit_ids=excluded_read_unit_ids,
        )
        yield from self._list_chunks(
            files,
            filesystem,
            predicate,
            limit,
            excluded_read_unit_ids,
            projected_columns,
        )

    def _list_chunks(
        self, manifests, filesystem, predicate, limit, excluded, projected_columns=None
    ):
        if limit is not None and limit <= 0:
            return
        excluded = excluded or frozenset()
        matched = 0
        files = (
            (str(path), size)
            for manifest in manifests
            for path, size in zip(manifest.paths, manifest.known_file_sizes)
        )
        for path, size in files:
            if size is None:
                if _has_excluded_stripe(path, excluded):
                    raise RuntimeError(
                        f"Cannot resume ORC stripes without layout for {path}"
                    )
                yield _orc_manifest([path], [None], [None])
                continue
            # Stripe exclusions and unfiltered limits need footer metadata.
            if size <= self._min_split_size and not excluded and limit is None:
                yield _orc_manifest([path], [size], [None])
                continue
            columns = projected_columns
            if columns is not None and predicate is not None:
                columns = list(
                    dict.fromkeys([*columns, *get_column_references(predicate)])
                )
            layout = read_orc_metadata(path, filesystem, columns)
            chunks = []
            stripes = layout.stripes
            excluded_stripe = stripes is not None and any(
                orc_read_unit_id(path, stripe.index) in excluded for stripe in stripes
            )
            # Avoid changing bin order when earlier files have contributed rows.
            # The per-task limit must also fit within the first nonempty stripe.
            first_rows = next((s.num_rows for s in stripes or () if s.num_rows), 0)
            limit_fits = (
                predicate is None
                and limit is not None
                and matched == 0
                and limit <= first_rows
            )
            split = stripes is not None and (
                excluded_stripe
                or (len(stripes) > 1 and size > self._min_split_size and not limit_fits)
            )
            if split:
                estimate = layout.estimated_max_stripe_bytes
                if estimate is None or estimate > self._max_decoded_stripe_bytes:
                    if excluded_stripe:
                        raise RuntimeError(
                            f"Cannot safely resume ORC stripes for {path}"
                        )
                    split = False
            if split and stripes is not None:
                for stripe in stripes:
                    if (
                        not stripe.num_rows
                        or orc_read_unit_id(path, stripe.index) in excluded
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
                if stripes is None and _has_excluded_stripe(path, excluded):
                    raise RuntimeError(
                        f"Cannot resume ORC stripes without layout for {path}"
                    )
                chunks.append(None)
                if predicate is None:
                    matched += layout.num_rows
            if chunks:
                yield _orc_manifest(
                    paths=[path] * len(chunks),
                    sizes=[size] * len(chunks),
                    chunk_metadatas=chunks,
                )
            if predicate is None and limit is not None and matched >= limit:
                return
