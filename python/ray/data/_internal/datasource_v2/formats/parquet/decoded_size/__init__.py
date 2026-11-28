"""Estimate the Arrow bytes a Parquet row group decodes to, from its footer.

The Arrow schema is walked into one chain of arrays per Parquet leaf: its structs
and lists, then the leaf. Each array is sized from the footer's counts as validity
bitmap plus offsets plus values, exactly. A string leaf without SizeStatistics takes
its values' bytes from the footer too: a bound, or with a dictionary page a guess
from the chunk's min and max. A leaf whose counts aren't exact, or whose
layout it doesn't model (views and dictionaries among them), is left to the caller.
The estimate is for the file's own schema, or for the schema the read casts every
batch to, if given.
"""

from ray.data._internal.datasource_v2.formats.parquet.decoded_size.added_columns import (
    Read,
    added_bytes,
    estimate_added_columns,
)
from ray.data._internal.datasource_v2.formats.parquet.decoded_size.estimate import (
    build_leaf_profiles,
    estimate_ancestors,
    estimate_leaf,
    estimate_row_group,
    estimate_string_bytes,
)
from ray.data._internal.datasource_v2.formats.parquet.decoded_size.footer_counts import (
    LeafChunk,
    leaf_chunks,
)
from ray.data._internal.datasource_v2.formats.parquet.decoded_size.schema_walk import (
    LeafProfile,
)

__all__ = [
    "LeafChunk",
    "LeafProfile",
    "Read",
    "added_bytes",
    "build_leaf_profiles",
    "estimate_added_columns",
    "estimate_ancestors",
    "estimate_leaf",
    "estimate_row_group",
    "estimate_string_bytes",
    "leaf_chunks",
]
