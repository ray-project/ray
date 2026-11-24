"""Share independently checked ORC test fixtures."""

import pyarrow as pa
from pyarrow import orc


def write_orc_stripes(path, table: pa.Table, counts=(5, 7, 4)):
    """Use Arrow's writer and decoder to check exact, small stripe boundaries."""
    assert sum(counts) == table.num_rows
    with orc.ORCWriter(
        str(path), stripe_size=1, batch_size=max(counts), compression="uncompressed"
    ) as writer:
        start = 0
        for count in counts:
            writer.write(table.slice(start, count))
            start += count
    reader = orc.ORCFile(str(path))
    assert [reader.read_stripe(i).num_rows for i in range(reader.nstripes)] == list(
        counts
    )
    return path
