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
    assert reader.read().equals(table)
    return path


def write_stripe_pipeline_files(directory):
    import random

    generator = random.Random(66999)
    paths = []
    for file_index in range(2):
        path = directory / f"f{file_index+1:04d}.orc"
        write_orc_stripes(
            path,
            pa.table(
                {
                    "id": range(file_index * 8, (file_index + 1) * 8),
                    "payload": [generator.randbytes(1024) for _ in range(8)],
                }
            ),
            counts=(2,) * 4,
        )
        paths.append(path)
    return paths
