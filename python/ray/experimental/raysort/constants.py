import os

from ray.experimental.raysort.types import ByteCount, RecordCount

__DIR__ = os.path.dirname(os.path.abspath(__file__))

# Basics
RECORD_SIZE = 100  # bytes

# Executable locations
GENSORT_PATH = os.path.join(__DIR__, "bin/gensort/64/gensort")
VALSORT_PATH = os.path.join(__DIR__, "bin/gensort/64/valsort")

# Filenames
WORK_DIR = "/tmp/raysort"
INPUT_MANIFEST_FILE = os.path.join(WORK_DIR, "input-manifest.csv")
OUTPUT_MANIFEST_FILE = os.path.join(WORK_DIR, "output-manifest.csv")
DATA_DIR_FMT = {
    "input": "{mnt}/tmp/input/",
    "output": "{mnt}/tmp/output/",
}
FILENAME_FMT = {
    "input": "input-{part_id:08}",
    "output": "output-{part_id:08}",
}

# Prometheus config
PROM_RAY_EXPORTER_PORT = 8090
PROM_NODE_EXPORTER_PORT = 8091


# Convenience functions
def bytes_to_records(n_bytes: ByteCount) -> RecordCount:
    assert n_bytes % RECORD_SIZE == 0
    return int(n_bytes / RECORD_SIZE)
