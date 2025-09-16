import pyarrow as pa
import uuid
import random
import string
import ray
import pyarrow.parquet as pq
from tqdm import tqdm

STRING_PLACEHOLDER = ""
UUID_PLACEHOLDER = uuid.UUID(int=0)
INT_PLACEHOLDER = 0

TARGET_SIZE_BYTES = 4096
NUM_FILES = 50

SCHEMA = pa.schema(
    [
        ("metadata00", pa.string()),
        ("metadata01", pa.list_(pa.binary(16))),
        ("metadata02", pa.string()),
        ("metadata03", pa.uint64()),
        ("metadata04", pa.list_(pa.binary(16))),
        ("metadata05", pa.list_(pa.binary(16))),
        ("metadata06", pa.binary(16)),
        ("metadata07", pa.string()),
        ("metadata08", pa.binary(16)),
        ("metadata09", pa.uint64()),
        ("metadata10", pa.binary(16)),
        ("metadata11", pa.list_(pa.binary(16))),
        ("metadata12", pa.uint64()),
        ("metadata13", pa.uint64()),
        ("metadata14", pa.list_(pa.binary(16))),
        ("span_text", pa.string()),
        ("metadata15", pa.binary(16)),
        ("metadata16", pa.string()),
        ("metadata17", pa.list_(pa.binary(16))),
        ("metadata18", pa.list_(pa.binary(16))),
    ]
)


def random_word(min_len=3, max_len=8):
    length = random.randint(min_len, max_len)
    return "".join(random.choices(string.ascii_lowercase, k=length))


def create_random_sentence():
    sentence = ""
    while len(sentence.encode("utf-8")) < TARGET_SIZE_BYTES:
        word = random_word()
        sentence += word + " "  # space between words

    # Trim to exact size
    sentence_bytes = sentence.encode("utf-8")[:TARGET_SIZE_BYTES]
    return sentence_bytes.decode("utf-8", errors="ignore")


def create_row():
    return {
        "metadata00": STRING_PLACEHOLDER,
        "metadata01": [UUID_PLACEHOLDER.bytes],
        "metadata02": STRING_PLACEHOLDER,
        "metadata03": INT_PLACEHOLDER,
        "metadata04": [UUID_PLACEHOLDER.bytes],
        "metadata05": [UUID_PLACEHOLDER.bytes],
        "metadata06": UUID_PLACEHOLDER.bytes,
        "metadata07": STRING_PLACEHOLDER,
        "metadata08": UUID_PLACEHOLDER.bytes,
        "metadata09": INT_PLACEHOLDER,
        "metadata10": UUID_PLACEHOLDER.bytes,
        "metadata11": [UUID_PLACEHOLDER.bytes],
        "metadata12": INT_PLACEHOLDER,
        "metadata13": None if random.random() < 0.01 else INT_PLACEHOLDER,
        "metadata14": [UUID_PLACEHOLDER.bytes],
        "span_text": create_random_sentence(),
        "metadata15": UUID_PLACEHOLDER.bytes,
        "metadata16": STRING_PLACEHOLDER,
        "metadata17": [UUID_PLACEHOLDER.bytes],
        "metadata18": [UUID_PLACEHOLDER.bytes],
    }


@ray.remote
def write_table(i: int):
    rows = []
    for _ in range(20_000):
        rows.append(create_row())

    table = pa.Table.from_pylist(rows, schema=SCHEMA)
    pq.write_table(
        table, f"s3://ray-benchmark-data-internal-us-west-2/text-spans/{i}.parquet"
    )


refs = [write_table.remote(i) for i in range(NUM_FILES)]

pbar = tqdm(total=len(refs))
while refs:
    ready, refs = ray.wait(refs, num_returns=1)
    pbar.update(len(ready))

pbar.close()
