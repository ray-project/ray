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
        ("permissions_deleted", pa.string()),
        ("permissions_user_ids", pa.list_(pa.binary(16))),
        ("permissions_space", pa.string()),
        ("span_index", pa.uint64()),
        ("permissions_group_ids", pa.list_(pa.binary(16))),
        ("permissions_team_ids", pa.list_(pa.binary(16))),
        ("collection_id", pa.binary(16)),
        ("permissions_public", pa.string()),
        ("space_id", pa.binary(16)),
        ("indexed_at", pa.uint64()),
        ("page_id", pa.binary(16)),
        ("ancestors", pa.list_(pa.binary(16))),
        ("last_edited_ds", pa.uint64()),
        ("span_length", pa.uint64()),
        ("authors", pa.list_(pa.binary(16))),
        ("span_text", pa.string()),
        ("team_id", pa.binary(16)),
        ("id", pa.string()),
        ("core_span_block_ids", pa.list_(pa.binary(16))),
        ("overlap_block_ids", pa.list_(pa.binary(16))),
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
