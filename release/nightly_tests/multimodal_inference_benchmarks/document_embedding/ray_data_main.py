from __future__ import annotations

import pymupdf
import ray
import ray.data
import torch
from langchain.text_splitter import RecursiveCharacterTextSplitter
from sentence_transformers import SentenceTransformer

import uuid
import time

EMBED_MODEL_ID = "sentence-transformers/all-MiniLM-L6-v2"
EMBEDDING_DIM = 384
NUM_GPU_NODES = 8
INPUT_PATH = "s3://anonymous@ray-example-data/digitalcorpora/metadata/"
OUTPUT_PATH = f"s3://ray-data-write-benchmark/{uuid.uuid4().hex}"

MAX_PDF_PAGES = 100
CHUNK_SIZE = 2048
CHUNK_OVERLAP = 200
EMBEDDING_BATCH_SIZE = 10

ray.init()


@ray.remote
def warmup():
    pass


# NOTE: On a fresh Ray cluster, it can take a minute or longer to schedule the first
#       task. To ensure benchmarks compare data processing speed and not cluster startup
#       overhead, this code launches a several tasks as warmup.
ray.get([warmup.remote() for _ in range(64)])


def extract_text_from_pdf(row):
    try:
        # NOTE: Remove the `bytes` column since we don't need it anymore. This is done by
        # the system automatically on Ray Data 2.51+ with the `with_column` API.
        bs = row.pop("bytes")
        doc = pymupdf.Document(stream=bs, filetype="pdf")
        if len(doc) > MAX_PDF_PAGES:
            path = row["path"]
            print(f"Skipping PDF {path} because it has {len(doc)} pages")
            return
        for page in doc:
            row["page_text"] = page.get_text()
            row["page_number"] = page.number
            yield row
    except Exception as e:
        path = row["path"]
        print(f"Error extracting text from PDF {path}: {e}")
        return


def chunker(row):
    splitter = RecursiveCharacterTextSplitter(
        chunk_size=CHUNK_SIZE, chunk_overlap=CHUNK_OVERLAP
    )
    page_text = row.pop("page_text")
    chunk_iter = splitter.split_text(page_text)
    for chunk_index, text in enumerate(chunk_iter):
        row["chunk"] = text
        row["chunk_id"] = chunk_index
        yield row


class Embedder:
    def __init__(self):
        device = "cuda" if torch.cuda.is_available() else "cpu"
        self.model = SentenceTransformer(EMBED_MODEL_ID, device=device)
        self.model.compile()

    def __call__(self, batch):
        embedding = self.model.encode(
            batch["chunk"],
        )
        batch["embedding"] = embedding
        return batch


start_time = time.time()

file_paths = (
    ray.data.read_parquet(INPUT_PATH)
    .filter(lambda row: row["file_name"].endswith(".pdf"))
    .take_all()
)
file_paths = [row["uploaded_pdf_path"] for row in file_paths]
ds = ray.data.read_binary_files(file_paths, include_paths=True)
ds = ds.flat_map(extract_text_from_pdf)
ds = ds.flat_map(chunker)
ds = ds.map_batches(
    Embedder, concurrency=NUM_GPU_NODES, num_gpus=1.0, batch_size=EMBEDDING_BATCH_SIZE
)
ds = ds.select_columns(["path", "page_number", "chunk_id", "chunk", "embedding"])
ds.write_parquet(OUTPUT_PATH)

print("Runtime:", time.time() - start_time)
