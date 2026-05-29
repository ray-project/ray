# This file is adapted from https://github.com/Eventual-Inc/Daft/tree/9da265d8f1e5d5814ae871bed3cee1b0757285f5/benchmarking/ai/document_embedding
from __future__ import annotations

import time
import uuid

import pymupdf
import torch
from langchain.text_splitter import RecursiveCharacterTextSplitter
import daft
from daft import col
import ray

EMBED_MODEL_ID = "sentence-transformers/all-MiniLM-L6-v2"
EMBEDDING_DIM = 384
NUM_GPU_NODES = 8
INPUT_PATH = "s3://ray-example-data/digitalcorpora/metadata/**/"
OUTPUT_PATH = f"s3://ray-data-write-benchmark/{uuid.uuid4().hex}"
MAX_PDF_PAGES = 100
CHUNK_SIZE = 2048
CHUNK_OVERLAP = 200
EMBEDDING_BATCH_SIZE = 10


daft.context.set_runner_ray()


@ray.remote
def warmup():
    pass


# NOTE: On a fresh Ray cluster, it can take a minute or longer to schedule the first
#       task. To ensure benchmarks compare data processing speed and not cluster startup
#       overhead, this code launches a several tasks as warmup.
ray.get([warmup.remote() for _ in range(64)])


def extract_text_from_parsed_pdf(pdf_bytes):
    try:
        doc = pymupdf.Document(stream=pdf_bytes, filetype="pdf")
        if len(doc) > MAX_PDF_PAGES:
            print(f"Skipping PDF because it has {len(doc)} pages")
            return None
        page_texts = [
            {"text": page.get_text(), "page_number": page.number} for page in doc
        ]
        return page_texts
    except Exception as e:
        print(f"Error extracting text from PDF {e}")
        return None


def chunk(text):
    splitter = RecursiveCharacterTextSplitter(
        chunk_size=CHUNK_SIZE, chunk_overlap=CHUNK_OVERLAP
    )
    chunk_iter = splitter.split_text(text)
    chunks = []
    for chunk_index, text in enumerate(chunk_iter):
        chunks.append(
            {
                "text": text,
                "chunk_id": chunk_index,
            }
        )
    return chunks


@daft.udf(
    return_dtype=daft.DataType.fixed_size_list(daft.DataType.float32(), EMBEDDING_DIM),
    concurrency=NUM_GPU_NODES,
    num_gpus=1.0,
    batch_size=EMBEDDING_BATCH_SIZE,
)
class Embedder:
    def __init__(self):
        from sentence_transformers import SentenceTransformer

        device = "cuda" if torch.cuda.is_available() else "cpu"
        self.model = SentenceTransformer(EMBED_MODEL_ID, device=device)
        self.model.compile()

    def __call__(self, text_col):
        if len(text_col) == 0:
            return []
        embeddings = self.model.encode(
            text_col.to_pylist(),
            convert_to_tensor=True,
            # torch_dtype=torch.bfloat16,
        )
        return embeddings.cpu().numpy()


start_time = time.time()

df = daft.read_parquet(INPUT_PATH)
df = df.where(daft.col("file_name").str.endswith(".pdf"))
df = df.with_column("pdf_bytes", df["uploaded_pdf_path"].url.download())
pages_struct_type = daft.DataType.struct(
    fields={"text": daft.DataType.string(), "page_number": daft.DataType.int32()}
)
df = df.with_column(
    "pages",
    df["pdf_bytes"].apply(
        extract_text_from_parsed_pdf,
        return_dtype=daft.DataType.list(pages_struct_type),
    ),
)
df = df.explode("pages")
df = df.with_columns(
    {"page_text": col("pages")["text"], "page_number": col("pages")["page_number"]}
)
df = df.where(daft.col("page_text").not_null())
chunks_struct_type = daft.DataType.struct(
    fields={"text": daft.DataType.string(), "chunk_id": daft.DataType.int32()}
)
df = df.with_column(
    "chunks",
    df["page_text"].apply(chunk, return_dtype=daft.DataType.list(chunks_struct_type)),
)
df = df.explode("chunks")
df = df.with_columns(
    {"chunk": col("chunks")["text"], "chunk_id": col("chunks")["chunk_id"]}
)
df = df.where(daft.col("chunk").not_null())
df = df.with_column("embedding", Embedder(df["chunk"]))
df = df.select("uploaded_pdf_path", "page_number", "chunk_id", "chunk", "embedding")
df.write_parquet(OUTPUT_PATH)

print("Runtime:", time.time() - start_time)
