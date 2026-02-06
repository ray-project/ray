"""Larger-scale test for write_turbopuffer."""

import time

import numpy as np

import ray

# Configuration
API_KEY = "tpuf_IRzJa8EVuzIJzzndbVIZUwLZNfPKqCWo"
NUM_ROWS = 100_000
VECTOR_DIM = 384  # Common embedding dimension (e.g., sentence-transformers)
NUM_PARTITIONS = 10

# Initialize Ray
ray.init()

print(f"Generating {NUM_ROWS:,} rows with {VECTOR_DIM}-dim vectors...")


def generate_batch(batch):
    """Generate random vectors and metadata for each row."""
    n = len(batch["id"])
    # Vectors must be list-of-lists for Turbopuffer JSON serialization
    vectors = np.random.randn(n, VECTOR_DIM).astype(np.float32)
    return {
        "id": batch["id"],
        "vector": [v.tolist() for v in vectors],
        "category": np.char.add("cat-", (batch["id"] % 100).astype(str)),
        "score": np.random.rand(n).astype(np.float32),
    }


# Create dataset: range -> map to add vectors and attributes
ds = (
    ray.data.range(NUM_ROWS)
    .repartition(NUM_PARTITIONS)
    .map_batches(generate_batch, batch_size=1000)
)

# print(f"Dataset: {ds.count():,} rows, {NUM_PARTITIONS} partitions")
# print(f"Schema: {ds.schema()}")
# print()

# Write to Turbopuffer
print("Writing to Turbopuffer...")
start = time.time()

ds.write_turbopuffer(
    namespace="ray-scale-test",
    api_key=API_KEY,
    region="gcp-us-central1",
    schema={
        "score": {"type": "float"},
        "category": {"type": "string"},
    },
)

elapsed = time.time() - start
print(
    f"Done! Wrote {NUM_ROWS:,} vectors in {elapsed:.1f}s ({NUM_ROWS/elapsed:.0f} rows/sec)"
)
