from typing import Any

from pprint import pprint
import ray
from ray.data.llm import build_llm_processor, vLLMEngineProcessorConfig

DATASET_LIMIT = 10_000

# Define the path to the sample CSV file hosted on S3.
# This dataset contains 2 million rows of synthetic customer data.
path = "https://llm-guide.s3.us-west-2.amazonaws.com/data/ray-data-llm/customers-2000000.csv"

# Load the CSV file into a Ray Dataset.
print("Loading dataset from remote URL...")
ds = ray.data.read_csv(path)

# Limit the dataset. If DATASET_LIMIT > dataset size, the entire dataset will be processed.
print(f"Limiting dataset to {DATASET_LIMIT} rows for initial processing.")
ds_small = ds.limit(DATASET_LIMIT)

# Repartition the dataset to enable parallelism across multiple workers (GPUs).
# By default, streaming datasets might not be optimally partitioned. Repartitioning
# splits the data into a specified number of blocks, allowing Ray to process them
# in parallel.
# Tip: Repartition count should typically be 2-4x your worker (GPU) count.
# Example: 4 GPUs → 8-16 partitions, 10 GPUs → 20-40 partitions.
# This ensures enough parallelism while avoiding excessive overhead.
num_partitions = 128
print(f"Repartitioning dataset into {num_partitions} blocks for parallelism...")
ds_small = ds_small.repartition(num_blocks=num_partitions)

processor_config = vLLMEngineProcessorConfig(
    model_source="unsloth/Llama-3.1-8B-Instruct",
    engine_kwargs=dict(
        max_model_len=256,  # Hard cap: system prompt + user prompt + output tokens must fit within this limit
    ),
    batch_size=256,
    accelerator_type="L4",
    concurrency=4,
)

# For better output token control, restrain generation to these choices
CHOICES = [
    "Law Firm",
    "Healthcare",
    "Technology",
    "Retail",
    "Consulting",
    "Manufacturing",
    "Finance",
    "Real Estate",
    "Other",
]

# Preprocess function prepares `messages` and `sampling_params` for vLLM engine.
# All other fields are ignored by the engine.
def preprocess(row: dict[str, Any]) -> dict[str, Any]:
    return dict(
        messages=[
            {
                "role": "system",
                "content": "You are a helpful assistant that infers company industries. "
                           "Based on the company name provided, output only the industry category. "
                           f"Choose from: {', '.join(CHOICES)}."
            },
            {
                "role": "user",
                "content": f"What industry is this company in: {row['Company']}"
            },
        ],
        sampling_params=dict(
            temperature=0,  # Use 0 for deterministic output
            max_tokens=16,  # Max output tokens. Industry names are short
            structured_outputs=dict(choice=CHOICES), # Constraint generation
        ),
    )

# Postprocess function extracts the generated text from the engine output.
# The **row syntax returns all original columns in the input dataset.
def postprocess(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "inferred_industry": row["generated_text"],
        **row,  # Include all original columns.
    }

# Build the LLM processor with the configuration and functions.
processor = build_llm_processor(
    processor_config,
    preprocess=preprocess,
    postprocess=postprocess,
)

# Run the processor on the small dataset.
processed_small = processor(ds_small)

# Materialize the dataset to memory.
processed_small = processed_small.materialize()

print(f"\nProcessed {processed_small.count()} rows successfully.")
# Display the first 3 entries to verify the output.
sampled = processed_small.take(3)
print("\n==================GENERATED OUTPUT===============\n")
pprint(sampled)
