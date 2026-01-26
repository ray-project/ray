from typing import Any

from pprint import pprint
import ray
from ray.data.llm import build_llm_processor, vLLMEngineProcessorConfig

DATASET_LIMIT = 1_000_000

# Define the path to the sample CSV file hosted on S3.
# This dataset contains 2 million rows of synthetic customer data.
path = "https://llm-guide.s3.us-west-2.amazonaws.com/data/ray-data-llm/customers-2000000.csv"

# Load the CSV file into a Ray Dataset.
print("Loading dataset from remote URL...")
ds = ray.data.read_csv(path)

# Limit the dataset. If DATASET_LIMIT > dataset size, the entire dataset will be processed.
print(f"Limiting dataset to {DATASET_LIMIT} rows for initial processing.")
ds_large = ds.limit(DATASET_LIMIT)

# As we increase our compute, we can increase the number of partitions for more parallelism
num_partitions_large = 256
print(f"Repartitioning dataset into {num_partitions_large} blocks for parallelism...")
ds_large = ds_large.repartition(num_blocks=num_partitions_large)

processor_config_large = vLLMEngineProcessorConfig(
    model_source="unsloth/Llama-3.1-8B-Instruct",
    engine_kwargs=dict(
        max_model_len=256,  # Hard cap: system prompt + user prompt + output tokens must fit within this limit
    ),
    batch_size=256,
    accelerator_type="L4",  # Or upgrade to larger GPU
    concurrency=10,  # Deploy 10 workers across 10 GPUs to maximize throughput
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
processor_large = build_llm_processor(
    processor_config_large,
    preprocess=preprocess,
    postprocess=postprocess,
)


# Run the processor on the small dataset.
processed_large = processor_large(ds_large)

# Materialize the dataset to memory.
processed_large = processed_large.materialize()

print(f"\nProcessed {processed_large.count()} rows successfully.")
# Display the first 3 entries to verify the output.
sampled = processed_large.take(3)
print("\n==================GENERATED OUTPUT===============\n")
pprint(sampled)
