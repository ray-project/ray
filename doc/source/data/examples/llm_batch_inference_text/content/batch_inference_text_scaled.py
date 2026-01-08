from typing import Any

from pprint import pprint
import ray
from ray.data.llm import build_llm_processor, vLLMEngineProcessorConfig

# Define the path to the sample CSV file hosted on S3.
# This dataset contains 2 million rows of synthetic customer data.
path = "https://llm-guide.s3.us-west-2.amazonaws.com/data/ray-data-llm/customers-2000000.csv"

# Load the CSV file into a Ray Dataset.
print("Loading dataset from remote URL...")
ds = ray.data.read_csv(path)

# The dataset has ~2M rows
# Configure how many images to process (default: 1M for demonstration).
print(f"Processing 1M rows... (or the whole dataset if you picked >2M)")
ds_large = ds.limit(1_000_000)

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

# Preprocess function prepares `messages` and `sampling_params` for vLLM engine.
# All other fields are ignored by the engine.
def preprocess(row: dict[str, Any]) -> dict[str, Any]:
    return dict(
        messages=[
            {
                "role": "system",
                "content": "You are a helpful assistant that reformats dates to MM-DD-YYYY. "
                            "Be concise and output only the formatted date and nothing else. "
                            "For example, if we ask to reformat 'Subscription Date': datetime.date(2020, 11, 29)' then your answer should only be '11-29-2020'"
            },
            {
                "role": "user",
                "content": f"Convert this date:\n{row['Subscription Date']}."
            },
        ],
        sampling_params=dict(
            temperature=0,  # Use 0 for deterministic date formatting
            max_tokens=32,  # Low max tokens because we are simply formatting a date
        ),
    )

# Postprocess function extracts the generated text from the engine output.
# The **row syntax returns all original columns in the input dataset.
def postprocess(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "formatted_date": row["generated_text"],
        **row,  # Include all original columns.
    }

# Build the LLM processor with the configuration and functions.
processor_large = build_llm_processor(
    processor_config_large,
    preprocess=preprocess,
    postprocess=postprocess,
)


# Run the same processor on the larger dataset.
processed_large = processor_large(ds_large)
processed_large = processed_large.materialize()

print(f"\nProcessed {processed_large.count()} rows successfully.")
# Display the first 3 entries to verify the output.
sampled = processed_large.take(3)
print("\n==================GENERATED OUTPUT===============\n")
pprint(sampled)
