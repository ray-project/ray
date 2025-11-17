from typing import Any

from pprint import pprint
import ray
from ray.data.llm import vLLMEngineProcessorConfig, build_llm_processor

# Define the path to the sample CSV file hosted on S3.
# This dataset contains 2 million rows of synthetic customer data.
path = "https://llm-guide.s3.us-west-2.amazonaws.com/data/ray-data-llm/customers-2000000.csv"

# Load the CSV file into a Ray Dataset.
print("Loading dataset from remote URL...")
ds = ray.data.read_csv(path)

# Limit the dataset to 10,000 rows for this example.
print("Limiting dataset to 10,000 rows for initial processing.")
ds_small = ds.limit(10_000)

# Repartition the dataset to enable parallelism across multiple workers (GPUs).
# By default, a large remote file might be read into a single block. Repartitioning
# splits the data into a specified number of blocks, allowing Ray to process them
# in parallel.
num_partitions = 64
print(f"Repartitioning dataset into {num_partitions} blocks for parallelism...")
ds_small = ds_small.repartition(num_blocks=num_partitions)


processor_config = vLLMEngineProcessorConfig(
    model_source="unsloth/Llama-3.1-8B-Instruct",
    engine_kwargs=dict(
        max_model_len= 256, # estimate system prompt + user prompt + output tokens (+ reasoning tokens if any)
    ),
    batch_size=256,
    accelerator_type="L4",
    concurrency=4,
)


# Preprocess function prepares `messages` and `sampling_params` for vLLM engine.
# All other fields are ignored by the engine.
def preprocess(row: dict[str, Any]) -> dict[str, Any]:
    return dict(
        messages=[
            {
                "role": "system",
                "content": "You are a helpful assistant that reformats dates to MM-DD-YYYY."
                "Be concise and output only the formatted date and nothing else."
                "For example, if we ask to reformat 'Subscription Date': datetime.date(2020, 11, 29)' then your answer should only be '11-29-2020'",
            },
            {
                "role": "user",
                "content": f"Convert this date:\n{row['Subscription Date']}.",
            },
        ],
        sampling_params=dict(
            temperature=0.3,
            max_tokens=32,  # low max tokens because we are simply formatting a date
            detokenize=False,
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
processor = build_llm_processor(
    processor_config,
    preprocess=preprocess,
    postprocess=postprocess,
)


# Run the processor on the small dataset.
processed_small = processor(ds_small)

# Materialize the dataset to memory.
# You can also use writing APIs such as write_parquet() or write_csv() to persist the dataset.
processed_small = processed_small.materialize()

# Display the first 3 entries to verify the output.
sampled = processed_small.take(3)
print("\n==================GENERATED OUTPUT===============\n")
pprint(sampled)
