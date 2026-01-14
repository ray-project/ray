from io import BytesIO
from typing import Any

import datasets
from PIL import Image
from pprint import pprint
import ray
from ray.data.llm import build_llm_processor, vLLMEngineProcessorConfig

DATASET_LIMIT = 10_000

# Load the BLIP3o/BLIP3o-Pretrain-Short-Caption dataset from Hugging Face with ~5M images.
print("Loading BLIP3o/BLIP3o-Pretrain-Short-Caption dataset from Hugging Face...")
hf_dataset = datasets.load_dataset("BLIP3o/BLIP3o-Pretrain-Short-Caption", split="train", streaming=True)
hf_dataset = hf_dataset.select_columns(["jpg"])

ds = ray.data.from_huggingface(hf_dataset)
print("Dataset loaded successfully.")

# Limit the dataset. If DATASET_LIMIT > dataset size, the entire dataset will be processed.
print(f"Limiting dataset to {DATASET_LIMIT} images for initial processing.")
ds_small = ds.limit(DATASET_LIMIT)

# Repartition the dataset to enable parallelism across multiple workers (GPUs).
# By default, streaming datasets might not be optimally partitioned. Repartitioning
# splits the data into a specified number of blocks, allowing Ray to process them
# in parallel.
# Tip: Repartition count should typically be 2-4x your worker (GPU) count.
# Example: 4 GPUs → 8-16 partitions, 10 GPUs → 20-40 partitions.
# This ensures enough parallelism while avoiding excessive overhead.
num_partitions = 64
print(f"Repartitioning dataset into {num_partitions} blocks for parallelism...")
ds_small = ds_small.repartition(num_blocks=num_partitions)


processor_config = vLLMEngineProcessorConfig(
    model_source="Qwen/Qwen2.5-VL-3B-Instruct",
    engine_kwargs=dict(
        max_model_len=8192
    ),
    batch_size=16,
    accelerator_type="L4",
    concurrency=4,
    has_image=True,  # Enable image input.
)


# Filter function to validate images before processing.
# Returns True for valid images, False for corrupt/malformed ones.
def is_valid_image(row: dict[str, Any]) -> bool:
    try:
        Image.open(BytesIO(row['jpg']['bytes']))
        return True
    except Exception:
        return False

# Preprocess function prepares messages with image content for the VLM.
def preprocess(row: dict[str, Any]) -> dict[str, Any]:
    # Convert bytes image to PIL 
    image = row['jpg']['bytes']
    image = Image.open(BytesIO(image))
    # Resize to 225x225 for consistency and predictable vision-token budget.
    # This resolution balances quality with memory usage. Adjust based on your
    # model's expected input size and available GPU memory.
    image = image.resize((225, 225), Image.Resampling.BICUBIC)
    
    return dict(
        messages=[
            {
                "role": "system",
                "content": "You are a helpful assistant that generates accurate and descriptive captions for images."
            },
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": "Describe this image in detail. Focus on the main subjects, actions, and setting."
                    },
                    {
                        "type": "image",
                        "image": image  # Ray Data accepts PIL Image or image URL.
                    }
                ]
            },
        ],
        sampling_params=dict(
            temperature=0.3,
            max_tokens=256
        ),
    )

# Postprocess function extracts the generated caption.
def postprocess(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "generated_caption": row["generated_text"],
        # Note: Don't include **row here to avoid returning the large image data.
        # Include only the fields you need in the output.
    }

# Build the LLM processor with the configuration and functions.
processor = build_llm_processor(
    processor_config,
    preprocess=preprocess,
    postprocess=postprocess,
)

# Filter out invalid images before processing.
ds_small_filtered = ds_small.filter(is_valid_image)

# Run the processor on the filtered dataset.
processed_small = processor(ds_small_filtered)

# Materialize the dataset to memory.
processed_small = processed_small.materialize()

print(f"\nProcessed {processed_small.count()} rows successfully.")
# Display the first 3 entries to verify the output.
sampled = processed_small.take(3)
print("\n==================GENERATED OUTPUT===============\n")
pprint(sampled)
