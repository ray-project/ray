import os
from typing import Any
from io import BytesIO

from pprint import pprint
import ray
import datasets
from PIL import Image
from ray.data.llm import vLLMEngineProcessorConfig, build_llm_processor

# Load the BLIP3o/BLIP3o-Pretrain-Short-Caption dataset from Hugging Face with ~5M images.
print("Loading BLIP3o/BLIP3o-Pretrain-Short-Caption dataset from Hugging Face...")
hf_dataset = datasets.load_dataset("BLIP3o/BLIP3o-Pretrain-Short-Caption", split="train", streaming=True)
hf_dataset = hf_dataset.select_columns(["jpg"])

ds = ray.data.from_huggingface(hf_dataset)
print("Dataset loaded successfully.")
IMAGE_COLUMN = 'jpg'


# The BLIP3o/BLIP3o-Pretrain-Short-Caption dataset contains ~5M of images.
# Configure how many images to process (default: 1M for demonstration).
dataset_limit = int(os.environ.get("LARGE_DATASET_LIMIT", 1_000_000))
print(f"Processing {dataset_limit:,} images from the dataset...")

# Apply the limit to the dataset.
ds_large = ds.limit(dataset_limit)

# Repartition for better parallelism.
num_partitions_large = 128
print(f"Repartitioning dataset into {num_partitions_large} blocks for parallelism...")
ds_large = ds_large.repartition(num_blocks=num_partitions_large)


processor_config = vLLMEngineProcessorConfig(
    model_source="Qwen/Qwen2.5-VL-3B-Instruct",
    engine_kwargs=dict(
        max_model_len=8192,
        max_num_batched_tokens=2048,
    ),
    batch_size=64,
    accelerator_type="L40S",
    concurrency=10,
    has_image=True,
)


# Preprocess function prepares messages with image content for the VLM.
def preprocess(row: dict[str, Any]) -> dict[str, Any]:
    # Get the image from the row
    image = row.get('image')
    
    # Convert to PIL Image if needed
    if not isinstance(image, Image.Image):
        if isinstance(image, dict) and 'bytes' in image:
            image = Image.open(BytesIO(image['bytes']))
        elif isinstance(image, bytes):
            image = Image.open(BytesIO(image))
    
    # Resize for consistency
    if image:
        image = image.resize((224, 224), Image.Resampling.BICUBIC)
    
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
            max_tokens=150,
            detokenize=False,
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

# Run the same processor on the larger dataset.
processed_large = processor(ds_large)
processed_large = processed_large.materialize()

# Display the first 3 entries to verify the output.
sampled = processed_large.take(3)
print("\n==================GENERATED OUTPUT===============\n")
pprint(sampled)

