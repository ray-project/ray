"""
This file serves as a documentation example and CI test for VLM batch inference with audio.

Structure:
1. Infrastructure setup: Dataset compatibility patches, dependency handling
2. Docs example (between __vlm_audio_example_start/end__): Embedded in Sphinx docs via literalinclude
3. Test validation and cleanup
"""


'''
# __audio_message_format_example_start__
"""Supported audio input formats: audio URL, audio binary data"""
{
    "messages": [
        {
            "role": "system",
            "content": "Provide a detailed description of the audio."
        },
        {
            "role": "user",
            "content": [
                {"type": "text", "text": "Describe what happens in this audio."},
                # Option 1: Provide audio URL
                {"type": "audio_url", "audio_url": {"url": "https://example.com/audio.wav"}},
                # Option 2: Provide audio binary data
                {"type": "input_audio", "input_audio": {"data": audio_base64, "format": "wav"}},
            ]
        },
    ]
}
# __audio_message_format_example_end__
'''

# __omni_audio_example_start__
import ray
from ray.data.llm import (
    vLLMEngineProcessorConfig,
    build_processor,
)

# __omni_audio_config_example_start__
audio_processor_config = vLLMEngineProcessorConfig(
    model_source="Qwen/Qwen2.5-Omni-3B",
    task_type="generate",
    engine_kwargs=dict(
        limit_mm_per_prompt={"audio": 1},
    ),
    batch_size=16,
    accelerator_type="L4",
    concurrency=1,
    prepare_multimodal_stage={
        "enabled": True,
        "chat_template_content_format": "openai",
    },
    chat_template_stage={"enabled": True},
    tokenize_stage={"enabled": True},
    detokenize_stage={"enabled": True},
)
# __omni_audio_config_example_end__


# __omni_audio_preprocess_example_start__
def audio_preprocess(row: dict) -> dict:
    """
    Preprocessing function for audio-language model inputs.

    Converts dataset rows into the format expected by the Omni model:
    - System prompt for analysis instructions
    - User message with text and audio content
    - Sampling parameters
    """
    return {
        "messages": [
            {
                "role": "system",
                "content": "You are a helpful assistant that analyzes audio. "
                "Listen to the audio carefully and provide detailed descriptions.",
            },
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": row["text"],
                    },
                    {
                        "type": "input_audio",
                        "input_audio": {
                            "data": row["audio_data"],
                            "format": "wav",
                        },
                    },
                ],
            },
        ],
        "sampling_params": {
            "temperature": 0.3,
            "max_tokens": 150,
            "detokenize": False,
        },
    }


def audio_postprocess(row: dict) -> dict:
    return {
        "resp": row["generated_text"],
    }


# __omni_audio_preprocess_example_end__


def load_audio_dataset():
    """
    Load audio dataset from MRSAudio Hugging Face dataset.
    """
    try:
        from datasets import load_dataset
        from huggingface_hub import hf_hub_download
        import base64

        dataset_name = "MRSAudio/MRSAudio"

        dataset = load_dataset(dataset_name, split="train")

        audio_items = []

        # Limit to first 10 samples for the example
        num_samples = min(10, len(dataset))
        for i in range(num_samples):
            item = dataset[i]

            audio_path = hf_hub_download(
                repo_id=dataset_name, filename=item["path"], repo_type="dataset"
            )

            with open(audio_path, "rb") as f:
                audio_bytes = f.read()

            audio_base64 = base64.b64encode(audio_bytes).decode("utf-8")
            audio_items.append(
                {
                    "audio_data": audio_base64,
                    "text": item.get("text", "Describe this audio."),
                }
            )

        audio_dataset = ray.data.from_items(audio_items)
        return audio_dataset
    except Exception as e:
        print(f"Error loading dataset: {e}")
        return None


def create_omni_audio_config():
    """Create Omni audio configuration."""
    return vLLMEngineProcessorConfig(
        model_source="Qwen/Qwen2.5-Omni-3B",
        task_type="generate",
        engine_kwargs=dict(
            enforce_eager=True,
            limit_mm_per_prompt={"audio": 1},
        ),
        batch_size=16,
        accelerator_type="L4",
        concurrency=1,
        prepare_multimodal_stage={
            "enabled": True,
            "chat_template_content_format": "openai",
        },
        chat_template_stage={"enabled": True},
        tokenize_stage={"enabled": True},
        detokenize_stage={"enabled": True},
    )


def run_omni_audio_example():
    """Run the complete Omni audio example workflow."""
    config = create_omni_audio_config()
    audio_dataset = load_audio_dataset()

    if audio_dataset:
        # Build processor with preprocessing and postprocessing
        processor = build_processor(
            config, preprocess=audio_preprocess, postprocess=audio_postprocess
        )

        print("Omni audio processor configured successfully")
        print(f"Model: {config.model_source}")
        print(f"Has multimodal support: {config.prepare_multimodal_stage.get('enabled', False)}")
        result = processor(audio_dataset).take_all()
        return config, processor, result
    # __omni_audio_run_example_end__
    return None, None, None


# __omni_audio_example_end__

if __name__ == "__main__":
    # Run the example Omni audio workflow only if GPU is available
    try:
        import torch

        if torch.cuda.is_available():
            run_omni_audio_example()
        else:
            print("Skipping Omni audio example run (no GPU available)")
    except Exception as e:
        print(f"Skipping Omni audio example run due to environment error: {e}")
