from typing import Dict

import numpy as np
import pandas as pd
import torch

from ray.data import read_images
from torchvision import transforms
from transformers import AutoTokenizer


def get_train_dataset(args, image_resolution=512):
    """Build a Dataset for fine-tuning DreamBooth model."""
    # Load a directory of images as a Ray Dataset
    instance_dataset = read_images(args.instance_images_dir)
    class_dataset = read_images(args.class_images_dir)

    # We now duplicate the instance images multiple times to make the
    # two sets contain exactly the same number of images.
    # This is so we can zip them up during training to compute the
    # prior preserving loss in one pass.
    #
    # Example: If we have 200 class images (for regularization) and 4 instance
    # images of our subject, then we'll duplicate the instance images 50 times
    # so that our dataset looks like:
    #
    #     instance_image_0, class_image_0
    #     instance_image_1, class_image_1
    #     instance_image_2, class_image_2
    #     instance_image_3, class_image_3
    #     instance_image_0, class_image_4
    #     instance_image_1, class_image_5
    #     ...
    dup_times = class_dataset.count() // instance_dataset.count()
    instance_dataset = instance_dataset.map_batches(
        lambda df: pd.concat([df] * dup_times), batch_format="pandas"
    )

    # Load tokenizer for tokenizing the image prompts.
    tokenizer = AutoTokenizer.from_pretrained(
        pretrained_model_name_or_path=args.model_dir,
        subfolder="tokenizer",
    )

    def _tokenize(prompt):
        return tokenizer(
            prompt,
            truncation=True,
            padding="max_length",
            max_length=tokenizer.model_max_length,
            return_tensors="pt",
        ).input_ids.numpy()

    # Get the token ids for both prompts.
    class_prompt_ids = _tokenize(args.class_prompt)[0]
    instance_prompt_ids = _tokenize(args.instance_prompt)[0]

    # START: image preprocessing
    transform = transforms.Compose(
        [
            transforms.ToTensor(),
            transforms.Resize(
                image_resolution,
                interpolation=transforms.InterpolationMode.BILINEAR,
                antialias=True,
            ),
            transforms.RandomCrop(image_resolution),
            # use the appropriate mean and std for your dataset
            transforms.Normalize([0.5], [0.5]),
        ]
    )

    def transform_image(
        batch: Dict[str, np.ndarray], output_column_name: str
    ) -> Dict[str, np.ndarray]:
        transformed_tensors = [transform(image).numpy() for image in batch["image"]]
        batch[output_column_name] = transformed_tensors
        return batch

    # END: image preprocessing

    # START: Apply preprocessing steps as Ray Dataset operations
    # For each dataset:
    # - perform image preprocessing
    # - drop the original image column
    # - add a new column with the tokenized prompts
    instance_dataset = (
        instance_dataset.map_batches(
            transform_image, fn_kwargs={"output_column_name": "instance_image"}
        )
        .drop_columns(["image"])
        .add_column(
            "instance_prompt_ids", lambda df: pd.Series([instance_prompt_ids] * len(df))
        )
    )
    # END: Apply preprocessing steps as Ray Dataset operations

    class_dataset = (
        class_dataset.map_batches(
            transform_image, fn_kwargs={"output_column_name": "class_image"}
        )
        .drop_columns(["image"])
        .add_column(
            "class_prompt_ids", lambda df: pd.Series([class_prompt_ids] * len(df))
        )
    )
    # --- Ray Data

    # We may have too many duplicates of the instance images, so limit the
    # dataset size so that len(instance_dataset) == len(class_dataset)
    final_size = min(instance_dataset.count(), class_dataset.count())

    # Now, zip the images up.
    train_dataset = (
        instance_dataset.limit(final_size)
        .repartition(final_size)
        .zip(class_dataset.limit(final_size).repartition(final_size))
    )

    print("Training dataset schema after pre-processing:")
    print(train_dataset.schema())

    return train_dataset.random_shuffle()


def collate(batch, dtype):
    """Build Torch training batch.

    B = batch size
    (C, W, H) = (channels, width, height)
    L = max length in tokens of the text guidance input

    Input batch schema (see `get_train_dataset` on how this was setup):
        instance_images: (B, C, W, H)
        class_images: (B, C, W, H)
        instance_prompt_ids: (B, L)
        class_prompt_ids: (B, L)

    Output batch schema:
        images: (2 * B, C, W, H)
            All instance images in the batch come before the class images:
            [instance_images[0], ..., instance_images[B-1], class_images[0], ...]
        prompt_ids: (2 * B, L)
            Prompt IDs are ordered the same way as the images.

    During training, a batch will be chunked into 2 sub-batches for
    prior preserving loss calculation.
    """

    images = torch.cat([batch["instance_image"], batch["class_image"]], dim=0)
    images = images.to(memory_format=torch.contiguous_format).to(dtype)

    batch_size = len(batch["instance_prompt_ids"])

    prompt_ids = torch.cat(
        [batch["instance_prompt_ids"], batch["class_prompt_ids"]], dim=0
    ).reshape(batch_size * 2, -1)

    return {
        "images": images,
        "prompt_ids": prompt_ids,  # token ids should stay int.
    }
