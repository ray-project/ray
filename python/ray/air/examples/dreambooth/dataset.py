import pandas as pd
import torch

from ray.data import read_images
from ray.data.preprocessors import TorchVisionPreprocessor
from torchvision import transforms
from transformers import AutoTokenizer


def get_train_dataset(args, image_resolution=512):
    """Build a Datastream for fine-tuning DreamBooth model."""
    # Load images into Datastream
    instance_dataset = read_images(args.instance_images_dir)
    class_dataset = read_images(args.class_images_dir)

    # We now duplicate the instance images multiple times to make the
    # two sets contain exactly the same number of images.
    # This is so we can zip them up during training to compute the
    # prior preserving loss in one pass.
    dup_times = class_dataset.count() // instance_dataset.count()
    instance_dataset = instance_dataset.map_batches(
        lambda df: pd.concat([df] * dup_times)
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

    # Image preprocessing.
    # Instance and class images used by this example are in sizes 700x700
    # and 512x512 respectively.
    # Depending on the sizes of actual training images, there may need to be a
    # transforms.Resize() step as well.
    transform = transforms.Compose(
        [
            transforms.ToTensor(),
            transforms.RandomCrop(image_resolution),
            transforms.Normalize([0.5], [0.5]),
        ]
    )
    preprocessor = TorchVisionPreprocessor(["image"], transform=transform)

    instance_dataset = preprocessor.transform(instance_dataset).add_column(
        "prompt_ids", lambda df: [instance_prompt_ids] * len(df)
    )
    class_dataset = preprocessor.transform(class_dataset).add_column(
        "prompt_ids", lambda df: [class_prompt_ids] * len(df)
    )
    # ---

    # Now, zip the images up.
    final_size = min(instance_dataset.count(), class_dataset.count())
    train_dataset = (
        instance_dataset.limit(final_size)
        .repartition(final_size)
        .zip(class_dataset.limit(final_size).repartition(final_size))
    )

    return train_dataset.random_shuffle()


def collate(batch, device, dtype):
    """Build Torch training batch."""
    # Layout of the batch is that instance image data (pixels, prompt ids) occupy
    # the top half of the batch. And class image data occupy the bottom half
    # of the batch.
    # During training, a batch will be chunked into 2 sub-batches for prior
    # preserving loss calculation.

    # batch["image"] = image1, image2
    # batch["image_1"] = reg1, reg2
    # After cat, we will have [image1, reg1, image2, reg]

    images = torch.cat([batch["image"], batch["image_1"]], dim=0)
    images = images.to(memory_format=torch.contiguous_format).float()

    batch_size = len(batch["prompt_ids"])

    # batch["prompt_ids"] = pr1, pr2
    # batch["prompt_ids_1"] = rr1, rr2
    # After stack+reshape, we will have [pr1, rr1, pr2, rr2]

    prompt_ids = torch.stack(
        [batch["prompt_ids"], batch["prompt_ids_1"]], dim=1
    ).reshape(batch_size * 2, -1)

    return {
        "prompt_ids": prompt_ids.to(device),  # token ids should stay int.
        "images": images.to(device, dtype=dtype),
    }
