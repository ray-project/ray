import os
import shutil

import matplotlib.pyplot as plt
import numpy as np
import ray
import torch
from PIL import Image
from scipy.spatial.distance import cdist
from transformers import CLIPModel, CLIPProcessor

from doggos.utils import add_class, url_to_array


class EmbeddingGenerator(object):
    def __init__(self, model_id):
        # Load CLIP model and processor
        self.model = CLIPModel.from_pretrained(model_id)
        self.processor = CLIPProcessor.from_pretrained(model_id)

    def __call__(self, batch, device="cpu"):
        # Load and preprocess images
        images = [
            Image.fromarray(np.uint8(img)).convert("RGB") for img in batch["image"]
        ]
        inputs = self.processor(images=images, return_tensors="pt", padding=True).to(
            device
        )

        # Generate embeddings
        self.model.to(device)
        with torch.inference_mode():
            batch["embedding"] = self.model.get_image_features(**inputs).cpu().numpy()

        return batch


def get_top_matches(query_embedding, embeddings_ds, class_filters=[], n=4):
    # Filter dataset (if needed)
    if class_filters:
        embeddings_ds = embeddings_ds.filter(lambda row: row["class"] in class_filters)

    # Compute cosine similarities in batches
    def compute_similarities(batch):
        embeddings = np.stack(batch["embedding"])
        similarities = (
            1 - cdist([query_embedding], embeddings, metric="cosine").flatten()
        )
        return {
            "class": batch["class"],
            "path": batch["path"],
            "similarity": similarities.tolist(),
        }

    # Apply map_batches to compute similarities
    similarities_ds = embeddings_ds.map_batches(
        compute_similarities,
        concurrency=4,
        batch_size=128,
        num_gpus=1,
    )
    top_matches = similarities_ds.sort("similarity", descending=True).take(n)
    return top_matches


def display_top_matches(url, matches):
    fig, axes = plt.subplots(1, len(matches) + 1, figsize=(15, 5))

    # Display query image
    axes[0].imshow(url_to_array(url=url))
    axes[0].axis("off")
    axes[0].set_title("Query image")

    # Display matches
    for i, match in enumerate(matches):
        bucket = match["path"].split("/")[0]
        key = "/".join(match["path"].split("/")[1:])
        url = f"https://{bucket}.s3.us-west-2.amazonaws.com/{key}"
        image = url_to_array(url=url)
        axes[i + 1].imshow(image)
        axes[i + 1].axis("off")
        axes[i + 1].set_title(f"{match['class']} ({match['similarity']:.2f})")

    plt.tight_layout()
    plt.show()


if __name__ == "__main__":

    # Load data
    ds = ray.data.read_images(
        "s3://doggos-dataset/train",
        include_paths=True,
        shuffle="files",
    )
    ds = ds.map(add_class)

    # Batch embedding generation
    embeddings_ds = ds.map_batches(
        EmbeddingGenerator,
        fn_constructor_kwargs={"model_id": "openai/clip-vit-base-patch32"},
        fn_kwargs={"device": "cuda"},
        concurrency=4,
        batch_size=64,
        num_gpus=1,
        accelerator_type="L4",
    )
    embeddings_ds = embeddings_ds.drop_columns(["image"])  # remove image column

    # Save to artifact storage
    embeddings_path = os.path.join("/mnt/user_storage", "doggos/embeddings")
    if os.path.exists(embeddings_path):
        shutil.rmtree(embeddings_path)  # clean up
    embeddings_ds.write_parquet(embeddings_path)
