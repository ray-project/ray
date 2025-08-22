import os
import shutil

import matplotlib.pyplot as plt
import numpy as np
import torch
from doggos.utils import add_class, url_to_array
from PIL import Image
from scipy.spatial.distance import cdist
from transformers import CLIPModel, CLIPProcessor

import ray


class EmbedImages(object):
    def __init__(self, model_id, device):
        # Load CLIP model and processor
        self.processor = CLIPProcessor.from_pretrained(model_id)
        self.model = CLIPModel.from_pretrained(model_id)
        self.model.to(device)
        self.device = device

    def __call__(self, batch):
        # Load and preprocess images
        images = [
            Image.fromarray(np.uint8(img)).convert("RGB") for img in batch["image"]
        ]
        inputs = self.processor(images=images, return_tensors="pt", padding=True).to(
            self.device
        )

        # Generate embeddings
        with torch.inference_mode():
            batch["embedding"] = self.model.get_image_features(**inputs).cpu().numpy()

        return batch


def get_top_matches(query_embedding, embeddings_ds, class_filters=None, n=4):
    rows = embeddings_ds.take_all()
    if class_filters:
        class_filters = set(class_filters)
        rows = [r for r in rows if r["class"] in class_filters]
    if not rows:
        return []

    # Vectorise
    embeddings = np.vstack([r["embedding"] for r in rows]).astype(np.float32)
    sims = 1 - cdist([query_embedding], embeddings, metric="cosine")[0]

    # Stable top N in NumPy
    k = min(n, sims.size)
    idx = np.argpartition(-sims, k - 1)[:k]
    idx = idx[np.argsort(-sims[idx])]

    # Package results
    return [
        {
            "class": rows[i]["class"],
            "path": rows[i]["path"],
            "similarity": float(sims[i]),
        }
        for i in idx
    ]


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
        EmbedImages,
        fn_constructor_kwargs={
            "model_id": "openai/clip-vit-base-patch32",
            "device": "cuda",
        },  # class kwargs
        fn_kwargs={},
        concurrency=4,
        batch_size=64,
        num_gpus=1,
        accelerator_type="T4",
    )
    embeddings_ds = embeddings_ds.drop_columns(["image"])  # remove image column

    # Save to artifact storage
    embeddings_path = os.path.join("/mnt/user_storage", "doggos/embeddings")
    if os.path.exists(embeddings_path):
        shutil.rmtree(embeddings_path)  # clean up
    embeddings_ds.write_parquet(embeddings_path)
