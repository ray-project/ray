#!/usr/bin/env python3
"""
Ray job to generate SigLIP text embeddings for tags and descriptions.

Usage:
    ray job submit --working-dir . -- python jobs/generate_text_embeddings.py --bucket my-bucket
"""

import argparse
import asyncio
import io
import json
import time

import aioboto3
import numpy as np
import ray
import torch
from transformers import AutoModel, AutoProcessor

from constants import MODEL_NAME, S3_EMBEDDINGS_PREFIX
from textbanks import TAGS, DESCRIPTIONS

from utils.s3 import get_s3_region


def load_textbanks() -> tuple[list[str], list[str]]:
    """Load tags and descriptions from textbanks module."""
    return TAGS, DESCRIPTIONS


def compute_text_embeddings(
    texts: list[str],
    processor,
    model,
    device: str,
    batch_size: int = 32,
) -> np.ndarray:
    """Compute normalized text embeddings using SigLIP."""
    all_embeddings = []
    
    for i in range(0, len(texts), batch_size):
        batch_texts = texts[i:i + batch_size]
        
        # Process text
        inputs = processor(
            text=batch_texts,
            padding="max_length",
            truncation=True,
            return_tensors="pt",
        )
        inputs = {k: v.to(device) for k, v in inputs.items()}
        
        # Get embeddings
        with torch.no_grad():
            outputs = model.get_text_features(**inputs)
        
        # L2 normalize
        embeddings = outputs.cpu().numpy()
        embeddings = embeddings / np.linalg.norm(embeddings, axis=1, keepdims=True)
        all_embeddings.append(embeddings)
    
    return np.vstack(all_embeddings).astype(np.float32)


async def save_to_s3(
    session: aioboto3.Session,
    embeddings: np.ndarray,
    texts: list[str],
    bucket: str,
    key: str,
) -> str:
    """Save embeddings and texts to S3 as npz file."""
    # Save as npz (embeddings + texts)
    buffer = io.BytesIO()
    np.savez_compressed(
        buffer,
        embeddings=embeddings,
        texts=np.array(texts, dtype=object),
    )
    buffer.seek(0)
    
    async with session.client("s3") as s3:
        await s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=buffer.getvalue(),
            ContentType="application/octet-stream",
        )
    
    return f"s3://{bucket}/{key}"


async def generate_and_upload(
    bucket: str,
    s3_prefix: str = S3_EMBEDDINGS_PREFIX,
) -> dict:
    """Generate embeddings and upload to S3."""
    print("=" * 60)
    print("Starting text embedding generation")
    print("=" * 60)
    
    # Load textbanks
    print("\n📚 Loading text banks...")
    tags, descriptions = load_textbanks()
    print(f"   Tags: {len(tags)}")
    print(f"   Descriptions: {len(descriptions)}")
    
    # Load model
    print(f"\n🤖 Loading SigLIP model: {MODEL_NAME}")
    device = "cuda" if torch.cuda.is_available() else "cpu"
    print(f"   Device: {device}")
    
    start = time.time()
    processor = AutoProcessor.from_pretrained(MODEL_NAME)
    model = AutoModel.from_pretrained(MODEL_NAME).to(device)
    model.eval()
    load_time = time.time() - start
    print(f"   Model loaded in {load_time:.1f}s")
    
    # Generate tag embeddings
    print("\n🏷️  Generating tag embeddings...")
    start = time.time()
    tag_embeddings = compute_text_embeddings(tags, processor, model, device)
    tag_time = time.time() - start
    print(f"   Shape: {tag_embeddings.shape}")
    print(f"   Time: {tag_time:.2f}s")
    
    # Generate description embeddings
    print("\n📝 Generating description embeddings...")
    start = time.time()
    desc_embeddings = compute_text_embeddings(descriptions, processor, model, device)
    desc_time = time.time() - start
    print(f"   Shape: {desc_embeddings.shape}")
    print(f"   Time: {desc_time:.2f}s")
    
    # Upload to S3 concurrently
    print(f"\n☁️  Uploading to S3 bucket: {bucket}")
    
    session = aioboto3.Session(region_name=get_s3_region(bucket))
    
    tag_uri, desc_uri = await asyncio.gather(
        save_to_s3(session, tag_embeddings, tags, bucket, f"{s3_prefix}tag_embeddings.npz"),
        save_to_s3(session, desc_embeddings, descriptions, bucket, f"{s3_prefix}description_embeddings.npz"),
    )
    
    print(f"   Tags: {tag_uri}")
    print(f"   Descriptions: {desc_uri}")
    print("\n✅ Done!")
    
    return {
        "tag_embeddings": {
            "s3_uri": tag_uri,
            "shape": list(tag_embeddings.shape),
            "count": len(tags),
        },
        "description_embeddings": {
            "s3_uri": desc_uri,
            "shape": list(desc_embeddings.shape),
            "count": len(descriptions),
        },
        "model": MODEL_NAME,
        "timing": {
            "model_load_s": load_time,
            "tag_embed_s": tag_time,
            "desc_embed_s": desc_time,
        },
    }


@ray.remote(num_gpus=1)
def generate_embeddings_task(bucket: str, s3_prefix: str = S3_EMBEDDINGS_PREFIX) -> dict:
    """Ray task wrapper for async embedding generation."""
    return asyncio.run(generate_and_upload(bucket, s3_prefix))


def main():
    parser = argparse.ArgumentParser(description="Generate text embeddings for decoder")
    parser.add_argument("--bucket", type=str, required=True, help="S3 bucket name")
    args = parser.parse_args()
    
    # Run the task on existing Ray cluster
    result = ray.get(generate_embeddings_task.remote(args.bucket))
    
    print("\n" + "=" * 60)
    print("Results:")
    print("=" * 60)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
