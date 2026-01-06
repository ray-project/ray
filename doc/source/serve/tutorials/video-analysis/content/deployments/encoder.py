"""VideoEncoder deployment - GPU-based frame encoding using SigLIP."""

import asyncio
import logging
from typing import List

import numpy as np
import torch
from ray import serve
from transformers import AutoModel, AutoProcessor

from constants import MODEL_NAME
from utils.video import frames_to_pil_list

logger = logging.getLogger(__name__)


@serve.deployment(
    num_replicas="auto",
    ray_actor_options={"num_gpus": 1, "num_cpus": 2},
    # GPU utilization is at 100% when this is set to 2. with L4
    # aka number on ongoing chunks that can be processed at once.
    max_ongoing_requests=2,
    autoscaling_config={
        "min_replicas": 1,
        "max_replicas": 10,
        "target_num_ongoing_requests": 2,
    },
)
class VideoEncoder:
    """
    Encodes video frames into embeddings using SigLIP.
    
    Returns both per-frame embeddings and pooled embedding.
    """
    
    def __init__(self):
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        print(f"VideoEncoder initializing on {self.device}")
        
        # Load SigLIP model and processor
        self.processor = AutoProcessor.from_pretrained(MODEL_NAME)
        self.model = AutoModel.from_pretrained(MODEL_NAME).to(self.device)
        self.model.eval()
        
        # Get embedding dimension
        self.embedding_dim = self.model.config.vision_config.hidden_size
        
        print(f"VideoEncoder ready (embedding_dim={self.embedding_dim})")
    
    def encode_frames(self, frames: np.ndarray) -> np.ndarray:
        """
        Encode frames and return per-frame embeddings.
        
        Args:
            frames: np.ndarray of shape (T, H, W, 3) uint8 RGB
        
        Returns:
            np.ndarray of shape (T, D) float32, L2-normalized per-frame embeddings
        """
        
        # Convert to PIL images
        pil_images = frames_to_pil_list(frames)
        
        # Process images
        inputs = self.processor(images=pil_images, return_tensors="pt").to(self.device)

        # Get embeddings
        with torch.no_grad():
            with torch.amp.autocast(device_type=self.device, enabled=self.device == "cuda"):
                outputs = self.model.get_image_features(**inputs)

                # L2 normalize on GPU (faster than CPU numpy)
                frame_embeddings = torch.nn.functional.normalize(outputs, p=2, dim=1)
        
        # Move to CPU and convert to numpy
        result = frame_embeddings.cpu().numpy().astype(np.float32)
        return result
    
    async def encode_unbatched(self, frames: np.ndarray) -> dict:
        """
        Unbatched entry point - processes single request directly.
        
        Args:
            frames: np.ndarray of shape (T, H, W, 3)
        
        Returns:
            dict with 'frame_embeddings' and 'embedding_dim'
        """
        print(f"Unbatched: {frames.shape[0]} frames")
        
        frame_embeddings = await asyncio.to_thread(self.encode_frames, frames)
        
        return {
            "frame_embeddings": frame_embeddings,
            "embedding_dim": self.embedding_dim,
        }
    
    @serve.batch(max_batch_size=2, batch_wait_timeout_s=0.1)
    async def encode_batched(self, frames_batch: List[np.ndarray]) -> List[dict]:
        """
        Batched entry point - collects multiple requests into single GPU call.
        
        Args:
            frames_batch: List of frame arrays, each of shape (T, H, W, 3)
        
        Returns:
            List of dicts, each with 'frame_embeddings' and 'embedding_dim'
        """
        frame_counts = [f.shape[0] for f in frames_batch]
        total_frames = sum(frame_counts)
        
        print(f"Batched: {len(frames_batch)} requests ({total_frames} total frames)")
        
        # Concatenate all frames into single batch
        all_frames = np.concatenate(frames_batch, axis=0)
        
        # Single forward pass for all frames
        all_embeddings = await asyncio.to_thread(self.encode_frames, all_frames)
        
        # Split results back per request
        results = []
        offset = 0
        for n_frames in frame_counts:
            chunk_embeddings = all_embeddings[offset:offset + n_frames]
            results.append({
                "frame_embeddings": chunk_embeddings,
                "embedding_dim": self.embedding_dim,
            })
            offset += n_frames
        
        return results
    
    async def __call__(self, frames: np.ndarray, use_batching: bool = False) -> dict:
        """
        Main entry point. Set use_batching=False for direct comparison.
        """
        if use_batching:
            return await self.encode_batched(frames)
        else:
            return await self.encode_unbatched(frames)
