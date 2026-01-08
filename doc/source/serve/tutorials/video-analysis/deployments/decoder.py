"""MultiDecoder deployment - CPU-based classification, retrieval, and scene detection."""

import io
import logging
import os

import aioboto3
import numpy as np
from ray import serve

from constants import (
    S3_EMBEDDINGS_PREFIX,
    SCENE_CHANGE_THRESHOLD,
    EMA_ALPHA,
)

from utils.s3 import get_s3_region

logger = logging.getLogger(__name__)


@serve.deployment(
    num_replicas="auto",
    ray_actor_options={"num_cpus": 1},
    max_ongoing_requests=4, # can be set higher than 4, but since the encoder is limited to 4, we need to keep it at 4.
    autoscaling_config={
        "min_replicas": 1,
        "max_replicas": 10,
        "target_num_ongoing_requests": 2,
    },
)
class MultiDecoder:
    """
    Decodes video embeddings into tags, captions, and scene changes.
    
    Uses precomputed text embeddings loaded from S3.
    This deployment is stateless - EMA state for scene detection is passed
    in and returned with each call, allowing the caller to maintain state
    continuity across multiple replicas.
    """
    
    async def __init__(self, bucket: str, s3_prefix: str = S3_EMBEDDINGS_PREFIX):
        """Initialize decoder with text embeddings from S3."""
        self.bucket = bucket
        self.ema_alpha = EMA_ALPHA
        self.scene_threshold = SCENE_CHANGE_THRESHOLD
        self.s3_prefix = s3_prefix
        logger.info(f"MultiDecoder initializing (bucket={self.bucket}, ema_alpha={self.ema_alpha}, threshold={self.scene_threshold})")
        
        await self._load_embeddings()
        
        logger.info(f"MultiDecoder ready (tags={len(self.tag_texts)}, descriptions={len(self.desc_texts)})")
    
    async def _load_embeddings(self):
        """Load precomputed text embeddings from S3."""
        session = aioboto3.Session(region_name=get_s3_region(self.bucket))
        
        async with session.client("s3") as s3:
            # Load tag embeddings
            tag_key = f"{self.s3_prefix}tag_embeddings.npz"
            response = await s3.get_object(Bucket=self.bucket, Key=tag_key)
            tag_data = await response["Body"].read()
            tag_npz = np.load(io.BytesIO(tag_data), allow_pickle=True)
            self.tag_embeddings = tag_npz["embeddings"]
            self.tag_texts = tag_npz["texts"].tolist()
            
            # Load description embeddings
            desc_key = f"{self.s3_prefix}description_embeddings.npz"
            response = await s3.get_object(Bucket=self.bucket, Key=desc_key)
            desc_data = await response["Body"].read()
            desc_npz = np.load(io.BytesIO(desc_data), allow_pickle=True)
            self.desc_embeddings = desc_npz["embeddings"]
            self.desc_texts = desc_npz["texts"].tolist()
    
    def _cosine_similarity(self, embedding: np.ndarray, bank: np.ndarray) -> np.ndarray:
        """Compute cosine similarity between embedding and all vectors in bank."""
        return bank @ embedding
    
    def _get_top_tags(self, embedding: np.ndarray, top_k: int = 5) -> list[dict]:
        """Get top-k matching tags with scores."""
        scores = self._cosine_similarity(embedding, self.tag_embeddings)
        top_indices = np.argsort(scores)[::-1][:top_k]
        return [
            {"text": self.tag_texts[i], "score": float(scores[i])}
            for i in top_indices
        ]
    
    def _get_retrieval_caption(self, embedding: np.ndarray) -> dict:
        """Get best matching description."""
        scores = self._cosine_similarity(embedding, self.desc_embeddings)
        best_idx = np.argmax(scores)
        return {
            "text": self.desc_texts[best_idx],
            "score": float(scores[best_idx]),
        }
    
    def _detect_scene_changes(
        self,
        frame_embeddings: np.ndarray,
        chunk_index: int,
        chunk_start_time: float,
        chunk_duration: float,
        ema_state: np.ndarray | None = None,
    ) -> tuple[list[dict], np.ndarray]:
        """
        Detect scene changes using EMA-based scoring.
        
        score_t = 1 - cosine(E_t, ema_t)
        ema_t = α * ema_{t-1} + (1-α) * E_t
        
        Args:
            frame_embeddings: (T, D) normalized embeddings
            chunk_index: Index of this chunk in the video
            chunk_start_time: Start time of chunk in video (seconds)
            chunk_duration: Duration of chunk (seconds)
            ema_state: EMA state from previous chunk, or None for first chunk
        
        Returns:
            Tuple of (scene_changes list, updated ema_state)
        """
        num_frames = len(frame_embeddings)
        if num_frames == 0:
            # Return empty changes and unchanged state (or zeros if no state)
            return [], ema_state if ema_state is not None else np.zeros(0)
        
        # Initialize EMA from first frame if no prior state
        ema = ema_state.copy() if ema_state is not None else frame_embeddings[0].copy()
        scene_changes = []
        
        for frame_idx, embedding in enumerate(frame_embeddings):
            # Compute score: how different is current frame from recent history
            similarity = float(np.dot(embedding, ema))
            score = max(0.0, 1.0 - similarity)
            
            # Detect scene change if score exceeds threshold
            if score >= self.scene_threshold:
                # Calculate timestamp within video
                frame_offset = (frame_idx / max(1, num_frames - 1)) * chunk_duration
                timestamp = chunk_start_time + frame_offset
                
                scene_changes.append({
                    "timestamp": round(timestamp, 3),
                    "score": round(score, 4),
                    "chunk_index": chunk_index,
                    "frame_index": frame_idx,
                })
            
            # Update EMA
            ema = self.ema_alpha * ema + (1 - self.ema_alpha) * embedding
            # Re-normalize
            ema = ema / np.linalg.norm(ema)
        
        return scene_changes, ema
    
    def __call__(
        self,
        encoder_output: dict,
        chunk_index: int,
        chunk_start_time: float,
        chunk_duration: float,
        top_k_tags: int = 5,
        ema_state: np.ndarray | None = None,
    ) -> dict:
        """
        Decode embeddings into tags, caption, and scene changes.
        
        Args:
            encoder_output: Dict with 'frame_embeddings' and 'embedding_dim'
            chunk_index: Index of this chunk in the video
            chunk_start_time: Start time of chunk (seconds)
            chunk_duration: Duration of chunk (seconds)
            top_k_tags: Number of top tags to return
            ema_state: EMA state from previous chunk for scene detection continuity.
                Pass None for the first chunk of a stream.
        
        Returns:
            Dict containing tags, retrieval_caption, scene_changes, and updated ema_state.
            The caller should pass the returned ema_state to the next chunk's call.
        """
        # Get frame embeddings from encoder output
        frame_embeddings = encoder_output["frame_embeddings"]
        
        # Calculate pooled embedding (mean across frames, normalized)
        pooled_embedding = frame_embeddings.mean(axis=0)
        pooled_embedding = pooled_embedding / np.linalg.norm(pooled_embedding)

        # Classification and retrieval on pooled embedding
        tags = self._get_top_tags(pooled_embedding, top_k=top_k_tags)
        caption = self._get_retrieval_caption(pooled_embedding)
        
        # Scene change detection on frame embeddings
        scene_changes, new_ema_state = self._detect_scene_changes(
            frame_embeddings=frame_embeddings,
            chunk_index=chunk_index,
            chunk_start_time=chunk_start_time,
            chunk_duration=chunk_duration,
            ema_state=ema_state,
        )
        
        return {
            "tags": tags,
            "retrieval_caption": caption,
            "scene_changes": scene_changes,
            "ema_state": new_ema_state,
        }
