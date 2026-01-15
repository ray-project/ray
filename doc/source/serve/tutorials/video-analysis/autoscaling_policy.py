"""
Application-level autoscaling policy for video processing pipeline.

Scaling strategy:
- VideoAnalyzer: scales based on its load (error_ratio = requests / capacity)
- VideoEncoder: scales based on its load, with floor = VideoAnalyzer replicas
- MultiDecoder: 0.5x VideoEncoder replicas

Error ratio formula:
    error_ratio = total_ongoing_requests / (target_per_replica × current_replicas)
    
    - error_ratio > 1.0 → over capacity → scale up
    - error_ratio < 1.0 → under capacity → scale down  
    - error_ratio = 1.0 → at capacity → no change
"""

import math
from typing import Dict, Tuple

from ray.serve._private.common import DeploymentID
from ray.serve.config import AutoscalingContext


def _get_error_ratio(ctx: AutoscalingContext) -> float:
    """
    Calculate error ratio: how much over/under target capacity we are.
    
    Returns 1.0 when idle to maintain current replicas.
    """
    target_per_replica = ctx.config.target_ongoing_requests or 1
    total_requests = ctx.total_num_requests
    current_replicas = ctx.current_num_replicas
    
    if total_requests == 0:
        return 1.0  # Idle: maintain current replicas
    
    total_capacity = target_per_replica * current_replicas
    return total_requests / total_capacity


def _scale_by_error_ratio(ctx: AutoscalingContext, floor: int = 0) -> int:
    """
    Calculate target replicas based on error ratio.
    
    Args:
        ctx: Deployment autoscaling context
        floor: Minimum replicas (in addition to capacity_adjusted_min)
    
    Returns:
        Target replica count, clamped to min/max limits
    """
    error_ratio = _get_error_ratio(ctx)
    
    # Scale current replicas by error ratio
    target = int(math.ceil(ctx.current_num_replicas * error_ratio))
    
    # Apply floor (e.g., encoder should have at least as many as analyzer)
    target = max(target, floor)
    
    # Clamp to configured limits
    return max(
        ctx.capacity_adjusted_min_replicas,
        min(ctx.capacity_adjusted_max_replicas, target),
    )


def _find_deployment(
    contexts: Dict[DeploymentID, AutoscalingContext], 
    name: str,
) -> Tuple[DeploymentID, AutoscalingContext]:
    """Find deployment by name."""
    for dep_id, ctx in contexts.items():
        if dep_id.name == name:
            return dep_id, ctx
    raise KeyError(f"Deployment '{name}' not found")


def coordinated_scaling_policy(
    contexts: Dict[DeploymentID, AutoscalingContext],
) -> Tuple[Dict[DeploymentID, int], Dict]:
    """
    Coordinated scaling for video processing pipeline.
    
    Scaling rules:
        VideoAnalyzer: scale by its own load
        VideoEncoder:  scale by its own load, floor = analyzer replicas
        MultiDecoder:  0.5x encoder replicas
    """
    decisions: Dict[DeploymentID, int] = {}

    # 1. VideoAnalyzer: scale by load
    analyzer_id, analyzer_ctx = _find_deployment(contexts, "VideoAnalyzer")
    analyzer_replicas = _scale_by_error_ratio(analyzer_ctx)
    decisions[analyzer_id] = analyzer_replicas

    # 2. VideoEncoder: scale by load, but at least as many as analyzer
    encoder_id, encoder_ctx = _find_deployment(contexts, "VideoEncoder")
    encoder_replicas = _scale_by_error_ratio(encoder_ctx, floor=analyzer_replicas)
    decisions[encoder_id] = encoder_replicas

    # 3. MultiDecoder: 0.5x encoder replicas
    decoder_id, decoder_ctx = _find_deployment(contexts, "MultiDecoder")
    decoder_replicas = max(1, math.ceil(encoder_replicas / 2))
    decoder_replicas = max(
        decoder_ctx.capacity_adjusted_min_replicas,
        min(decoder_ctx.capacity_adjusted_max_replicas, decoder_replicas),
    )
    decisions[decoder_id] = decoder_replicas

    return decisions, {}
