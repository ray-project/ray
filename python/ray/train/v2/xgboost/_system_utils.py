"""
System Detection Utilities for XGBoost Training

This module contains utilities for detecting and analyzing system characteristics
to optimize XGBoost external memory training performance.

Key components:
- _detect_numa_configuration: NUMA topology detection and recommendations
- _get_storage_performance_info: Storage type and performance analysis
- _get_node_memory_limit_gb: Ray cluster memory capacity detection
- _estimate_dataset_memory_usage: Dataset memory footprint estimation
"""

import logging
import subprocess
from typing import Any, Dict

logger = logging.getLogger(__name__)


def _detect_numa_configuration() -> Dict[str, Any]:
    """Detect NUMA configuration and provide optimization recommendations.

    This function analyzes the system's NUMA topology and provides recommendations
    for optimal external memory performance on multi-socket systems.

    Returns:
        Dictionary containing NUMA configuration info and recommendations
    """
    numa_info = {
        "numa_nodes_detected": 0,
        "gpu_numa_mapping": {},
        "recommendations": [],
        "optimal_affinity_commands": [],
        "performance_impact": "unknown",
    }

    try:
        # Try to detect NUMA nodes
        result = subprocess.run(
            ["numactl", "--hardware"], capture_output=True, text=True, timeout=5
        )
        if result.returncode == 0:
            # Parse numactl output for node count
            lines = result.stdout.split("\n")
            for line in lines:
                if "available:" in line and "nodes" in line:
                    # Extract number like "available: 2 nodes (0-1)"
                    parts = line.split()
                    for i, part in enumerate(parts):
                        if part.isdigit():
                            numa_info["numa_nodes_detected"] = int(part)
                            break

        # Try to get GPU NUMA mapping via nvidia-smi
        try:
            result = subprocess.run(
                ["nvidia-smi", "topo", "-m"], capture_output=True, text=True, timeout=10
            )
            if result.returncode == 0:
                lines = result.stdout.split("\n")
                for line in lines:
                    if line.startswith("GPU") and "NUMA Affinity" in result.stdout:
                        # Parse GPU to NUMA mapping
                        parts = line.split()
                        if len(parts) >= 2:
                            gpu_id = parts[0]  # GPU0, GPU1, etc.
                            # Find NUMA Affinity column
                            headers = None
                            for header_line in lines:
                                if "NUMA Affinity" in header_line:
                                    headers = header_line.split()
                                    break
                            if headers and "NUMA" in headers:
                                numa_col_idx = None
                                for i, header in enumerate(headers):
                                    if "NUMA" in header and "Affinity" in header:
                                        numa_col_idx = i
                                        break
                                if numa_col_idx and len(parts) > numa_col_idx:
                                    numa_node = parts[numa_col_idx]
                                    numa_info["gpu_numa_mapping"][gpu_id] = numa_node
        except (subprocess.TimeoutExpired, subprocess.CalledProcessError):
            pass

    except (
        subprocess.TimeoutExpired,
        subprocess.CalledProcessError,
        FileNotFoundError,
    ):
        # numactl not available or failed
        numa_info["recommendations"].append(
            "NUMA tools not available. Install numactl for multi-socket optimization."
        )

    # Generate recommendations
    if numa_info["numa_nodes_detected"] > 1:
        numa_info["performance_impact"] = "high"
        numa_info["recommendations"].extend(
            [
                f"Multi-socket system detected ({numa_info['numa_nodes_detected']} NUMA nodes)",
                "Incorrect NUMA affinity can reduce bandwidth by 50% for external memory training",
                "Use numactl for optimal performance on multi-socket systems",
            ]
        )

        # Generate specific commands
        for gpu_id, numa_node in numa_info["gpu_numa_mapping"].items():
            cmd = f"numactl --membind={numa_node} --cpunodebind={numa_node} python train.py"
            numa_info["optimal_affinity_commands"].append(f"{gpu_id}: {cmd}")
            numa_info["recommendations"].append(
                f"For {gpu_id}: bind to NUMA node {numa_node}"
            )

        if not numa_info["gpu_numa_mapping"]:
            numa_info["recommendations"].extend(
                [
                    "Run 'nvidia-smi topo -m' to check GPU NUMA affinity",
                    "Example: numactl --membind=0 --cpunodebind=0 python train.py",
                ]
            )

    elif numa_info["numa_nodes_detected"] == 1:
        numa_info["performance_impact"] = "low"
        numa_info["recommendations"].append(
            "Single NUMA node detected - no affinity configuration needed"
        )

    return numa_info


def _get_storage_performance_info() -> Dict[str, Any]:
    """Detect storage configuration and provide performance recommendations.

    Analyzes the storage setup and provides guidance for external memory training
    based on storage type and performance characteristics.

    .. note::
        This function currently relies on Linux-specific commands (``df``, ``findmnt``)
        and may not work on other operating systems.

    Returns:
        Dictionary with storage info and performance recommendations
    """
    storage_info = {
        "storage_type": "unknown",
        "estimated_bandwidth_gbps": 0,
        "recommended_batch_size": 10000,
        "performance_rating": "unknown",
        "recommendations": [],
    }

    try:
        import os

        # Get filesystem info for current directory (where cache will be stored)
        result = subprocess.run(["df", "-T", "."], capture_output=True, text=True)
        if result.returncode == 0:
            lines = result.stdout.split("\n")
            if len(lines) > 1:
                parts = lines[1].split()
                if len(parts) > 1:
                    filesystem = parts[1].lower()

                    # Try to determine storage type from filesystem and mount info
                    if "tmpfs" in filesystem:
                        storage_info["storage_type"] = "memory"
                        storage_info["estimated_bandwidth_gbps"] = 50
                        storage_info["performance_rating"] = "excellent"
                    elif "nfs" in filesystem or "cifs" in filesystem:
                        storage_info["storage_type"] = "network"
                        storage_info["estimated_bandwidth_gbps"] = 1
                        storage_info["performance_rating"] = "poor"

        # Try to detect NVMe vs SATA from /proc/mounts and /sys
        try:
            # Check if we're on an NVMe device
            cwd = os.getcwd()
            result = subprocess.run(
                ["findmnt", "-T", cwd], capture_output=True, text=True
            )
            if result.returncode == 0 and "nvme" in result.stdout.lower():
                storage_info["storage_type"] = "nvme"
                storage_info["estimated_bandwidth_gbps"] = 6  # Typical PCIe 4.0 NVMe
                storage_info["performance_rating"] = "excellent"
            elif "ssd" in result.stdout.lower() or "solid" in result.stdout.lower():
                storage_info["storage_type"] = "ssd"
                storage_info["estimated_bandwidth_gbps"] = 3  # Typical SATA SSD
                storage_info["performance_rating"] = "good"
        except subprocess.CalledProcessError:
            pass

    except (subprocess.CalledProcessError, FileNotFoundError):
        storage_info["recommendations"].append("Could not detect storage configuration")

    # Generate recommendations based on detected storage
    if storage_info["storage_type"] == "nvme":
        storage_info["recommendations"].extend(
            [
                "NVMe SSD detected - excellent for external memory training",
                "Recommended batch size: 10,000-50,000 rows per batch",
                "Expected performance: ~6GB/s, practical for large datasets",
            ]
        )
        storage_info["recommended_batch_size"] = 25000

    elif storage_info["storage_type"] == "ssd":
        storage_info["recommendations"].extend(
            [
                "SATA SSD detected - good for external memory training",
                "Recommended batch size: 5,000-25,000 rows per batch",
                "Expected performance: ~3GB/s, suitable for moderate datasets",
            ]
        )
        storage_info["recommended_batch_size"] = 15000

    elif storage_info["storage_type"] == "network":
        storage_info["recommendations"].extend(
            [
                "Network storage detected - not recommended for external memory",
                "Consider local SSD/NVMe for cache storage",
                "Performance will be severely limited by network latency",
            ]
        )
        storage_info["recommended_batch_size"] = 5000

    elif storage_info["storage_type"] == "memory":
        storage_info["recommendations"].extend(
            [
                "Memory filesystem detected - excellent performance",
                "Warning: Cache files will be lost on restart",
                "Consider persistent storage for long training sessions",
            ]
        )
        storage_info["recommended_batch_size"] = 50000

    else:
        storage_info["recommendations"].extend(
            [
                "Unknown storage type - use NVMe SSD for optimal performance",
                "External memory training is I/O bound",
                "Recommended: ≥6GB/s storage bandwidth for practical training",
            ]
        )

    return storage_info


def _get_node_memory_limit_gb() -> float:
    """Get the memory limit per worker node in the Ray cluster.

    This function calculates the average memory available per worker node,
    excluding head nodes which may have different resource allocations.

    In autoscaling scenarios where no worker nodes are currently available,
    falls back to a conservative 8GB default.

    Returns:
        Memory limit in GB per worker node. Defaults to 8GB if cluster info
        unavailable or in autoscaling scenarios with no active worker nodes.
    """
    import ray

    try:
        # Initialize Ray if not already initialized
        ray.init(ignore_reinit_error=True)

        # Get cluster resources and node information
        cluster_resources = ray.cluster_resources()

        # Try to get more accurate node information
        try:
            # Get nodes information for more accurate calculation
            nodes = ray.nodes()
            # Filter to only include worker nodes (exclude head nodes)
            worker_nodes = [
                node
                for node in nodes
                if node["Alive"] and "node:__internal_head__" not in node["Resources"]
            ]

            if worker_nodes:
                # Calculate average memory per worker node from actual node data
                total_worker_memory = sum(
                    node["Resources"].get("memory", 0) for node in worker_nodes
                )
                num_worker_nodes = len(worker_nodes)

                if total_worker_memory > 0 and num_worker_nodes > 0:
                    memory_per_node_gb = (total_worker_memory / num_worker_nodes) / (
                        1024**3
                    )
                    # Sanity check: ensure reasonable bounds (1GB - 1TB per node)
                    return max(1.0, min(1024.0, memory_per_node_gb))
            else:
                # No worker nodes found - likely autoscaling scenario
                # Fall back to hardcoded default for autoscaling environments
                return 8.0

        except Exception:
            # Fall back to cluster resources if node information unavailable
            pass

        # Fallback method using cluster resources
        total_memory_bytes = cluster_resources.get("memory", 0)
        total_cpus = cluster_resources.get("CPU", 1)

        if total_memory_bytes > 0 and total_cpus > 0:
            # Estimate number of nodes based on typical CPU/memory ratios
            # Most cloud instances have 2-8 GB per CPU, assume 4GB per CPU as baseline
            estimated_cpus_per_node = max(
                1, min(64, total_cpus // 4)
            )  # Assume 4-node minimum, 64 CPU max per node
            estimated_nodes = max(1, total_cpus // estimated_cpus_per_node)

            # Calculate memory per node
            memory_per_node_gb = (total_memory_bytes / estimated_nodes) / (1024**3)

            # Sanity check: ensure reasonable bounds (1GB - 1TB per node)
            return max(1.0, min(1024.0, memory_per_node_gb))
        else:
            # Fallback to default if cluster resources not available
            return 8.0

    except Exception:
        # Fallback to default if Ray cluster info unavailable
        return 8.0


def _estimate_dataset_memory_usage(dataset_shard) -> Dict[str, float]:
    """Estimate memory usage for a dataset shard.

    Args:
        dataset_shard: Ray Data dataset shard

    Returns:
        Dictionary with memory usage estimates in GB
    """
    stats = dataset_shard.stats()
    estimates = {
        "raw_size_gb": 0.0,
        "materialized_size_gb": 0.0,
        "xgboost_peak_size_gb": 0.0,
        "recommended_batch_size": 10000,
    }

    if stats and stats.total_bytes:
        raw_size_gb = stats.total_bytes / (1024**3)
        estimates["raw_size_gb"] = raw_size_gb

        # Estimate materialized size (often larger due to pandas overhead)
        estimates["materialized_size_gb"] = raw_size_gb * 1.5

        # XGBoost typically uses 2-3x memory during training
        estimates["xgboost_peak_size_gb"] = raw_size_gb * 3

        # Calculate recommended batch size
        if raw_size_gb > 0:
            target_batch_gb = min(10, raw_size_gb * 0.1)  # 10% of dataset or 10GB max
            estimated_row_size = stats.total_bytes / max(stats.dataset_size or 1, 1)
            estimates["recommended_batch_size"] = max(
                1000, int((target_batch_gb * 1024**3) / estimated_row_size)
            )

    return estimates
