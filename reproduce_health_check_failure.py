#!/usr/bin/env python3
"""
Reproduce raylet health check failure under high load with CPU throttling.

This script simulates the conditions that cause nodes to be marked dead:
1. Enables resource isolation with tight CPU limits (simulating T4 worker nodes)
2. Runs object store intensive batch inference workload
3. Monitors health check RPC latency
4. Should trigger health check failures after ~15-30 seconds

Requirements:
- Linux with cgroup v2 support
- Root or appropriate cgroup permissions
- Ray built from source (for latest health check monitoring)
"""

import argparse
import os
import sys
import time
import tempfile
import psutil
import numpy as np
from typing import List

try:
    import ray
except ImportError:
    print("Error: Ray is not installed. Please install Ray first.")
    sys.exit(1)


class HealthCheckMonitor:
    """Monitor Ray health check metrics from the dashboard."""
    
    def __init__(self):
        self.metrics_samples = []
        
    def collect_metrics(self):
        """Collect health check latency from Ray metrics."""
        try:
            # Get metrics from the local node
            import requests
            metrics_port = os.environ.get("RAY_METRICS_EXPORT_PORT", "8080")
            response = requests.get(f"http://localhost:{metrics_port}/metrics", timeout=1)
            
            # Parse health check latency
            for line in response.text.split('\n'):
                if 'health_check_rpc_latency_ms' in line and not line.startswith('#'):
                    # Extract the value
                    parts = line.split()
                    if len(parts) >= 2:
                        value = float(parts[-1])
                        self.metrics_samples.append(value)
                        return value
        except Exception as e:
            print(f"Warning: Could not collect metrics: {e}")
            return None
        return None


def create_batch_inference_task(object_size_mb: int, num_iterations: int):
    """
    Create a CPU and object-store intensive task that simulates batch inference.
    
    Args:
        object_size_mb: Size of each "batch" in MB
        num_iterations: Number of processing iterations
    """
    @ray.remote
    def process_batch(batch_data: np.ndarray, iteration: int) -> np.ndarray:
        """Process a batch - CPU intensive + object store transfers."""
        # Simulate inference: matrix operations (CPU intensive)
        result = batch_data
        for _ in range(5):
            result = np.matmul(result.T, result)
            result = result / (np.max(result) + 1e-10)  # Normalize to prevent overflow
            
        # Simulate post-processing
        result = np.abs(np.fft.fft2(result[:1000, :1000]))
        
        # Return large result (triggers object store)
        return result.astype(np.float32)
    
    return process_batch


def run_workload(
    num_workers: int = 8,
    object_size_mb: int = 50,
    num_batches: int = 100,
    parallel_tasks: int = 16
):
    """
    Run the object-store intensive workload that triggers the issue.
    
    Args:
        num_workers: Number of worker processes
        object_size_mb: Size of each batch in MB
        num_batches: Total number of batches to process
        parallel_tasks: Number of concurrent tasks (creates backpressure)
    """
    print(f"\n{'='*70}")
    print(f"Starting workload:")
    print(f"  Workers: {num_workers}")
    print(f"  Object size: {object_size_mb} MB")
    print(f"  Total batches: {num_batches}")
    print(f"  Parallel tasks: {parallel_tasks}")
    print(f"{'='*70}\n")
    
    process_batch = create_batch_inference_task(object_size_mb, 10)
    
    # Create initial data
    batch_shape = (int(np.sqrt(object_size_mb * 1024 * 1024 / 8)), 
                   int(np.sqrt(object_size_mb * 1024 * 1024 / 8)))
    
    pending_tasks = []
    completed = 0
    
    monitor = HealthCheckMonitor()
    start_time = time.time()
    last_metric_time = start_time
    
    for i in range(num_batches):
        # Create batch data
        batch_data = np.random.rand(*batch_shape).astype(np.float32)
        
        # Submit task
        task_ref = process_batch.remote(batch_data, i)
        pending_tasks.append(task_ref)
        
        # Maintain backpressure
        if len(pending_tasks) >= parallel_tasks:
            # Wait for one to complete
            ready, pending_tasks = ray.wait(pending_tasks, num_returns=1, timeout=10.0)
            if ready:
                try:
                    ray.get(ready[0], timeout=5.0)
                    completed += 1
                except Exception as e:
                    print(f"Task failed: {e}")
        
        # Monitor health check latency every 2 seconds
        if time.time() - last_metric_time > 2.0:
            elapsed = time.time() - start_time
            latency = monitor.collect_metrics()
            
            # Check raylet CPU usage
            try:
                current_process = psutil.Process()
                raylet_processes = [p for p in psutil.process_iter(['name', 'cpu_percent']) 
                                  if 'raylet' in p.info['name'].lower()]
                if raylet_processes:
                    raylet_cpu = raylet_processes[0].cpu_percent(interval=0.1)
                else:
                    raylet_cpu = 0.0
            except:
                raylet_cpu = 0.0
            
            print(f"[{elapsed:.1f}s] Completed: {completed}/{num_batches} | "
                  f"Pending: {len(pending_tasks)} | "
                  f"Raylet CPU: {raylet_cpu:.1f}% | "
                  f"Health check latency: {latency if latency else 'N/A'}")
            
            last_metric_time = time.time()
            
            # Check if latency is dangerously high
            if latency and latency > 5000:
                print(f"\n⚠️  WARNING: Health check latency is HIGH ({latency:.0f}ms)!")
                print("    Node may be marked dead soon!")
    
    # Wait for remaining tasks
    print("\nWaiting for remaining tasks...")
    if pending_tasks:
        try:
            ray.get(pending_tasks, timeout=30.0)
        except Exception as e:
            print(f"Some tasks failed: {e}")
    
    elapsed = time.time() - start_time
    print(f"\n{'='*70}")
    print(f"Workload completed in {elapsed:.1f}s")
    print(f"Completed {completed}/{num_batches} batches")
    print(f"{'='*70}\n")


def setup_ray_with_throttling(
    system_reserved_cpu: float = 0.4,  # 5% of 8 CPUs
    health_check_period_ms: int = 3000,
    health_check_timeout_ms: int = 10000,
    health_check_failure_threshold: int = 5,
    num_cpus: int = 8,
    object_store_memory: int = 2 * 1024**3,  # 2GB
):
    """
    Setup Ray with tight resource limits to reproduce the issue.
    
    Args:
        system_reserved_cpu: CPU cores reserved for raylet (low value triggers throttling)
        health_check_period_ms: How often to check (default: 3s)
        health_check_timeout_ms: Timeout per check (default: 10s)
        health_check_failure_threshold: Failures before marking dead (default: 5)
        num_cpus: Total CPUs available
        object_store_memory: Object store size in bytes
    """
    
    # Check if we can use resource isolation
    use_resource_isolation = False
    cgroup_path = None
    
    if sys.platform == "linux":
        # Check for cgroup v2
        if os.path.exists("/sys/fs/cgroup/cgroup.controllers"):
            cgroup_path = tempfile.mkdtemp(prefix="ray_reproduce_")
            use_resource_isolation = True
            print(f"✓ Using cgroup v2 resource isolation at: {cgroup_path}")
        else:
            print("⚠️  cgroup v2 not detected, running without resource isolation")
            print("   (Issue is less likely to reproduce)")
    else:
        print(f"⚠️  Not on Linux ({sys.platform}), cannot use resource isolation")
        print("   (Issue is less likely to reproduce)")
    
    print(f"\nRay configuration:")
    print(f"  CPUs: {num_cpus}")
    print(f"  System reserved CPU: {system_reserved_cpu} cores ({system_reserved_cpu/num_cpus*100:.1f}%)")
    print(f"  Object store: {object_store_memory / 1024**3:.1f} GB")
    print(f"  Health check period: {health_check_period_ms}ms")
    print(f"  Health check timeout: {health_check_timeout_ms}ms")
    print(f"  Health check failure threshold: {health_check_failure_threshold}")
    print(f"  Time to node death: ~{health_check_period_ms * health_check_failure_threshold / 1000:.0f}s minimum\n")
    
    ray_config = {
        "health_check_period_ms": health_check_period_ms,
        "health_check_timeout_ms": health_check_timeout_ms,
        "health_check_failure_threshold": health_check_failure_threshold,
        # Increase logging for debugging
        "handler_warning_timeout_ms": 500,
    }
    
    init_kwargs = {
        "num_cpus": num_cpus,
        "object_store_memory": object_store_memory,
        "_system_config": ray_config,
        "logging_level": "info",
    }
    
    if use_resource_isolation and cgroup_path:
        # This requires sudo/root on most systems
        try:
            init_kwargs["_enable_resource_isolation"] = True
            init_kwargs["_cgroup_path"] = cgroup_path
            init_kwargs["_system_reserved_cpu"] = system_reserved_cpu
            init_kwargs["_system_reserved_memory"] = 1 * 1024**3  # 1GB
        except Exception as e:
            print(f"Warning: Could not enable resource isolation: {e}")
            use_resource_isolation = False
    
    try:
        ray.init(**init_kwargs)
        print("✓ Ray initialized successfully\n")
        return cgroup_path
    except Exception as e:
        print(f"Error initializing Ray: {e}")
        if "permission" in str(e).lower() or "cgroup" in str(e).lower():
            print("\nNote: Resource isolation requires root/sudo permissions.")
            print("Try running with: sudo $(which python) reproduce_health_check_failure.py")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="Reproduce raylet health check failure under high load",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic reproduction (mild load)
  python reproduce_health_check_failure.py
  
  # Aggressive reproduction (more likely to trigger)
  python reproduce_health_check_failure.py --aggressive
  
  # Custom configuration
  python reproduce_health_check_failure.py --system-cpu 0.3 --workers 16 --batches 200
  
Note: For best results, run with resource isolation enabled (requires Linux + root):
  sudo $(which python) reproduce_health_check_failure.py --aggressive
        """
    )
    
    parser.add_argument(
        "--system-cpu",
        type=float,
        default=0.4,
        help="CPU cores reserved for raylet (lower = more likely to throttle). Default: 0.4"
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=8,
        help="Number of worker CPUs. Default: 8"
    )
    parser.add_argument(
        "--object-size-mb",
        type=int,
        default=50,
        help="Size of each batch in MB. Default: 50"
    )
    parser.add_argument(
        "--batches",
        type=int,
        default=100,
        help="Number of batches to process. Default: 100"
    )
    parser.add_argument(
        "--parallel",
        type=int,
        default=16,
        help="Number of parallel tasks (higher = more backpressure). Default: 16"
    )
    parser.add_argument(
        "--aggressive",
        action="store_true",
        help="Use aggressive settings to maximize chance of reproduction"
    )
    parser.add_argument(
        "--health-check-period-ms",
        type=int,
        default=3000,
        help="Health check period in milliseconds. Default: 3000"
    )
    parser.add_argument(
        "--health-check-timeout-ms",
        type=int,
        default=10000,
        help="Health check timeout in milliseconds. Default: 10000"
    )
    
    args = parser.parse_args()
    
    # Override with aggressive settings if requested
    if args.aggressive:
        print("Using AGGRESSIVE settings to maximize reproduction chance\n")
        args.system_cpu = 0.25  # Very tight - 3% of 8 CPUs
        args.object_size_mb = 100
        args.batches = 200
        args.parallel = 32
    
    print("="*70)
    print("Raylet Health Check Failure Reproduction Script")
    print("="*70)
    
    cgroup_path = None
    try:
        # Setup Ray
        cgroup_path = setup_ray_with_throttling(
            system_reserved_cpu=args.system_cpu,
            health_check_period_ms=args.health_check_period_ms,
            health_check_timeout_ms=args.health_check_timeout_ms,
            health_check_failure_threshold=5,
            num_cpus=args.workers,
            object_store_memory=2 * 1024**3,
        )
        
        print("Starting workload in 3 seconds...")
        time.sleep(3)
        
        # Run workload
        run_workload(
            num_workers=args.workers,
            object_size_mb=args.object_size_mb,
            num_batches=args.batches,
            parallel_tasks=args.parallel
        )
        
        print("\n✓ Workload completed!")
        print("\nIf you saw health check latency > 5000ms, the issue was reproduced.")
        print("Check Ray dashboard for node failures or run:")
        print("  ray status")
        
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
    except Exception as e:
        print(f"\n\nError: {e}")
        import traceback
        traceback.print_exc()
    finally:
        print("\nShutting down Ray...")
        ray.shutdown()
        
        # Cleanup cgroup
        if cgroup_path and os.path.exists(cgroup_path):
            try:
                os.rmdir(cgroup_path)
            except:
                pass


if __name__ == "__main__":
    main()
