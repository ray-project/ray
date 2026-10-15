---
myst:
  html_meta:
    description: "Troubleshoot multi-node GPU serving on KubeRay: head Pod placement on GPU nodes, AWS OFI plugin issues, and NCCL debugging."
---

(serve-multi-node-gpu-troubleshooting)=

# Troubleshoot multi-node GPU serving on KubeRay

This guide helps you diagnose and resolve common issues when you deploy multi-node GPU workloads on KubeRay, particularly large language model (LLM) serving with vLLM.

## Debugging strategy

To isolate a problem with multi-node GPU serving, work through the following steps:

1. **Test on different platforms**: Compare behavior across the following setups:
   - Single node without KubeRay
   - Standalone vLLM server on KubeRay
   - Ray Serve LLM deployment on KubeRay

1. **Vary hardware configurations**: Test with different GPU types, such as A100s and H100s, to identify hardware-specific issues.

1. **Use minimal reproducers**: Create simplified test cases that isolate specific components, such as NCCL or model loading.

## Common issues and solutions

Each of the following issues lists its symptoms, its root cause, and a solution.

### 1. Head Pod scheduled on GPU node

**Symptoms**:
- `ray status` shows duplicate GPU resources, for example, 24 GPUs when the cluster has only 16.
- Model serving hangs when you use pipeline parallelism with a pipeline-parallel size greater than one.
- Resource allocation conflicts occur.

**Root cause**: The Ray head Pod is incorrectly scheduled on a GPU worker node, which causes resource accounting issues.

**Solution**: Configure the head Pod to use zero GPUs in your RayCluster specification:

```yaml
apiVersion: ray.io/v1
kind: RayCluster
metadata:
  name: my-cluster
spec:
  headGroupSpec:
    rayStartParams:
      num-cpus: "0"
      num-gpus: "0"  # Ensure head pod doesn't claim GPU resources.
    # ... other head group configuration
```

(aws-ofi-plugin-version-issues-h100-specific)=
### 2. AWS OFI plugin version issues on H100 instances

**Symptoms**:
- NCCL fails to initialize on H100 instances.
- The workload runs on A100 but fails on H100 with an identical configuration.
- Topology files are malformed.

**Root cause**: An outdated `aws-ofi-plugin` in the container image causes NCCL topology detection to fail on H100 instances.

**Related issues**:
- [NVIDIA NCCL issue #1726](https://github.com/NVIDIA/nccl/issues/1726)
- [vLLM issue #18997](https://github.com/vllm-project/vllm/issues/18997)
- [AWS OFI NCCL fix](https://github.com/aws/aws-ofi-nccl/pull/916)

**Solution**:
- Update to a container image with a newer `aws-ofi-plugin`.
- Run the NCCL debugging script below to verify that NCCL works as expected.
- Consider adjusting the configuration for your hardware.

## Further troubleshooting

If issues persist after you follow this guide, try the following steps:

1. **Collect diagnostic information**: Run the NCCL debugging script below and save the output.
1. **Check compatibility**: Verify that your Ray, vLLM, PyTorch, and CUDA versions are compatible.
1. **Review logs**: Examine the Ray cluster logs and worker Pod logs for more error details.
1. **Verify hardware**: If possible, test with different GPU types.
1. **Ask the community**: Share your findings with the Ray and vLLM communities for more help.

## Additional resources

- {doc}`Ray multi-node GPU guide </kuberay/user-guides/gpu>`
- [vLLM distributed serving documentation](https://docs.vllm.ai/en/latest/serving/parallelism_scaling/)
- [NCCL troubleshooting guide](https://docs.nvidia.com/deeplearning/nccl/user-guide/docs/troubleshooting.html)

## NCCL debugging script

Use this diagnostic script to identify NCCL-related issues in your multi-node GPU setup:

```python
#!/usr/bin/env python3
"""
NCCL Diagnostic Script for Multi-Node GPU Serving

This script helps identify NCCL configuration issues that can cause
multi-node GPU serving failures. Run this script on each node to verify
NCCL function before deploying distributed workloads.

Usage: python3 multi-node-nccl-check.py
"""
import os
import sys
import socket
import torch
from datetime import datetime

def log(msg):
    """Log messages with timestamp for better debugging."""
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] {msg}", flush=True)

def print_environment_info():
    """Print relevant environment information for debugging."""
    log("=== Environment Information ===")
    log(f"Hostname: {socket.gethostname()}")
    log(f"CUDA_VISIBLE_DEVICES: {os.environ.get('CUDA_VISIBLE_DEVICES', 'not set')}")
    
    # Print all NCCL-related environment variables.
    nccl_vars = [var for var in os.environ.keys() if var.startswith('NCCL_')]
    if nccl_vars:
        log("NCCL Environment Variables:")
        for var in sorted(nccl_vars):
            log(f"  {var}: {os.environ[var]}")
    else:
        log("No NCCL environment variables set")

def check_cuda_availability():
    """Verify CUDA is available and functional."""
    log("\n=== CUDA Availability Check ===")
    
    if not torch.cuda.is_available():
        log("ERROR: CUDA not available")
        return False
    
    device_count = torch.cuda.device_count()
    log(f"CUDA device count: {device_count}")
    log(f"PyTorch version: {torch.__version__}")
    
    # Check NCCL availability in PyTorch.
    try:
        import torch.distributed as dist
        if hasattr(torch.distributed, 'nccl'):
            log(f"PyTorch NCCL available: {torch.distributed.is_nccl_available()}")
    except Exception as e:
        log(f"Error checking NCCL availability: {e}")
    
    return True

def test_individual_gpus():
    """Test that each GPU is working individually."""
    log("\n=== Individual GPU Tests ===")
    
    for gpu_id in range(torch.cuda.device_count()):
        log(f"\n--- Testing GPU {gpu_id} ---")
        
        try:
            torch.cuda.set_device(gpu_id)
            device = torch.cuda.current_device()
            
            log(f"Device {device}: {torch.cuda.get_device_name(device)}")
            
            # Print device properties.
            props = torch.cuda.get_device_properties(device)
            log(f"  Compute capability: {props.major}.{props.minor}")
            log(f"  Total memory: {props.total_memory / 1024**3:.2f} GB")
            
            # Test basic CUDA operations.
            log("  Testing basic CUDA operations...")
            tensor = torch.ones(1000, device=f'cuda:{gpu_id}')
            result = tensor.sum()
            log(f"  Basic CUDA test passed: sum = {result.item()}")
            
            # Test cross-GPU operations if multiple GPUs are available.
            if torch.cuda.device_count() > 1:
                log("  Testing cross-GPU operations...")
                try:
                    other_gpu = (gpu_id + 1) % torch.cuda.device_count()
                    test_tensor = torch.randn(10, 10, device=f'cuda:{gpu_id}')
                    tensor_copy = test_tensor.to(f'cuda:{other_gpu}')
                    log(f"  Cross-GPU copy successful: GPU {gpu_id} -> GPU {other_gpu}")
                except Exception as e:
                    log(f"  Cross-GPU copy failed: {e}")
            
            # Test memory allocation.
            log("  Testing large memory allocations...")
            try:
                large_tensor = torch.zeros(1000, 1000, device=f'cuda:{gpu_id}')
                log("  Large memory allocation successful")
                del large_tensor
            except Exception as e:
                log(f"  Large memory allocation failed: {e}")
                
        except Exception as e:
            log(f"ERROR testing GPU {gpu_id}: {e}")
            import traceback
            log(f"Traceback:\n{traceback.format_exc()}")

def test_nccl_initialization():
    """Test NCCL initialization and basic operations."""
    log("\n=== NCCL Initialization Test ===")
    
    try:
        import torch.distributed as dist
        
        # Set up single-process NCCL environment.
        os.environ['MASTER_ADDR'] = 'localhost'
        os.environ['MASTER_PORT'] = '29500'
        os.environ['RANK'] = '0'
        os.environ['WORLD_SIZE'] = '1'
        
        log("Attempting single-process NCCL initialization...")
        dist.init_process_group(
            backend='nccl',
            rank=0,
            world_size=1
        )
        
        log("Single-process NCCL initialization successful!")
        
        # Test basic NCCL operation.
        if torch.cuda.is_available():
            device = torch.cuda.current_device()
            tensor = torch.ones(10, device=device)
            
            # This is a no-op with world_size=1 but exercises NCCL
            dist.all_reduce(tensor)
            log("NCCL all_reduce test successful!")
        
        dist.destroy_process_group()
        log("NCCL cleanup successful!")
        
    except Exception as e:
        log(f"NCCL initialization failed: {e}")
        import traceback
        log(f"Full traceback:\n{traceback.format_exc()}")

def main():
    """Main diagnostic routine."""
    log("Starting NCCL Diagnostic Script")
    log("=" * 50)
    
    print_environment_info()
    
    if not check_cuda_availability():
        sys.exit(1)
    
    test_individual_gpus()
    test_nccl_initialization()
    
    log("\n" + "=" * 50)
    log("NCCL diagnostic script completed")
    log("If you encountered errors, check the specific error messages above")
    log("and refer to the troubleshooting guide for solutions.")

if __name__ == "__main__":
    main()

