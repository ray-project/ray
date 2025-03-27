import ray
import torch
import time
import subprocess
from schedule_executor import ScheduleExecutor, session
import torchvision.models as models
import torch.multiprocessing as mp
import os


def verify_gpus():
    """Verify multiple GPUs are available"""
    # Clear any existing CUDA settings
    if 'CUDA_VISIBLE_DEVICES' in os.environ:
        del os.environ['CUDA_VISIBLE_DEVICES']
    
    # Check available GPUs
    gpu_count = torch.cuda.device_count()
    print(f"\nFound {gpu_count} GPUs")
    for i in range(gpu_count):
        print(f"GPU {i}: {torch.cuda.get_device_name(i)}")
        print(f"Memory: {torch.cuda.get_device_properties(i).total_memory / 1e9:.2f} GB")
    return gpu_count

def get_gpu_info():
    """Get GPU utilization info using nvidia-smi"""
    try:
        output = subprocess.check_output(
            ['nvidia-smi', '--query-gpu=gpu_name,memory.used,utilization.gpu', 
             '--format=csv,noheader,nounits']
        ).decode()
        print("\nGPU Status:")
        for idx, line in enumerate(output.strip().split('\n')):
            name, mem_used, util = line.split(', ')
            print(f"GPU {idx} ({name}): Memory Used: {mem_used}MB, Utilization: {util}%")
    except:
        print("Could not get GPU information")

# Initialize Ray with 2 A6000 GPUs
#ray.init(num_gpus=2, include_dashboard=False)

def setup_test():
    # Check GPUs before loading models
    get_gpu_info()
    gpu_count = verify_gpus()
    if gpu_count < 2:
        raise RuntimeError("This schedule requires at least 2 GPUs")
    
    # Initialize Ray with multiple GPUs
    #ray.init(num_gpus=2, runtime_env={"env_vars": {"CUDA_VISIBLE_DEVICES": "0,1"}})
   
    print("\nLoading models...")
    model_registry = {
        'vit': models.vit_b_16(pretrained=True),  # Using direct torchvision import
        'shuffle': models.shufflenet_v2_x1_0(pretrained=True),
        'resnet': models.resnet50(pretrained=True)
    }
    
    # Schedule for 2 A6000 GPUs
    node_schedules = [
        # A6000 GPU 0: VIT only
        {
            'node_id': 'A6000_0',
            'gpu_id': 0,
            'duty_cycle': 12.57,
            'sessions': [
                (session('vit', 25, 3820.06, batch_size=48), 1.0)
            ]
        },
        # A6000 GPU 1: Mixed workload
        {
            'node_id': 'A6000_1',
            'gpu_id': 1,
            'duty_cycle': 89.84,
            'sessions': [
                (session('vit', 25, 1179.94, batch_size=106), 0.2818),
                (session('resnet', 50, 1000.0, batch_size=90), 0.4888),
                (session('shuffle', 100, 179.94, batch_size=17), 0.1848)
            ]
        }
    ]
    
    get_gpu_info()  # Check GPU status after model loading
    return model_registry, node_schedules

def old_generate_test_load(executor, duration_seconds=60):
    """Generate test load for models"""
    print(f"\nStarting test load generation for {duration_seconds} seconds...")
    start_time = time.time()
    request_counts = {'vit': 0, 'resnet': 0, 'shuffle': 0}
    
    while time.time() - start_time < duration_seconds:
        try:
            # Sample input tensor
            input_tensor = torch.randn(3, 224, 224)
            
            # VIT requests (split between GPUs)
            for _ in range(50):
                request_id = f'vit_{time.time()}'
                if executor.submit_request('vit', request_id, input_tensor):
                    request_counts['vit'] += 1
            
            # ResNet requests (GPU 1)
            for _ in range(10):
                request_id = f'resnet_{time.time()}'
                if executor.submit_request('resnet', request_id, input_tensor):
                    request_counts['resnet'] += 1
            
            # ShuffleNet requests (GPU 1)
            for _ in range(2):
                request_id = f'shuffle_{time.time()}'
                if executor.submit_request('shuffle', request_id, input_tensor):
                    request_counts['shuffle'] += 1
            
            # Print progress every 10 seconds
            elapsed = time.time() - start_time
            if int(elapsed) % 10 == 0:
                print(f"\nElapsed: {int(elapsed)}s")
                print("Requests submitted:", request_counts)
                get_gpu_info()
                
            time.sleep(0.01)
            
        except Exception as e:
            print(f"Error in load generation: {e}")


def generate_test_load(executor, duration_seconds=60):
    """Generate test load for models"""
    print(f"\nStarting test load generation for {duration_seconds} seconds...")
    start_time = time.time()
    request_counts = {'vit': 0, 'resnet': 0, 'shuffle': 0}

    try:
        while time.time() - start_time < duration_seconds:
            # Sample input tensor
            input_tensor = torch.randn(3, 224, 224)
            current_time = time.time()

            # Reduce the number of requests per iteration
            # VIT requests (split between GPUs)
            for _ in range(10):  # Reduced from 50
                request_id = f'vit_{current_time}'
                if executor.submit_request('vit', request_id, input_tensor):
                    request_counts['vit'] += 1

            # ResNet requests (GPU 1)
            for _ in range(5):  # Reduced from 10
                request_id = f'resnet_{current_time}'
                if executor.submit_request('resnet', request_id, input_tensor):
                    request_counts['resnet'] += 1

            # ShuffleNet requests (GPU 1)
            request_id = f'shuffle_{current_time}'  # Only 1 request per iteration
            if executor.submit_request('shuffle', request_id, input_tensor):
                request_counts['shuffle'] += 1

            # Add a small sleep to prevent overwhelming the queues
            time.sleep(0.05)  # Increased sleep time

            # Print progress every 10 seconds
            elapsed = time.time() - start_time
            if int(elapsed) % 10 == 0:
                print(f"\nElapsed: {int(elapsed)}s")
                print("Requests submitted:", request_counts)
                for i in range(torch.cuda.device_count()):
                    memory = torch.cuda.memory_allocated(i) / 1e9
                    print(f"GPU {i} Memory Used: {memory:.2f} GB")

    except Exception as e:
        print(f"Error generating load: {e}")

    return request_counts

def main():
    try:
        # Verify GPUs before Ray initialization
        gpu_count = verify_gpus()
        if gpu_count < 2:
            raise RuntimeError("This schedule requires at least 2 GPUs")

        # Initialize Ray with specific GPU configuration
        ray.init(
            runtime_env={
                "env_vars": {
                    "CUDA_VISIBLE_DEVICES": "0,1",
                    "CUDA_DEVICE_ORDER": "PCI_BUS_ID",
                    "RAY_DISABLE_MEMORY_MONITOR": "1"  # Prevent Ray from limiting GPU memory
                }
            },
            num_gpus=2
        )

        # Initialize with multiple GPUs
        model_registry, node_schedules = setup_test()
        
        print("\nInitializing Schedule Executor...")
        executor = ScheduleExecutor(node_schedules, model_registry)
        
        print("\nStarting execution...")
        executor.start()
        
        # Run test load
        final_counts = generate_test_load(executor)
        
        # Print final statistics
        print("\nTest completed. Final Statistics:")
        stats = executor.get_system_stats()
        print(f"\nTotal Requests Processed: {stats['total_requests']}")
        print(f"Failed Requests: {stats['failed_requests']}")
        print("\nSubmitted Requests by Model:", final_counts)
        
        print("\nQueue Statistics:")
        for model, queue_stats in stats['queue_stats'].items():
            print(f"\n{model}:")
            for stat, value in queue_stats.items():
                print(f"  {stat}: {value}")
                
    except Exception as e:
        print(f"Test error: {e}")
    finally:
        print("\nShutting down Ray...")
        if 'executor' in locals():
            executor.stop()
        ray.shutdown()


if __name__ == "__main__":
    main()

