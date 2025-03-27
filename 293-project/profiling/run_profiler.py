import torch
import torchvision
import torchvision.transforms as transforms
from torchvision.models import resnet50, ResNet50_Weights
from torch.utils.data import DataLoader, Subset
import numpy as np
from ModelProfiler import ModelProfiler
import time
from datetime import timedelta
import os
import psutil
import matplotlib.pyplot as plt
import json
from typing import Dict, List
import gc

class ProfilerRunner:
    def __init__(self, output_dir: str = "profiling_results"):
        self.output_dir = output_dir
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        os.makedirs(output_dir, exist_ok=True)
        
    def check_system_resources(self) -> Dict:
        """Check system resources before running profiler"""
        system_info = {
            'cuda_available': torch.cuda.is_available(),
            'gpu_name': torch.cuda.get_device_name() if torch.cuda.is_available() else None,
            'gpu_memory_total': None,
            'gpu_memory_available': None,
            'ram_total': psutil.virtual_memory().total / (1024**3),  # GB
            'ram_available': psutil.virtual_memory().available / (1024**3)  # GB
        }
        
        if system_info['cuda_available']:
            total_mem = torch.cuda.get_device_properties(0).total_memory / (1024**3)
            available_mem = total_mem - torch.cuda.memory_allocated() / (1024**3)
            system_info.update({
                'gpu_memory_total': total_mem,
                'gpu_memory_available': available_mem
            })
            
        return system_info

    def get_model_info(self, model: torch.nn.Module) -> Dict:
        """Get information about the model"""
        return {
            'name': 'ResNet50',
            'type': model.__class__.__name__,
            'num_parameters': sum(p.numel() for p in model.parameters()),
            'input_shape': (3, 224, 224)
        }
    
    def get_dataset_info(self, dataset) -> Dict:
        """Get information about the dataset"""
        return {
            'name': 'CIFAR-10',
            'size': len(dataset),
            'input_shape': (3, 224, 224)
        }

    def get_image_dataset(self, dataset_size: int = 5000) -> torch.utils.data.Dataset:
        """Load and prepare CIFAR-10 dataset"""
        transform = transforms.Compose([
            transforms.Resize(224),
            transforms.ToTensor(),
            transforms.Normalize(
                mean=[0.485, 0.456, 0.406],
                std=[0.229, 0.224, 0.225]
            )
        ])
        
        try:
            dataset = torchvision.datasets.CIFAR10(
                root='./data',
                train=True,
                download=True,
                transform=transform
            )
            
            indices = torch.randperm(len(dataset))[:dataset_size]
            subset = Subset(dataset, indices)
            
            test_loader = DataLoader(subset, batch_size=1)
            first_batch = next(iter(test_loader))
            if isinstance(first_batch, (tuple, list)):
                first_batch = first_batch[0]
            print(f"Successfully loaded dataset. Sample shape: {first_batch.shape}")
            
            return subset
            
        except Exception as e:
            raise RuntimeError(f"Failed to load dataset: {e}")

    def prepare_model(self) -> torch.nn.Module:
        """Prepare ResNet50 model"""
        try:
            model = resnet50(weights=ResNet50_Weights.DEFAULT)
            model.eval()
            model.to(self.device)
            
            dummy_input = torch.randn(1, 3, 224, 224).to(self.device)
            with torch.no_grad():
                _ = model(dummy_input)
            
            return model
            
        except Exception as e:
            raise RuntimeError(f"Failed to prepare model: {e}")

    def visualize_results(self, results: List[dict], save_path: str):
        """Create visualization of profiling results"""
        successful_results = [r for r in results if r['status'] == 'success']
        
        if not successful_results:
            print("No successful results to visualize")
            return
        
        batch_sizes = [r['batch_size'] for r in successful_results]
        throughputs = [r['throughput'] for r in successful_results]
        latencies = [r['avg_latency_ms'] for r in successful_results]
        memory_usage = [r['peak_memory_mb'] for r in successful_results]
        
        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 10))
        
        # Throughput vs Batch Size
        ax1.plot(batch_sizes, throughputs, 'b-')
        ax1.set_title('Throughput vs Batch Size')
        ax1.set_xlabel('Batch Size')
        ax1.set_ylabel('Throughput (samples/sec)')
        ax1.grid(True)
        
        # Latency vs Batch Size
        ax2.plot(batch_sizes, latencies, 'r-')
        ax2.set_title('Latency vs Batch Size')
        ax2.set_xlabel('Batch Size')
        ax2.set_ylabel('Latency (ms)')
        ax2.grid(True)
        
        # Memory Usage vs Batch Size
        ax3.plot(batch_sizes, memory_usage, 'g-')
        ax3.set_title('Memory Usage vs Batch Size')
        ax3.set_xlabel('Batch Size')
        ax3.set_ylabel('Peak Memory (MB)')
        ax3.grid(True)
        
        # Efficiency vs Batch Size
        efficiency = [t/b for t, b in zip(throughputs, batch_sizes)]
        ax4.plot(batch_sizes, efficiency, 'm-')
        ax4.set_title('Efficiency vs Batch Size')
        ax4.set_xlabel('Batch Size')
        ax4.set_ylabel('Throughput per Batch Item')
        ax4.grid(True)
        
        plt.tight_layout()
        plt.savefig(save_path)
        plt.close()

    def run_profiling(self):
        """Run the complete profiling process"""
        start_time = time.time()
        
        try:
            # Check system resources
            print("\nChecking system resources...")
            system_info = self.check_system_resources()
            
            if not system_info['cuda_available']:
                raise RuntimeError("CUDA is not available. GPU is required for profiling.")
            
            print("\nSystem Information:")
            print(f"GPU: {system_info['gpu_name']}")
            print(f"GPU Memory: {system_info['gpu_memory_total']:.2f} GB total, "
                  f"{system_info['gpu_memory_available']:.2f} GB available")
            print(f"RAM: {system_info['ram_total']:.2f} GB total, "
                  f"{system_info['ram_available']:.2f} GB available")
            
            # Prepare model and dataset
            print("\nPreparing model and dataset...")
            model = self.prepare_model()
            dataset = self.get_image_dataset()
            
            # Get model and dataset info
            model_info = self.get_model_info(model)
            dataset_info = self.get_dataset_info(dataset)
            
            # Initialize profiler
            profiler = ModelProfiler(
                model=model,
                input_shapes=[(3, 224, 224)],
                min_batch_size=1,
                max_batch_size=512,
                batch_size_step=1,
                warmup_runs=3,
                num_runs=10,
                output_dir=self.output_dir
            )
            
            # Run profiling
            print("\nStarting profiling process...")
            results = profiler.profile_all()
            
            # Save results with additional information
            profiler.save_results(results, 
                                model_name="resnet50",
                                model_info=model_info,
                                dataset_info=dataset_info)
            
            # Create visualizations
            print("\nGenerating visualizations...")
            vis_file = os.path.join(self.output_dir, f"profiling_plots_{time.strftime('%Y%m%d_%H%M%S')}.png")
            self.visualize_results(results, vis_file)
            
            # Print summary
            successful_results = [r for r in results if r['status'] == 'success']
            if successful_results:
                max_throughput = max(successful_results, key=lambda x: x['throughput'])
                min_latency = min(successful_results, key=lambda x: x['avg_latency_ms'])
                
                print("\nProfiling Summary:")
                print(f"Total time: {timedelta(seconds=int(time.time() - start_time))}")
                print(f"Successful batch sizes: {len(successful_results)}/{len(results)}")
                print(f"Best throughput: {max_throughput['throughput']:.2f} samples/sec "
                      f"(batch size: {max_throughput['batch_size']})")
                print(f"Best latency: {min_latency['avg_latency_ms']:.2f} ms "
                      f"(batch size: {min_latency['batch_size']})")
                
                print(f"\nPlots saved to: {vis_file}")
            
        except Exception as e:
            print(f"\nError during profiling: {e}")
            raise
        finally:
            # Cleanup
            print("\nCleaning up...")
            gc.collect()
            torch.cuda.empty_cache()

def main():
    try:
        runner = ProfilerRunner()
        runner.run_profiling()
    except KeyboardInterrupt:
        print("\nProfiling interrupted by user")
    except Exception as e:
        print(f"\nProfiling failed: {e}")
    finally:
        print("\nProfiling complete")

if __name__ == "__main__":
    main()
