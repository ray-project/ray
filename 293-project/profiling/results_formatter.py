import json
import csv
from datetime import datetime
import os
from typing import List, Dict
import numpy as np

class ResultsFormatter:
    """Format and save profiling results in various formats"""
    
    def __init__(self, results: List[dict], gpu_info: dict, config: dict, 
                 model_info: dict = None, dataset_info: dict = None):
        self.results = results
        self.gpu_info = gpu_info
        self.config = config
        self.successful_results = [r for r in results if r['status'] == 'success']
        
        # Default model and dataset info if none provided
        self.model_info = model_info or {
            'name': 'Unknown Model',
            'type': 'Unknown',
            'num_parameters': 0,
            'input_shape': None
        }
        
        self.dataset_info = dataset_info or {
            'name': 'Unknown Dataset',
            'size': 0,
            'input_shape': None
        }

    def format_text(self) -> str:
        """Format results as a detailed text report"""
        lines = []
        lines.append("=" * 120)
        lines.append("MODEL PROFILING RESULTS")
        lines.append("=" * 120)

        # Performance Metrics Table
        lines.append("PERFORMANCE METRICS:")
        lines.append("-" * 120)
        header = f"{'Batch':<8} {'Status':<8} {'Latency (ms)':<25} {'Throughput':<12} {'Memory (MB)':<12} {'MB/Sample':<12} {'Memory %':<8}"
        lines.append(header)
        lines.append("-" * 120)

        for result in self.results:
            if result['status'] == 'success':
                latency_str = f"{result['avg_latency_ms']:.2f} ± {result['std_latency_ms']:.2f} ({result['min_latency_ms']:.2f}-{result['max_latency_ms']:.2f})"
                memory_percent = (result['peak_memory_mb'] / self.gpu_info['total_memory']) * 100
                line = (f"{result['batch_size']:<8} {'OK':<8} {latency_str:<25} "
                       f"{result['throughput']:>9.1f}      {result['peak_memory_mb']:>9.1f}     "
                       f"{result['memory_per_sample_mb']:>6.1f}    {memory_percent:>6.1f}%")
            else:
                line = f"{result['batch_size']:<8} {result['status']:<8} {'N/A':<25} {'N/A':<12} {'N/A':<12} {'N/A':<12} {'N/A':<8}"
            lines.append(line)

        if self.successful_results:
            lines.append("-" * 120)
            lines.append("PERFORMANCE ANALYSIS:")
            lines.append("-" * 60)

            # Best configurations
            best_throughput = max(self.successful_results, key=lambda x: x['throughput'])
            best_latency = min(self.successful_results, key=lambda x: x['avg_latency_ms'])
            best_memory = min(self.successful_results, key=lambda x: x['memory_per_sample_mb'])

            lines.append("Optimal Configurations:")
            lines.append("├─ Best Throughput:")
            lines.append(f"│  ├─ Batch Size: {best_throughput['batch_size']}")
            lines.append(f"│  ├─ Throughput: {best_throughput['throughput']:.1f} samples/sec")
            lines.append(f"│  ├─ Latency: {best_throughput['avg_latency_ms']:.2f} ms")
            lines.append(f"│  └─ Memory: {best_throughput['peak_memory_mb']:.1f} MB")

            lines.append("├─ Best Latency:")
            lines.append(f"│  ├─ Batch Size: {best_latency['batch_size']}")
            lines.append(f"│  ├─ Latency: {best_latency['avg_latency_ms']:.2f} ± {best_latency['std_latency_ms']:.2f} ms")
            lines.append(f"│  └─ Throughput: {best_latency['throughput']:.1f} samples/sec")

            lines.append("└─ Best Memory Efficiency:")
            lines.append(f"   ├─ Batch Size: {best_memory['batch_size']}")
            lines.append(f"   ├─ Memory per sample: {best_memory['memory_per_sample_mb']:.1f} MB")
            lines.append(f"   └─ Total Memory: {best_memory['peak_memory_mb']:.1f} MB")

            # Scaling Analysis
            first_result = self.successful_results[0]
            last_result = self.successful_results[-1]
            
            lines.append("Scaling Analysis:")
            lines.append(f"├─ Batch Size Range: {first_result['batch_size']} → {last_result['batch_size']}")
            lines.append(f"├─ Throughput Scaling: {last_result['throughput']/first_result['throughput']:.2f}x")
            lines.append(f"├─ Latency Scaling: {last_result['avg_latency_ms']/first_result['avg_latency_ms']:.2f}x")
            lines.append(f"└─ Memory Scaling: {last_result['peak_memory_mb']/first_result['peak_memory_mb']:.2f}x")

            # Variability Analysis
            cv_values = [r['std_latency_ms']/r['avg_latency_ms'] for r in self.successful_results]
            lines.append("Variability Analysis:")
            lines.append(f"├─ Average Coefficient of Variation: {np.mean(cv_values):.3f}")
            lines.append(f"└─ Maximum Coefficient of Variation: {np.max(cv_values):.3f}")

            # System Information
            lines.append("System Information:")
            lines.append(f"├─ Device: {self.gpu_info['name']}")
            lines.append(f"├─ CUDA Version: {self.gpu_info['cuda_version']}")
            lines.append(f"├─ Compute Capability: {self.gpu_info['compute_capability']}")
            lines.append(f"└─ Total GPU Memory: {self.gpu_info['total_memory']:.1f} MB")

            # Test Configuration
            lines.append("Test Configuration:")
            # Model Information
            lines.append("├─ Model Configuration:")
            lines.append(f"│  ├─ Name: {self.model_info['name']}")
            lines.append(f"│  ├─ Type: {self.model_info['type']}")
            lines.append(f"│  ├─ Parameters: {self.model_info['num_parameters']:,}")
            lines.append(f"│  └─ Input Shape: {self.model_info['input_shape']}")
            
            # Dataset Information
            lines.append("├─ Dataset Configuration:")
            lines.append(f"│  ├─ Name: {self.dataset_info['name']}")
            lines.append(f"│  ├─ Size: {self.dataset_info['size']:,} samples")
            lines.append(f"│  └─ Input Shape: {self.dataset_info['input_shape']}")
            
            # Test Parameters
            lines.append("└─ Test Parameters:")
            lines.append(f"   ├─ Warmup Runs: {self.config['warmup_runs']}")
            lines.append(f"   ├─ Measurement Runs: {self.config['num_runs']}")
            lines.append(f"   ├─ Batch Size Step: {self.config['batch_size_step']}")
            lines.append(f"   └─ Early Stopping Threshold: 0.01")

        lines.append("=" * 120)
        return "\n".join(lines)

    def save_results(self, output_dir: str, prefix: str = "profiling"):
        """Save results in multiple formats"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Save detailed text report
        text_path = os.path.join(output_dir, f"{prefix}_report_{timestamp}.txt")
        with open(text_path, 'w') as f:
            f.write(self.format_text())

        return text_path
