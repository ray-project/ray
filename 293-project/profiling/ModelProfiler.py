# ModelProfiler.py
import torch
import torch.cuda as cuda
import time
import numpy as np
from typing import List, Tuple, Optional, Dict
import psutil
import gc
import os
import json
import csv
from datetime import datetime, timedelta

class ModelProfiler:
    def __init__(self, model: torch.nn.Module, input_shapes: List[Tuple], 
                 min_batch_size: int = 1,
                 max_batch_size: int = 512,
                 batch_size_step: int = 1,
                 warmup_runs: int = 3,
                 num_runs: int = 10,
                 output_dir: str = "profiling_results"):
        """Initialize the profiler with a model and parameters to test"""
        if min_batch_size < 1:
            raise ValueError("min_batch_size must be at least 1")
        if max_batch_size < min_batch_size:
            raise ValueError("max_batch_size must be greater than or equal to min_batch_size")
        if batch_size_step < 1:
            raise ValueError("batch_size_step must be at least 1")
        if warmup_runs < 0:
            raise ValueError("warmup_runs must be non-negative")
        if num_runs < 1:
            raise ValueError("num_runs must be at least 1")

        if not torch.cuda.is_available():
            raise RuntimeError("CUDA is not available. GPU is required for profiling.")

        self.model = model.cuda().eval()
        self.input_shapes = input_shapes
        self.min_batch_size = min_batch_size
        self.max_batch_size = max_batch_size
        self.batch_size_step = batch_size_step
        self.warmup_runs = warmup_runs
        self.num_runs = num_runs
        self.output_dir = output_dir
        
        self.batch_sizes = list(range(min_batch_size, max_batch_size + 1, batch_size_step))
        
        try:
            os.makedirs(output_dir, exist_ok=True)
        except Exception as e:
            raise RuntimeError(f"Failed to create output directory: {e}")
        
        self._reset_cuda_state()
        
        try:
            self.gpu_info = self._get_gpu_info()
        except Exception as e:
            raise RuntimeError(f"Failed to get GPU info: {e}")

    def _reset_cuda_state(self):
        """Reset CUDA state properly"""
        torch.cuda.empty_cache()
        torch.cuda.reset_peak_memory_stats()
        if hasattr(torch.cuda, 'reset_accumulated_memory_stats'):
            torch.cuda.reset_accumulated_memory_stats()
        gc.collect()

    def _get_gpu_info(self):
        """Get GPU info with proper error handling"""
        return {
            'name': torch.cuda.get_device_name(),
            'total_memory': torch.cuda.get_device_properties(0).total_memory / 1024**2,
            'cuda_version': torch.version.cuda,
            'compute_capability': f"{torch.cuda.get_device_capability()[0]}.{torch.cuda.get_device_capability()[1]}"
        }

    def _generate_input(self, batch_size: int) -> List[torch.Tensor]:
        """Generate input tensors with proper error handling"""
        try:
            return [torch.randn(batch_size, *shape, device='cuda', dtype=torch.float32)
                    for shape in self.input_shapes]
        except Exception as e:
            raise RuntimeError(f"Failed to generate input tensors: {e}")

    def _measure_memory(self) -> Tuple[float, float, float]:
        """Measure current, peak GPU memory usage and available memory in MB"""
        current_mem = torch.cuda.memory_allocated() / 1024**2
        peak_mem = torch.cuda.max_memory_allocated() / 1024**2
        available_mem = (torch.cuda.get_device_properties(0).total_memory / 1024**2) - current_mem
        return current_mem, peak_mem, available_mem

    def _measure_latency(self, inputs: List[torch.Tensor]) -> float:
        """Measure inference latency with proper synchronization"""
        torch.cuda.synchronize()
        
        start_event = torch.cuda.Event(enable_timing=True)
        end_event = torch.cuda.Event(enable_timing=True)
        
        try:
            start_event.record()
            with torch.cuda.amp.autocast(enabled=True):
                with torch.no_grad():
                    _ = self.model(*inputs)
            end_event.record()
            
            torch.cuda.synchronize()
            return start_event.elapsed_time(end_event)
        except Exception as e:
            raise RuntimeError(f"Failed to measure latency: {e}")
        finally:
            del start_event
            del end_event

    def profile_batch_size(self, batch_size: int) -> dict:
        """Profile model performance for a specific batch size"""
        result = {
            'batch_size': batch_size,
            'status': 'initialized',
            'error': None
        }
        
        try:
            self._reset_cuda_state()
            inputs = self._generate_input(batch_size)
            
            for _ in range(self.warmup_runs):
                self._measure_latency(inputs)
            
            latencies = []
            self._reset_cuda_state()
            
            for _ in range(self.num_runs):
                try:
                    latency = self._measure_latency(inputs)
                    latencies.append(latency)
                except Exception as e:
                    print(f"Warning: Failed measurement run: {e}")
                    continue
            
            if not latencies:
                raise RuntimeError("All measurement runs failed")
            
            current_mem, peak_mem, available_mem = self._measure_memory()
            avg_latency = np.mean(latencies)
            throughput = (batch_size * 1000) / avg_latency
            
            result.update({
                'status': 'success',
                'avg_latency_ms': avg_latency,
                'std_latency_ms': np.std(latencies),
                'min_latency_ms': np.min(latencies),
                'max_latency_ms': np.max(latencies),
                'throughput': throughput,
                'throughput_efficiency': throughput / batch_size,
                'current_memory_mb': current_mem,
                'peak_memory_mb': peak_mem,
                'available_memory_mb': available_mem,
                'memory_per_sample_mb': peak_mem / batch_size,
                'raw_latencies': latencies,
                'memory_utilization': (peak_mem / self.gpu_info['total_memory']) * 100
            })
            
        except torch.cuda.OutOfMemoryError:
            result.update({
                'status': 'OOM',
                'error': 'Out of memory error'
            })
        except Exception as e:
            result.update({
                'status': 'error',
                'error': str(e)
            })
        finally:
            self._reset_cuda_state()
            for inp in inputs:
                del inp
            
        return result

    def profile_all(self) -> List[dict]:
        """Profile all batch sizes with progress tracking"""
        results = []
        consecutive_errors = 0
        max_consecutive_errors = 3
        
        total_configs = len(self.batch_sizes)
        start_time = time.time()
        
        try:
            for i, batch_size in enumerate(self.batch_sizes, 1):
                elapsed_time = time.time() - start_time
                avg_time_per_batch = elapsed_time / i if i > 0 else 0
                estimated_remaining = avg_time_per_batch * (total_configs - i)
                
                print(f"\nBatch {i}/{total_configs} (Size: {batch_size})")
                print(f"Estimated remaining time: {timedelta(seconds=int(estimated_remaining))}")
                
                result = self.profile_batch_size(batch_size)
                results.append(result)
                
                if result['status'] == 'success':
                    consecutive_errors = 0
                    print(f"Success - Latency: {result['avg_latency_ms']:.2f}ms, "
                          f"Throughput: {result['throughput']:.1f} samples/sec")
                else:
                    consecutive_errors += 1
                    print(f"Failed: {result['status']} - {result.get('error', 'Unknown error')}")
                    
                    if consecutive_errors >= max_consecutive_errors:
                        print("\nStopping due to consecutive errors")
                        break
                
                self._reset_cuda_state()
                
        except KeyboardInterrupt:
            print("\nProfiling interrupted by user")
        except Exception as e:
            print(f"\nProfiling stopped due to error: {e}")
        finally:
            self._reset_cuda_state()
        
        return results

    def save_results(self, results: List[dict], model_name: str = "unnamed_model",
                    model_info: dict = None, dataset_info: dict = None):
        """Save profiling results to files"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base_filename = f"{model_name}_{timestamp}"
        
        # Prepare the detailed report
        report_lines = []
        report_lines.append("=" * 120)
        report_lines.append("MODEL PROFILING RESULTS")
        report_lines.append("=" * 120)
        
        # Performance Metrics Table
        report_lines.append("PERFORMANCE METRICS:")
        report_lines.append("-" * 120)
        header = f"{'Batch':<8} {'Status':<8} {'Latency (ms)':<25} {'Throughput':<12} {'Memory (MB)':<12} {'MB/Sample':<12} {'Memory %':<8}"
        report_lines.append(header)
        report_lines.append("-" * 120)
        
        successful_results = [r for r in results if r['status'] == 'success']
        
        for r in results:
            if r['status'] == 'success':
                latency_str = f"{r['avg_latency_ms']:.2f} ± {r['std_latency_ms']:.2f} ({r['min_latency_ms']:.2f}-{r['max_latency_ms']:.2f})"
                memory_percent = (r['peak_memory_mb'] / self.gpu_info['total_memory']) * 100
                line = (f"{r['batch_size']:<8} {'OK':<8} {latency_str:<25} "
                       f"{r['throughput']:>9.1f}      {r['peak_memory_mb']:>9.1f}     "
                       f"{r['memory_per_sample_mb']:>6.1f}    {memory_percent:>6.1f}%")
            else:
                line = f"{r['batch_size']:<8} {r['status']:<8} {'N/A':<25} {'N/A':<12} {'N/A':<12} {'N/A':<12} {'N/A':<8}"
            report_lines.append(line)
        
        if successful_results:
            report_lines.append("-" * 120)
            report_lines.append("PERFORMANCE ANALYSIS:")
            report_lines.append("-" * 60)
            
            # Best configurations
            best_throughput = max(successful_results, key=lambda x: x['throughput'])
            best_latency = min(successful_results, key=lambda x: x['avg_latency_ms'])
            best_memory = min(successful_results, key=lambda x: x['memory_per_sample_mb'])
            
            report_lines.append("Optimal Configurations:")
            report_lines.append("├─ Best Throughput:")
            report_lines.append(f"│  ├─ Batch Size: {best_throughput['batch_size']}")
            report_lines.append(f"│  ├─ Throughput: {best_throughput['throughput']:.1f} samples/sec")
            report_lines.append(f"│  ├─ Latency: {best_throughput['avg_latency_ms']:.2f} ms")
            report_lines.append(f"│  └─ Memory: {best_throughput['peak_memory_mb']:.1f} MB")
            
            report_lines.append("├─ Best Latency:")
            report_lines.append(f"│  ├─ Batch Size: {best_latency['batch_size']}")
            report_lines.append(f"│  ├─ Latency: {best_latency['avg_latency_ms']:.2f} ± {best_latency['std_latency_ms']:.2f} ms")
            report_lines.append(f"│  └─ Throughput: {best_latency['throughput']:.1f} samples/sec")
            
            first_result = successful_results[0]
            last_result = successful_results[-1]
            
            # Scaling Analysis
            report_lines.append("Scaling Analysis:")
            report_lines.append(f"├─ Batch Size Range: {first_result['batch_size']} → {last_result['batch_size']}")
            report_lines.append(f"├─ Throughput Scaling: {last_result['throughput']/first_result['throughput']:.2f}x")
            report_lines.append(f"├─ Latency Scaling: {last_result['avg_latency_ms']/first_result['avg_latency_ms']:.2f}x")
            report_lines.append(f"└─ Memory Scaling: {last_result['peak_memory_mb']/first_result['peak_memory_mb']:.2f}x")
            
            # System Information
            report_lines.append("System Information:")
            report_lines.append(f"├─ Device: {self.gpu_info['name']}")
            report_lines.append(f"├─ CUDA Version: {self.gpu_info['cuda_version']}")
            report_lines.append(f"├─ Compute Capability: {self.gpu_info['compute_capability']}")
            report_lines.append(f"└─ Total GPU Memory: {self.gpu_info['total_memory']:.1f} MB")
            
            # Test Configuration
            report_lines.append("Test Configuration:")
            
            # Model Information
            if model_info:
                report_lines.append("├─ Model Configuration:")
                report_lines.append(f"│  ├─ Name: {model_info.get('name', 'Unknown')}")
                report_lines.append(f"│  ├─ Type: {model_info.get('type', 'Unknown')}")
                report_lines.append(f"│  ├─ Parameters: {model_info.get('num_parameters', 0):,}")
                report_lines.append(f"│  └─ Input Shape: {model_info.get('input_shape', 'Unknown')}")
            
            # Dataset Information
            if dataset_info:
                report_lines.append("├─ Dataset Configuration:")
                report_lines.append(f"│  ├─ Name: {dataset_info.get('name', 'Unknown')}")
                report_lines.append(f"│  ├─ Size: {dataset_info.get('size', 0):,} samples")
                report_lines.append(f"│  └─ Input Shape: {dataset_info.get('input_shape', 'Unknown')}")
            
            # Test Parameters (continuing from previous code)
            report_lines.append("└─ Test Parameters:")
            report_lines.append(f"   ├─ Warmup Runs: {self.warmup_runs}")
            report_lines.append(f"   ├─ Measurement Runs: {self.num_runs}")
            report_lines.append(f"   ├─ Batch Size Step: {self.batch_size_step}")
            report_lines.append(f"   └─ Early Stopping Threshold: 0.01")
        
        report_lines.append("=" * 120)
        
        # Save detailed report
        report_path = os.path.join(self.output_dir, f"{base_filename}_report.txt")
        with open(report_path, 'w') as f:
            f.write('\n'.join(report_lines))
        
        # Save JSON
        json_path = os.path.join(self.output_dir, f"{base_filename}_detailed.json")
        full_results = {
            'model_name': model_name,
            'model_info': model_info,
            'dataset_info': dataset_info,
            'timestamp': timestamp,
            'gpu_info': self.gpu_info,
            'config': {
                'input_shapes': self.input_shapes,
                'min_batch_size': self.min_batch_size,
                'max_batch_size': self.max_batch_size,
                'batch_size_step': self.batch_size_step,
                'warmup_runs': self.warmup_runs,
                'num_runs': self.num_runs
            },
            'results': results
        }
        
        with open(json_path, 'w') as f:
            json.dump(full_results, f, indent=2)
        
        # Save CSV
        csv_path = os.path.join(self.output_dir, f"{base_filename}_summary.csv")
        fieldnames = ['batch_size', 'status', 'avg_latency_ms', 'std_latency_ms', 
                     'throughput', 'throughput_efficiency', 'peak_memory_mb', 
                     'memory_per_sample_mb', 'memory_utilization']
        
        with open(csv_path, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for result in results:
                if result['status'] == 'success':
                    row = {k: result[k] for k in fieldnames}
                    writer.writerow(row)
                else:
                    writer.writerow({
                        'batch_size': result['batch_size'],
                        'status': result['status']
                    })
        
        print(f"\nResults saved to:")
        print(f"Detailed Report: {report_path}")
        print(f"Detailed JSON: {json_path}")
        print(f"Summary CSV: {csv_path}")

    def print_results(self, results: List[dict]):
        """Print profiling results in a formatted table"""
        headers = ['Batch Size', 'Status', 'Latency (ms)', 'Throughput', 'Memory (MB)', 'MB/Sample']
        print('\n' + '-' * 100)
        print(f"{headers[0]:<12} {headers[1]:<10} {headers[2]:<20} {headers[3]:<15} {headers[4]:<15} {headers[5]:<10}")
        print('-' * 100)
        
        for r in results:
            if r['status'] == 'success':
                print(f"{r['batch_size']:<12} "
                      f"{'OK':<10} "
                      f"{r['avg_latency_ms']:>6.2f} ± {r['std_latency_ms']:>5.2f}    "
                      f"{r['throughput']:>9.1f}      "
                      f"{r['peak_memory_mb']:>9.1f}     "
                      f"{r['memory_per_sample_mb']:>6.1f}")
            else:
                print(f"{r['batch_size']:<12} "
                      f"{r['status']:<10} "
                      f"{'N/A':<20} {'N/A':<15} {'N/A':<15} {'N/A':<10}")
        print('-' * 100)
