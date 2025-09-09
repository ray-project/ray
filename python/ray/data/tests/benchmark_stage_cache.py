"""Benchmark script for Ray Data stage caching functionality."""

import gc
import statistics
import time
from typing import List, Tuple

import ray
from ray.data._internal.cache.stage_cache import (
    clear_stage_cache,
    get_stage_cache_stats,
)
from ray.data.context import DataContext


class StageCacheBenchmark:
    """Benchmark suite for stage cache performance."""

    def __init__(self):
        self.results = {}

    def setup_ray(self):
        """Initialize Ray if not already initialized."""
        if not ray.is_initialized():
            ray.init()

    def cleanup(self):
        """Clean up resources."""
        clear_stage_cache()
        gc.collect()

    def time_operation(
        self, operation_func, num_runs: int = 3
    ) -> Tuple[float, float, List[float]]:
        """Time an operation multiple times and return mean, std, and all times."""
        times = []

        for _ in range(num_runs):
            # Clean up before each run
            gc.collect()

            start_time = time.time()
            operation_func()
            end_time = time.time()

            times.append(end_time - start_time)

        mean_time = statistics.mean(times)
        std_time = statistics.stdev(times) if len(times) > 1 else 0.0

        return mean_time, std_time, times

    def benchmark_cache_vs_no_cache(
        self, dataset_size: int = 1000, num_iterations: int = 5
    ):
        """Benchmark identical operations with and without stage cache."""
        print(
            f"\n🔄 Benchmarking cache vs no-cache (size={dataset_size}, iterations={num_iterations})"
        )

        def create_dataset():
            return ray.data.range(dataset_size).map(
                lambda x: {"computed": x["id"] ** 2 % 1000}
            )

        # Benchmark WITHOUT stage cache
        DataContext.get_current().enable_stage_cache = False
        clear_stage_cache()

        def run_without_cache():
            results = []
            for _ in range(num_iterations):
                ds = create_dataset()
                mat = ds.materialize()
                results.append(mat.count())
            return results

        no_cache_mean, no_cache_std, no_cache_times = self.time_operation(
            run_without_cache
        )

        # Benchmark WITH stage cache
        DataContext.get_current().enable_stage_cache = True
        clear_stage_cache()

        def run_with_cache():
            results = []
            for _ in range(num_iterations):
                ds = create_dataset()
                mat = ds.materialize()
                results.append(mat.count())
            return results

        cache_mean, cache_std, cache_times = self.time_operation(run_with_cache)
        cache_stats = get_stage_cache_stats()

        # Calculate speedup
        speedup = no_cache_mean / cache_mean if cache_mean > 0 else 0

        print(f"📊 Results:")
        print(f"   Without cache: {no_cache_mean:.3f}s ± {no_cache_std:.3f}s")
        print(f"   With cache:    {cache_mean:.3f}s ± {cache_std:.3f}s")
        print(f"   Speedup:       {speedup:.2f}x")
        print(
            f"   Cache stats:   {cache_stats['hits']} hits, {cache_stats['misses']} misses"
        )
        print(f"   Hit rate:      {cache_stats['hit_rate']:.1%}")

        self.results["cache_vs_no_cache"] = {
            "dataset_size": dataset_size,
            "iterations": num_iterations,
            "no_cache_time": no_cache_mean,
            "cache_time": cache_mean,
            "speedup": speedup,
            "cache_stats": cache_stats,
        }

        return speedup

    def benchmark_fingerprint_performance(self, num_operations: int = 100):
        """Benchmark fingerprint computation performance."""
        print(
            f"\n🔍 Benchmarking fingerprint computation ({num_operations} operations)"
        )

        from ray.data._internal.cache.stage_cache import StageCache

        cache = StageCache()

        # Create datasets of varying complexity
        datasets = []
        for i in range(num_operations):
            if i % 3 == 0:  # Simple dataset
                ds = ray.data.range(100)
            elif i % 3 == 1:  # Medium complexity
                ds = (
                    ray.data.range(100)
                    .map(lambda x: x["id"] * 2)
                    .filter(lambda x: x > 50)
                )
            else:  # Complex dataset
                ds = (
                    ray.data.range(100)
                    .map(lambda x: {"a": x["id"]})
                    .filter(lambda x: x["a"] > 10)
                    .map(lambda x: {"b": x["a"] * 2})
                    .filter(lambda x: x["b"] < 150)
                )
            datasets.append(ds)

        def compute_fingerprints():
            fingerprints = []
            for ds in datasets:
                fp = cache._compute_plan_fingerprint(ds._logical_plan)
                fingerprints.append(fp)
            return fingerprints

        fp_mean, fp_std, fp_times = self.time_operation(compute_fingerprints)

        avg_time_per_fp = fp_mean / num_operations * 1000  # Convert to milliseconds

        print(f"📊 Fingerprint Results:")
        print(f"   Total time:     {fp_mean:.3f}s ± {fp_std:.3f}s")
        print(f"   Per operation:  {avg_time_per_fp:.2f}ms")
        print(f"   Operations/sec: {num_operations / fp_mean:.1f}")

        self.results["fingerprint_performance"] = {
            "num_operations": num_operations,
            "total_time": fp_mean,
            "time_per_operation_ms": avg_time_per_fp,
        }

        return avg_time_per_fp

    def benchmark_cache_scalability(self, cache_sizes: List[int] = None):
        """Benchmark cache performance with different numbers of cached entries."""
        if cache_sizes is None:
            cache_sizes = [10, 50, 100, 200, 500]

        print(f"\n📈 Benchmarking cache scalability")

        DataContext.get_current().enable_stage_cache = True
        scalability_results = {}

        for cache_size in cache_sizes:
            print(f"   Testing with {cache_size} cache entries...")
            clear_stage_cache()

            # Pre-populate cache with unique entries
            for i in range(cache_size):
                ds = ray.data.range(10).map(
                    lambda x, i=i: {"cache_entry": i, "value": x["id"]}
                )
                mat = ds.materialize()

            # Now test cache lookup performance
            def test_cache_lookups():
                # Test both hits and misses
                hit_count = 0
                miss_count = 0

                # Test cache hits (first half of entries)
                for i in range(cache_size // 2):
                    ds = ray.data.range(10).map(
                        lambda x, i=i: {"cache_entry": i, "value": x["id"]}
                    )
                    mat = ds.materialize()
                    hit_count += 1

                # Test cache misses (new entries)
                for i in range(cache_size, cache_size + cache_size // 2):
                    ds = ray.data.range(10).map(
                        lambda x, i=i: {"cache_entry": i, "value": x["id"]}
                    )
                    mat = ds.materialize()
                    miss_count += 1

                return hit_count, miss_count

            lookup_mean, lookup_std, _ = self.time_operation(test_cache_lookups)
            cache_stats = get_stage_cache_stats()

            scalability_results[cache_size] = {
                "lookup_time": lookup_mean,
                "cache_stats": cache_stats,
            }

            print(
                f"     Lookup time: {lookup_mean:.3f}s, Cache size: {cache_stats['cache_size']}"
            )

        print(f"📊 Scalability Results:")
        for cache_size, result in scalability_results.items():
            print(
                f"   {cache_size:3d} entries: {result['lookup_time']:.3f}s lookup time"
            )

        self.results["scalability"] = scalability_results
        return scalability_results

    def benchmark_concurrent_performance(
        self, num_threads: int = 8, operations_per_thread: int = 10
    ):
        """Benchmark concurrent cache performance."""
        print(
            f"\n🔀 Benchmarking concurrent performance ({num_threads} threads, {operations_per_thread} ops each)"
        )

        import threading
        from concurrent.futures import ThreadPoolExecutor

        DataContext.get_current().enable_stage_cache = True
        clear_stage_cache()

        def worker_function(thread_id):
            """Worker function for concurrent testing."""
            results = []
            start_time = time.time()

            for i in range(operations_per_thread):
                if i % 3 == 0:  # Some identical operations (should hit cache)
                    ds = ray.data.range(20).map(lambda x: {"shared": x["id"] * 2})
                else:  # Some unique operations
                    ds = ray.data.range(20).map(
                        lambda x, t=thread_id, i=i: {"unique": x["id"] * t * i}
                    )

                mat = ds.materialize()
                results.append(mat.count())

            end_time = time.time()
            return thread_id, end_time - start_time, results

        # Run concurrent operations
        start_time = time.time()
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(worker_function, i) for i in range(num_threads)]
            results = [future.result() for future in futures]
        total_time = time.time() - start_time

        # Analyze results
        thread_times = [time_taken for _, time_taken, _ in results]
        avg_thread_time = statistics.mean(thread_times)
        cache_stats = get_stage_cache_stats()

        total_operations = num_threads * operations_per_thread
        operations_per_second = total_operations / total_time

        print(f"📊 Concurrent Results:")
        print(f"   Total time:        {total_time:.3f}s")
        print(f"   Avg thread time:   {avg_thread_time:.3f}s")
        print(f"   Operations/sec:    {operations_per_second:.1f}")
        print(f"   Cache hits:        {cache_stats['hits']}")
        print(f"   Cache misses:      {cache_stats['misses']}")
        print(f"   Hit rate:          {cache_stats['hit_rate']:.1%}")

        self.results["concurrent_performance"] = {
            "num_threads": num_threads,
            "operations_per_thread": operations_per_thread,
            "total_time": total_time,
            "operations_per_second": operations_per_second,
            "cache_stats": cache_stats,
        }

        return operations_per_second

    def run_full_benchmark_suite(self):
        """Run the complete benchmark suite."""
        print("🚀 Starting Ray Data Stage Cache Benchmark Suite")
        print("=" * 60)

        self.setup_ray()

        try:
            # Benchmark 1: Cache vs No Cache
            self.benchmark_cache_vs_no_cache(dataset_size=500, num_iterations=8)

            # Benchmark 2: Fingerprint Performance
            self.benchmark_fingerprint_performance(num_operations=200)

            # Benchmark 3: Cache Scalability
            self.benchmark_cache_scalability([10, 25, 50, 100])

            # Benchmark 4: Concurrent Performance
            self.benchmark_concurrent_performance(
                num_threads=6, operations_per_thread=8
            )

        finally:
            self.cleanup()

        print("\n" + "=" * 60)
        print("🏁 Benchmark Suite Complete!")

        return self.results

    def print_summary(self):
        """Print a summary of all benchmark results."""
        print("\n📋 BENCHMARK SUMMARY")
        print("-" * 40)

        if "cache_vs_no_cache" in self.results:
            result = self.results["cache_vs_no_cache"]
            print(f"Cache Speedup:        {result['speedup']:.2f}x")
            print(f"Cache Hit Rate:       {result['cache_stats']['hit_rate']:.1%}")

        if "fingerprint_performance" in self.results:
            result = self.results["fingerprint_performance"]
            print(
                f"Fingerprint Time:     {result['time_per_operation_ms']:.2f}ms per operation"
            )

        if "concurrent_performance" in self.results:
            result = self.results["concurrent_performance"]
            print(
                f"Concurrent Throughput: {result['operations_per_second']:.1f} ops/sec"
            )

        print("-" * 40)


def main():
    """Main function to run benchmarks."""
    benchmark = StageCacheBenchmark()

    try:
        results = benchmark.run_full_benchmark_suite()
        benchmark.print_summary()

        print(f"\n💾 Raw results available in benchmark.results:")
        for key, value in results.items():
            print(f"   {key}: {value}")

    except Exception as e:
        print(f"❌ Benchmark failed: {e}")
        raise
    finally:
        if ray.is_initialized():
            ray.shutdown()


if __name__ == "__main__":
    main()
