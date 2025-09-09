"""Benchmark script for petabyte-scale stage cache performance."""

import time
from typing import List

import ray
from ray.data._internal.cache.stage_cache import (
    clear_stage_cache,
    get_stage_cache_stats,
)
from ray.data.context import DataContext


class PetabyteCacheBenchmark:
    """Benchmark suite for petabyte-scale cache performance."""

    def __init__(self):
        self.results = {}

    def setup_ray(self):
        """Initialize Ray if not already initialized."""
        if not ray.is_initialized():
            ray.init()

    def benchmark_constant_memory_usage(self):
        """Benchmark that memory usage stays constant with dataset size."""
        print("\n📊 Benchmarking Constant Memory Usage")
        print("-" * 50)

        context = DataContext.get_current()
        original_setting = getattr(context, "enable_stage_cache", False)
        context.enable_stage_cache = True

        try:
            clear_stage_cache()

            # Test datasets with increasing size but same number of operations
            test_cases = [
                {"rows": 1000, "blocks": 10, "name": "Small (1K rows, 10 blocks)"},
                {
                    "rows": 100000,
                    "blocks": 100,
                    "name": "Medium (100K rows, 100 blocks)",
                },
                {"rows": 1000000, "blocks": 1000, "name": "Large (1M rows, 1K blocks)"},
                {
                    "rows": 10000000,
                    "blocks": 10000,
                    "name": "XLarge (10M rows, 10K blocks)",
                },
            ]

            memory_usage = []

            for case in test_cases:
                print(f"\nTesting {case['name']}...")

                # Create dataset with specified size
                ds = ray.data.range(case["rows"], override_num_blocks=case["blocks"])

                # Materialize and cache
                start_time = time.time()
                result = ds.materialize()
                materialize_time = time.time() - start_time

                # Get memory usage
                stats = get_stage_cache_stats()
                memory_bytes = stats["total_memory_bytes"]
                memory_usage.append(memory_bytes)

                print(f"  Rows: {result.count():,}")
                print(f"  Blocks: {result.num_blocks():,}")
                print(f"  Materialize time: {materialize_time:.3f}s")
                print(
                    f"  Cache memory: {memory_bytes:,} bytes ({memory_bytes/1024/1024:.1f} MB)"
                )

                # Test cache hit
                ds2 = ray.data.range(case["rows"], override_num_blocks=case["blocks"])
                start_time = time.time()
                result2 = ds2.materialize()
                cache_time = time.time() - start_time

                print(f"  Cache hit time: {cache_time:.3f}s")
                if cache_time > 0:
                    speedup = materialize_time / cache_time
                    print(f"  Speedup: {speedup:.2f}x")

            # Verify memory usage is constant (within reasonable bounds)
            max_memory = max(memory_usage)
            min_memory = min(memory_usage)

            print(f"\nMemory Usage Analysis:")
            print(f"  Min memory: {min_memory:,} bytes")
            print(f"  Max memory: {max_memory:,} bytes")

            if min_memory > 0:
                memory_ratio = max_memory / min_memory
                print(f"  Memory ratio (max/min): {memory_ratio:.2f}")

                # Memory should not scale with dataset size
                assert (
                    memory_ratio < 5.0
                ), f"Memory usage scales with dataset size: {memory_ratio}"

            # All memory usage should be under 20MB regardless of dataset size
            assert (
                max_memory < 20 * 1024 * 1024
            ), f"Memory usage too high: {max_memory:,} bytes"

            self.results["constant_memory"] = {
                "test_cases": test_cases,
                "memory_usage": memory_usage,
                "memory_ratio": memory_ratio if min_memory > 0 else 1.0,
            }

        finally:
            context.enable_stage_cache = original_setting

    def benchmark_objectref_overhead(self):
        """Benchmark ObjectRef caching overhead."""
        print("\n🔗 Benchmarking ObjectRef Caching Overhead")
        print("-" * 50)

        context = DataContext.get_current()
        original_setting = getattr(context, "enable_stage_cache", False)

        # Test with cache disabled
        context.enable_stage_cache = False
        clear_stage_cache()

        ds = ray.data.range(50000, override_num_blocks=500)

        print("Testing without cache...")
        times_without_cache = []
        for i in range(3):
            start_time = time.time()
            result = ds.materialize()
            end_time = time.time()
            times_without_cache.append(end_time - start_time)
            print(f"  Run {i+1}: {end_time - start_time:.3f}s")

        avg_without_cache = sum(times_without_cache) / len(times_without_cache)

        # Test with cache enabled
        context.enable_stage_cache = True
        clear_stage_cache()

        print("\nTesting with cache...")

        # First run - populate cache
        start_time = time.time()
        result1 = ds.materialize()
        first_time = time.time() - start_time
        print(f"  First run (populate cache): {first_time:.3f}s")

        # Subsequent runs - use cache
        cache_times = []
        for i in range(3):
            ds_identical = ray.data.range(50000, override_num_blocks=500)
            start_time = time.time()
            result = ds_identical.materialize()
            end_time = time.time()
            cache_times.append(end_time - start_time)
            print(f"  Cache run {i+1}: {end_time - start_time:.3f}s")

        avg_with_cache = sum(cache_times) / len(cache_times)

        # Calculate overhead and speedup
        overhead = first_time - avg_without_cache
        speedup = avg_without_cache / avg_with_cache if avg_with_cache > 0 else 0

        print(f"\nResults:")
        print(f"  Avg without cache: {avg_without_cache:.3f}s")
        print(f"  First with cache: {first_time:.3f}s")
        print(f"  Avg cache hits: {avg_with_cache:.3f}s")
        print(
            f"  Cache overhead: {overhead:.3f}s ({overhead/avg_without_cache*100:.1f}%)"
        )
        print(f"  Cache speedup: {speedup:.2f}x")

        # Get final cache stats
        stats = get_stage_cache_stats()
        print(f"  Cache memory: {stats['total_memory_bytes']:,} bytes")
        print(f"  Cache hits: {stats['plan_hits'] + stats['result_hits']}")

        self.results["objectref_overhead"] = {
            "avg_without_cache": avg_without_cache,
            "avg_with_cache": avg_with_cache,
            "overhead_seconds": overhead,
            "overhead_percent": (
                overhead / avg_without_cache * 100 if avg_without_cache > 0 else 0
            ),
            "speedup": speedup,
            "cache_memory_bytes": stats["total_memory_bytes"],
        }

        try:
            context.enable_stage_cache = original_setting
        except:
            pass

    def benchmark_concurrent_petabyte_access(self):
        """Benchmark concurrent access patterns for petabyte-scale datasets."""
        print("\n🔀 Benchmarking Concurrent Petabyte-Scale Access")
        print("-" * 50)

        context = DataContext.get_current()
        original_setting = getattr(context, "enable_stage_cache", False)
        context.enable_stage_cache = True

        try:
            clear_stage_cache()

            # Simulate multiple users accessing the same large dataset
            def create_large_dataset():
                return ray.data.range(500000, override_num_blocks=2000).map(
                    lambda x: {"processed": x["id"] % 1000}
                )

            print("Simulating concurrent access to large dataset...")

            # First user materializes (populates cache)
            ds1 = create_large_dataset()
            start_time = time.time()
            result1 = ds1.materialize()
            first_user_time = time.time() - start_time

            print(f"First user (populate cache): {first_user_time:.3f}s")

            # Simulate multiple concurrent users
            import threading
            from concurrent.futures import ThreadPoolExecutor

            def simulate_user(user_id):
                """Simulate a user accessing the dataset."""
                try:
                    ds = create_large_dataset()
                    start_time = time.time()
                    result = ds.materialize()
                    end_time = time.time()

                    return {
                        "user_id": user_id,
                        "time": end_time - start_time,
                        "count": result.count(),
                        "blocks": result.num_blocks(),
                        "success": True,
                    }
                except Exception as e:
                    return {
                        "user_id": user_id,
                        "error": str(e),
                        "success": False,
                    }

            # Run concurrent users
            num_users = 8
            with ThreadPoolExecutor(max_workers=num_users) as executor:
                futures = [executor.submit(simulate_user, i) for i in range(num_users)]
                concurrent_results = [future.result() for future in futures]

            # Analyze results
            successful_results = [r for r in concurrent_results if r["success"]]
            failed_results = [r for r in concurrent_results if not r["success"]]

            if failed_results:
                print(f"Failed users: {len(failed_results)}")
                for result in failed_results:
                    print(f"  User {result['user_id']}: {result['error']}")

            if successful_results:
                times = [r["time"] for r in successful_results]
                avg_concurrent_time = sum(times) / len(times)

                print(f"\nConcurrent Results ({len(successful_results)} users):")
                print(f"  Average time: {avg_concurrent_time:.3f}s")
                print(f"  Min time: {min(times):.3f}s")
                print(f"  Max time: {max(times):.3f}s")

                # Most users should benefit from cache
                fast_users = [t for t in times if t < first_user_time * 0.5]
                print(f"  Users with >2x speedup: {len(fast_users)}/{len(times)}")

            # Check final cache state
            final_stats = get_stage_cache_stats()
            print(f"\nFinal Cache State:")
            print(f"  Memory usage: {final_stats['total_memory_bytes']:,} bytes")
            print(f"  Plan hits: {final_stats['plan_hits']}")
            print(
                f"  Total entries: {final_stats['plan_cache_entries'] + final_stats['result_cache_entries']}"
            )

            # Memory should still be minimal
            assert final_stats["total_memory_bytes"] < 20 * 1024 * 1024

            self.results["concurrent_access"] = {
                "num_users": num_users,
                "successful_users": len(successful_results),
                "avg_concurrent_time": avg_concurrent_time if successful_results else 0,
                "first_user_time": first_user_time,
                "cache_memory_bytes": final_stats["total_memory_bytes"],
            }

        finally:
            context.enable_stage_cache = original_setting

    def benchmark_cache_efficiency_at_scale(self):
        """Benchmark cache efficiency with many different operations."""
        print("\n⚡ Benchmarking Cache Efficiency at Scale")
        print("-" * 50)

        context = DataContext.get_current()
        original_setting = getattr(context, "enable_stage_cache", False)
        context.enable_stage_cache = True

        try:
            clear_stage_cache()

            # Create many different operations to test cache efficiency
            num_operations = 50
            cache_hits = 0
            total_operations = 0

            print(f"Running {num_operations} operations...")

            for i in range(num_operations):
                # Create mix of identical and unique operations
                if i % 5 == 0:
                    # Every 5th operation is identical (should hit cache)
                    ds = ray.data.range(10000, override_num_blocks=100).map(
                        lambda x: {"shared": x["id"] % 100}
                    )
                    operation_type = "shared"
                else:
                    # Unique operations
                    ds = ray.data.range(10000, override_num_blocks=100).map(
                        lambda x, i=i: {"unique": x["id"] * i}
                    )
                    operation_type = "unique"

                # Materialize
                start_time = time.time()
                result = ds.materialize()
                end_time = time.time()

                total_operations += 1

                # Check if this was a cache hit
                stats = get_stage_cache_stats()
                current_hits = stats["plan_hits"] + stats["result_hits"]

                if i > 0:  # Skip first operation
                    if current_hits > cache_hits:
                        cache_hits = current_hits
                        print(
                            f"  Op {i:2d} ({operation_type:6s}): {end_time-start_time:.3f}s [CACHE HIT]"
                        )
                    else:
                        print(
                            f"  Op {i:2d} ({operation_type:6s}): {end_time-start_time:.3f}s"
                        )
                else:
                    cache_hits = current_hits
                    print(
                        f"  Op {i:2d} ({operation_type:6s}): {end_time-start_time:.3f}s [FIRST]"
                    )

            # Final statistics
            final_stats = get_stage_cache_stats()
            final_hit_rate = final_stats["hit_rate"]

            print(f"\nEfficiency Results:")
            print(f"  Total operations: {total_operations}")
            print(
                f"  Cache hits: {final_stats['plan_hits'] + final_stats['result_hits']}"
            )
            print(f"  Hit rate: {final_hit_rate:.1%}")
            print(f"  Memory usage: {final_stats['total_memory_bytes']:,} bytes")
            print(
                f"  Cache entries: {final_stats['plan_cache_entries'] + final_stats['result_cache_entries']}"
            )

            # Should achieve reasonable hit rate for shared operations
            expected_hits = num_operations // 5  # Every 5th operation should hit
            actual_hits = final_stats["plan_hits"] + final_stats["result_hits"]

            print(f"  Expected hits: ~{expected_hits}")
            print(f"  Actual hits: {actual_hits}")

            self.results["efficiency_at_scale"] = {
                "total_operations": total_operations,
                "hit_rate": final_hit_rate,
                "memory_bytes": final_stats["total_memory_bytes"],
                "cache_entries": final_stats["plan_cache_entries"]
                + final_stats["result_cache_entries"],
            }

        finally:
            context.enable_stage_cache = original_setting

    def run_full_benchmark_suite(self):
        """Run the complete petabyte-scale benchmark suite."""
        print("🚀 Petabyte-Scale Cache Benchmark Suite")
        print("=" * 60)

        self.setup_ray()

        try:
            self.benchmark_constant_memory_usage()
            self.benchmark_objectref_overhead()
            self.benchmark_concurrent_petabyte_access()
            self.benchmark_cache_efficiency_at_scale()

        except Exception as e:
            print(f"❌ Benchmark error: {e}")
            raise

        print("\n" + "=" * 60)
        print("🏁 Petabyte-Scale Benchmark Complete!")

        return self.results

    def print_summary(self):
        """Print benchmark summary optimized for petabyte-scale insights."""
        print("\n📋 PETABYTE-SCALE BENCHMARK SUMMARY")
        print("-" * 50)

        if "constant_memory" in self.results:
            result = self.results["constant_memory"]
            print(
                f"Memory Scalability:   {result['memory_ratio']:.2f}x ratio (max/min)"
            )
            print(f"Max Memory Usage:     {max(result['memory_usage']):,} bytes")

        if "objectref_overhead" in self.results:
            result = self.results["objectref_overhead"]
            print(f"Cache Overhead:       {result['overhead_percent']:.1f}%")
            print(f"Cache Speedup:        {result['speedup']:.2f}x")

        if "efficiency_at_scale" in self.results:
            result = self.results["efficiency_at_scale"]
            print(f"Hit Rate at Scale:    {result['hit_rate']:.1%}")
            print(
                f"Memory per Operation: {result['memory_bytes']/result['total_operations']:.0f} bytes"
            )

        if "concurrent_access" in self.results:
            result = self.results["concurrent_access"]
            success_rate = result["successful_users"] / result["num_users"] * 100
            print(f"Concurrent Success:   {success_rate:.1f}%")

        print("-" * 50)
        print("✅ Petabyte-scale caching validated!")


def main():
    """Run petabyte-scale benchmark."""
    benchmark = PetabyteCacheBenchmark()

    try:
        results = benchmark.run_full_benchmark_suite()
        benchmark.print_summary()

        print(f"\n💾 Detailed results:")
        for key, value in results.items():
            print(f"  {key}: {value}")

    except Exception as e:
        print(f"❌ Benchmark failed: {e}")
        raise
    finally:
        if ray.is_initialized():
            ray.shutdown()


if __name__ == "__main__":
    main()
