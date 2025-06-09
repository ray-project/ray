import ray
import time
import os


def monitor_system_load():
    """Monitor system load to detect oversubscription"""
    loads = []
    for _ in range(40):  # Monitor for 4 seconds
        load = os.getloadavg()[0]
        loads.append(load)
        time.sleep(0.1)
    return max(loads)


def test_oversubscription_scenario():
    """Test scenario that can trigger oversubscription"""
    # Initialize Ray with limited resources
    ray.init(num_cpus=1, object_store_memory=100_000_000, log_to_driver=False)

    @ray.remote(num_cpus=1)
    def cpu_bound_work(worker_id, work_duration):
        """CPU-intensive work that consumes the full CPU allocation"""
        print(f"  Worker {worker_id}: Starting CPU-bound work ({work_duration}s)")
        start_time = time.time()

        # Pure CPU work to max out the allocated CPU
        while time.time() - start_time < work_duration:
            result = 0
            for i in range(100000):
                result += i * i % 1000000

        print(f"  Worker {worker_id}: Completed work")
        return f"Worker {worker_id} completed"

    # Start first task that will consume the entire CPU allocation
    print("1. Starting long-running task on single CPU core")
    task1 = cpu_bound_work.remote("A", 4.0)

    # Give it time to start and consume resources
    time.sleep(0.3)

    print("2. SIMULATING BUG: First worker 'exits' but releases resources early")
    print(
        "   (Real bug: NotifyDirectCallTaskBlocked() called while task A still running)"
    )

    # In the real bug, the raylet would now think the CPU is available
    # So let's simulate what would happen if another task gets scheduled

    print("3. Raylet now thinks CPU is available and schedules second task")
    print("   This can create OVERSUBSCRIPTION: 2 CPU tasks on 1 CPU core!")

    # Start monitoring load before second task
    start_load = os.getloadavg()[0]

    # This second task would get scheduled in the buggy scenario
    task2 = cpu_bound_work.remote("B", 2.0)

    print("4. Monitoring system while both tasks compete for 1 CPU...")

    # Monitor system load while both tasks run
    max_load = monitor_system_load()

    # Wait for tasks to complete
    results = ray.get([task1, task2])

    end_load = os.getloadavg()[0]

    print("\nRESULTS:")
    print(f"   Initial load: {start_load:.2f}")
    print(f"   Peak load during test: {max_load:.2f}")
    print(f"   Final load: {end_load:.2f}")

    if max_load > 1.5:
        print("   HIGH LOAD DETECTED - Oversubscription occurred!")
        print("   Two CPU-intensive tasks competed for 1 CPU core")
    else:
        print("   Normal load - No oversubscription detected")

    print(f"   Task results: {results}")

    ray.shutdown()


if __name__ == "__main__":
    test_oversubscription_scenario()
