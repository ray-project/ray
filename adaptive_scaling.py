import ray
from ray.autoscaler.sdk import request_resources

# Initialize Ray
ray.init()

def adaptive_scaling(min_nodes, max_nodes, upper_threshold=0.8, lower_threshold=0.3):
    while True:
        # Get current cluster stats
        cluster_stats = ray.cluster_resources()
        used_cpus = cluster_stats.get("CPU", 0)
        available_cpus = cluster_stats.get("CPU_available", 0)
        utilization = used_cpus / (used_cpus + available_cpus)

        # Scale up if usage exceeds upper threshold
        if utilization > upper_threshold and len(ray.nodes()) < max_nodes:
            request_resources(num_cpus=available_cpus + 1)  # Request additional resources

        # Scale down if usage is below lower threshold
        elif utilization < lower_threshold and len(ray.nodes()) > min_nodes:
            request_resources(num_cpus=available_cpus - 1)  # Reduce resources

        # Sleep to avoid rapid scaling adjustments
        time.sleep(5)  # Adjust based on use case
