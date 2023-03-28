# __without_limit_start__
import ray

# Assume this Ray node has 16 CPUs and 16G memory.
ray.init()


@ray.remote
def process(file):
    # Actual work is reading the file and process the data.
    # Assume it needs to use 2G memory.
    pass


NUM_FILES = 1000
result_refs = []
for i in range(NUM_FILES):
    # By default, process task will use 1 CPU resource and no other resources.
    # This means 16 tasks can run concurrently
    # and will OOM since 32G memory is needed while the node only has 16G.
    result_refs.append(process.remote(f"{i}.csv"))
ray.get(result_refs)
# __without_limit_end__

# __with_limit_start__
result_refs = []
for i in range(NUM_FILES):
    # Now each task will use 2G memory resource
    # and the number of concurrently running tasks is limited to 8.
    # In this case, setting num_cpus to 2 has the same effect.
    result_refs.append(
        process.options(memory=2 * 1024 * 1024 * 1024).remote(f"{i}.csv")
    )
ray.get(result_refs)
# __with_limit_end__
