import ray


def main():
    """This script runs in a container with 1 CPU limit and 1G memory limit.
    Validate that Ray reads the correct limits.
    """
    cpu_limit = ray._private.utils.get_num_cpus()
    mem_limit_gb = round(ray._private.utils.get_system_memory() / 10**9, 2)
    assert cpu_limit == 1, cpu_limit
    assert mem_limit_gb == 1.00, mem_limit_gb
    print(f"Confirmed cpu limit {cpu_limit}.")
    print(f"Confirmed memory limit {mem_limit_gb} gigabyte.")


if __name__ == "__main__":
    main()
