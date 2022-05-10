import ray


def main():
    """Submits CPU request."""
    ray.autoscaler.sdk.request_resources(num_cpus=2)


if __name__ == "__main__":
    ray.init("auto")
    main()
