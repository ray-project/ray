import ray


def main():
    """Submits custom resource request."""
    # Workers and head are annotated as having 5 "Custom2" capacity each,
    # so this should trigger upscaling of two workers.
    # (One of the bundles will be "placed" on the head.)
    ray.autoscaler.sdk.request_resources(
        bundles=[{"Custom2": 3}, {"Custom2": 3}, {"Custom2": 3}]
    )


if __name__ == "__main__":
    ray.init("auto")
    main()
