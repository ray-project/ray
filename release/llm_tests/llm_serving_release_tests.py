from anyscale_service_utils import start_service

CLOUD = "serve_release_tests_cloud"


def main():

    with start_service(
        service_name="replica-scalability",
        image_uri=image_uri,
        compute_config=compute_config,
        applications=[noop_1k_application],
        working_dir="workloads",
        cloud=CLOUD,
    ) as service_name:
        pass


if __name__ == "__main__":
    main()
