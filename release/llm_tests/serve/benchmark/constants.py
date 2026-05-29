"""This file contains constants used throughout the Buildkite files."""

# Config for the rayllm release test.
RAYLLM_RELEASE_TEST_SERVICE_NAME = "rayllm_release_test_service"
RAYLLM_RELEASE_TEST_COMPUTE_CONFIG_NAME = "rayllm-release-test"

DEFAULT_CLOUD = "serve_release_tests_cloud"

CLOUD_PROVIDER_TO_CLOUD_NAME = {
    "aws": DEFAULT_CLOUD,
    "gcp": "anyscale_gcp_public_default_cloud_us_west_1",
}
