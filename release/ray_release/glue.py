from ray_release.config import Test


def run_release_test(test: Test, ray_wheels_url: str):
    run_type = test["run"].get("type", "command")
