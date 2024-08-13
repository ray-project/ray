import sys
import time

import click

from anyscale.ci.ray_wheels_lib import check_wheels_exist_on_s3

WHEEL_CHECK_TIMEOUT = 7200  # 2 hours
RAY_WHEELS_S3_URL = "https://ray-wheels.s3.us-west-2.amazonaws.com"


@click.command()
@click.option("--branch", required=True, type=str)
@click.option("--commit_hash", required=True, type=str)
@click.option("--ray_version", required=True, type=str)
def main(branch: str, commit_hash: str, ray_version: str):
    start_time = time.time()
    while not check_wheels_exist_on_s3(
        RAY_WHEELS_S3_URL, branch, commit_hash, ray_version
    ):
        current_time = time.time()
        # If the check takes more than 2 hours, fail job
        if current_time - start_time > WHEEL_CHECK_TIMEOUT:
            sys.exit(42)
        print("Wheels are not yet on S3. Sleeping for 5 minutes...")
        time.sleep(300)
    print("All wheels are on S3!")


if __name__ == "__main__":
    main()
