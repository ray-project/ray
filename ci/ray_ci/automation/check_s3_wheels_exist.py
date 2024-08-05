import click
import time
import sys
from ci.ray_ci.automation.ray_wheels_lib import check_wheels_exist_on_s3


@click.command()
@click.option("--ray_version", required=True, type=str)
@click.option("--commit_hash", required=True, type=str)
def main(ray_version, commit_hash):
    all_wheels_exist = False
    start_time = time.time()
    while not all_wheels_exist:
        current_time = time.time()
        # If the check takes more than 2 hours, fail job
        if current_time - start_time > 7200:
            sys.exit(42)
        time.sleep(300)
        all_wheels_exist = check_wheels_exist_on_s3(commit_hash=commit_hash, ray_version=ray_version)

if __name__ == "__main__":
    main()
