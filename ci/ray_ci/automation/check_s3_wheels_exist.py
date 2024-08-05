import click
import time
import sys
from ci.ray_ci.automation.ray_wheels_lib import check_wheels_exist_on_s3


@click.command()
@click.option("--ray_version", required=True, type=str)
@click.option("--commit_hash", required=True, type=str)
def main(ray_version, commit_hash):
    start_time = time.time()
    while not check_wheels_exist_on_s3(commit_hash=commit_hash, ray_version=ray_version):
        current_time = time.time()
        # If the check takes more than 2 hours, fail job
        if current_time - start_time > 7200:
            sys.exit(42)
        print("Wheels are not yet on S3. Sleeping for 5 minutes...")
        time.sleep(300)
    print("All release wheels are on S3")

if __name__ == "__main__":
    main()
