import boto3
import botocore
import subprocess
import tarfile
import os
import click
from botocore import UNSIGNED
from botocore.client import Config
import time
import requests

S3_BUCKET = "ray-ci-results"
DOC_BUILD_DIR_S3 = "doc_build"
LAST_BUILD_CUTOFF = 3  # how many days ago to consider a build outdated
PENDING_FILES_PATH = "pending_files.txt"
ENVIRONMENT_PICKLE = "_build/doctrees/environment.pickle"
DOC_BUILD_S3_URL = "https://ray-ci-results.s3.us-west-2.amazonaws.com/doc_build"


def find_latest_master_commit():
    """Find latest commit that was pushed to origin/master that is also on local env."""
    latest_commits = (
        subprocess.check_output(
            [
                "git",
                "log",
                "-n",
                "100",
                "--format=%H",
            ]
        )
        .strip()
        .decode("utf-8")
        .split("\n")
    )
    for commit in latest_commits:
        result = requests.head(f"{DOC_BUILD_S3_URL}/{commit}.tgz")
        if result.status_code == 200:
            return commit
    raise Exception(
        "No cache found for latest master commit."
        "Please merge with upstream master or use 'make develop'."
    )


def fetch_cache_from_s3(commit, target_file_path):
    """
    Fetch doc cache archive from ray-ci-results S3 bucket

    Args:
        commit: The commit hash of the doc cache to fetch
        target_file_path: The file path to save the doc cache archive
    """
    # Create an S3 client
    s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))
    s3_file_path = f"{DOC_BUILD_DIR_S3}/{commit}.tgz"
    try:
        print(f"Fetching doc cache from commit {commit}...")
        s3.download_file(S3_BUCKET, s3_file_path, target_file_path)
        print(f"Successfully downloaded {s3_file_path} to {target_file_path}")
    except botocore.exceptions.ClientError as e:
        print(f"Failed to download {s3_file_path} from S3: {str(e)}")
        raise e


def extract_cache(cache_path: str, doc_dir: str):
    """
    Extract the doc cache archive to overwrite the ray/doc directory

    Args:
        file_path: The file path of the doc cache archive
    """
    with tarfile.open(cache_path, "r:gz") as tar:
        tar.extractall(doc_dir)
    print(f"Extracted {cache_path} to {doc_dir}")


def list_changed_and_added_files(ray_dir: str, latest_master_commit: str):
    """
    List all changed and added untracked files in the repo.
    This is to prevent cache environment from updating timestamp of these files.
    """
    untracked_files = (
        subprocess.check_output(
            ["git", "ls-files", "--others"],
            cwd=ray_dir,
        )
        .decode("utf-8")
        .split(os.linesep)
    )
    modified_files = (
        subprocess.check_output(
            ["git", "ls-files", "--modified"],
            cwd=ray_dir,
        )
        .decode("utf-8")
        .split(os.linesep)
    )
    diff_files_with_master = (
        subprocess.check_output(
            ["git", "diff", "--name-only", latest_master_commit],
            cwd=ray_dir,
        )
        .decode("utf-8")
        .split(os.linesep)
    )
    filenames = []
    for file in untracked_files + modified_files + diff_files_with_master:
        filename = file
        if filename.startswith("doc/"):  # Remove "doc/" prefix
            filename = filename.replace("doc/", "")
        if filename.startswith("source/"):  # Remove "doc/" prefix
            filename = filename.replace("source/", "")
        filenames.append(filename)
    return filenames


def should_load_cache(ray_dir: str):
    """
    Check if cache should be loaded based on the timestamp of last build.
    """
    ray_doc_dir = os.path.join(ray_dir, "doc")
    if not os.path.exists(f"{ray_doc_dir}/{ENVIRONMENT_PICKLE}"):
        print("Doc build environment pickle file does not exist.")
        return True
    last_build_time = os.path.getmtime(f"{ray_doc_dir}/{ENVIRONMENT_PICKLE}")
    current_time = time.time()
    # Load cache if last build was more than LAST_BUILD_CUTOFF days ago
    print("time diff: ", current_time - last_build_time)
    if current_time - last_build_time > LAST_BUILD_CUTOFF * 60 * 60 * 24:
        print(f"Last build was more than {LAST_BUILD_CUTOFF} days ago.")
        return True
    return False


@click.command()
@click.option("--ray-dir", default="/ray", help="Path to Ray repo")
def main(ray_dir: str) -> None:
    if not should_load_cache(ray_dir):
        print("Skip loading global cache...")
        return
    print("Loading global cache ...")
    latest_master_commit = find_latest_master_commit()
    # List all changed and added files in the repo
    filenames = list_changed_and_added_files(ray_dir, latest_master_commit)
    with open(
        f"{ray_dir}/{PENDING_FILES_PATH}", "w"
    ) as f:  # Save to file to be used when updating cache environment
        f.write("\n".join(filenames))

    cache_path = f"{ray_dir}/doc.tgz"
    # Fetch cache of that commit from S3 to cache_path
    fetch_cache_from_s3(latest_master_commit, cache_path)
    # Extract cache to override ray/doc directory
    extract_cache(cache_path, f"{ray_dir}/doc")
    os.remove(cache_path)


if __name__ == "__main__":
    main()
