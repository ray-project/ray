import boto3
import botocore
import subprocess
import tarfile
import os
import click

S3_BUCKET = "ray-ci-results"
DOC_BUILD_DIR_S3= "doc_build"

def find_latest_commit():
    """Find latest commit that was pushed to origin/master that is also on local env."""
    latest_commit = subprocess.check_output(["git", "log", "--pretty=format:%H", "--first-parent", "origin/master", "-n", "1"]).decode("utf-8")
    return latest_commit

def fetch_cache_from_s3(commit, target_file_path):
    """
    Fetch doc cache archive from ray-ci-results S3 bucket
    
    Args:
        commit (str): The commit hash of the doc cache to fetch
        target_file_path (str): The file path to save the doc cache archive
    """
    # Create an S3 client
    s3 = boto3.client("s3")
    s3_file_path = f"{DOC_BUILD_DIR_S3}/{commit}.tgz"
    try:
        print(f"Downloading {commit}.tgz from S3...")
        s3.download_file(S3_BUCKET, s3_file_path, target_file_path)
        print(f"Successfully downloaded {s3_file_path} to {target_file_path}")
    except botocore.exceptions.ClientError as e:
        print(f"Failed to download {s3_file_path} from S3: {str(e)}")
        raise e

def extract_cache(cache_path: str, doc_dir: str):
    """
    Extract the doc cache archive to overwrite the ray/doc directory

    Args:
        file_path (str): The file path of the doc cache archive
    """
    with tarfile.open(cache_path, "r:gz") as tar:
        tar.extractall(doc_dir)
    print(f"Extracted {cache_path} to {doc_dir}")

def list_changed_and_added_files(ray_dir: str):
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
    return untracked_files + modified_files

@click.command()
@click.option("--ray-dir", default="/ray", help="Path to Ray repo")
def main(ray_dir: str) -> None:
    # List all changed and added files in the repo
    files = list_changed_and_added_files(ray_dir)
    with open(f"{ray_dir}/pending_files.txt", "w") as f:
        for file in files:
            # Only keep filename without extension and remove "doc/" prefix
            filename = file.split(".")[0]
            if filename.startswith("doc/"):
                filename = filename.replace("doc/", "")
            f.write(filename + "\n")

    commit = find_latest_commit()
    cache_path = f"{ray_dir}/doc.tgz"
    # Fetch cache of that commit from S3 to cache_path
    fetch_cache_from_s3(commit, cache_path)
    # Extract cache to override ray/doc directory
    extract_cache(cache_path, f"{ray_dir}/doc")

if __name__ == "__main__":
    main()