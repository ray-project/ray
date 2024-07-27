import boto3
import botocore
import subprocess
import tarfile
import os

ray_dir = "/Users/kevin/ray"
S3_BUCKET = "ray-ci-results"
DOC_BUILD_DIR_S3= "doc_build"
def find_latest_commit():
    """Fetch latest commit that was pushed to origin/master."""
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
    s3 = boto3.client('s3')
    s3_file_path = f"{DOC_BUILD_DIR_S3}/{commit}.tgz"
    try:
        print(f"Downloading {commit}.tgz from S3...")
        s3.download_file(S3_BUCKET, s3_file_path, target_file_path)
        print(f"Successfully downloaded {s3_file_path} to {target_file_path}")
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            print("The object does not exist.")
        else:
            raise

def extract_cache(file_path):
    """
    Extract the doc cache archive to the ray/doc directory

    Args:
        file_path (str): The file path of the doc cache archive
    """
    with tarfile.open(file_path, "r:gz") as tar:
        tar.extractall(f"{ray_dir}/doc")
    print(f"Extracted {file_path} to /Users/kevin/ray/doc")

def list_changed_and_added_files():
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

def main() -> None:
    # files = list_changed_and_added_files()
    # with open(f"{ray_dir}/pending_files.txt", "w") as f:
    #     for file in files:
    #         filename = file.split(".")[0]
    #         if filename.startswith("doc/"):
    #             filename = filename.replace("doc/", "")
    #         f.write(filename + "\n")
    commit = find_latest_commit()
    print(commit)
    target_file_path = f"{ray_dir}/doc.tgz"
    fetch_cache_from_s3(commit, target_file_path)
    extract_cache(target_file_path)

if __name__ == "__main__":
    main()
# object_name = f"doc_build/{commit}.tgz"
# file_name = f"{ray_dir}/doc.tgz"
# fetch_cache_from_s3(object_name, file_name)
#extract_cache(f"{ray_dir}/doc.tgz")