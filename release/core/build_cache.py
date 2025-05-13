import subprocess
import json
import requests
from typing import Tuple

RAY_CHECKOUT_DIR = "/tmp/ray-checkout"
BAZEL_CACHE_PORT = 9095
BAZEL_CACHE_SERVER = f"http://localhost:{BAZEL_CACHE_PORT}"


def _get_temporary_credentials() -> Tuple[str, str, str]:
    identity_output = subprocess.run(
        ["aws", "sts", "get-caller-identity"],
        capture_output=True,
        text=True,
        check=True,
    )
    identity_output_json = json.loads(identity_output.stdout)
    account_id = identity_output_json["Account"]
    role = identity_output_json["Arn"].split("/")[1]
    credentials_output = subprocess.run(
        [
            "aws",
            "sts",
            "assume-role",
            "--role-arn",
            f"arn:aws:iam::{account_id}:role/{role}",
            "--role-session-name",
            "ray-cache-server",
            "--duration-seconds",
            "3600",
            "--output",
            "json",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    credentials_output_json = json.loads(credentials_output.stdout)
    return (
        credentials_output_json["Credentials"]["AccessKeyId"],
        credentials_output_json["Credentials"]["SecretAccessKey"],
        credentials_output_json["Credentials"]["SessionToken"],
    )


def run_cache_server(access_key_id: str, secret_access_key: str, session_token: str):
    process = subprocess.Popen(
        [
            "bazel-remote",
            "--s3.bucket=core-bazel-cache",
            "--host=0.0.0.0",
            f"--port={BAZEL_CACHE_PORT}",
            f"--grpc_port={BAZEL_CACHE_PORT + 1}",
            "--dir=/tmp/ray-checkout-cache",
            "--max_size=100",
            "--s3.auth_method=access_key",
            f"--s3.access_key_id={access_key_id}",
            f"--s3.secret_access_key={secret_access_key}",
            f"--s3.session_token={session_token}",
            "--s3.endpoint=s3.amazonaws.com",
        ],
    )

    # Wait for the server to start
    while True:
        try:
            response = requests.get(f"{BAZEL_CACHE_SERVER}/status")
            if response.status_code == 200:
                print("Cache server is running.")
                break
        except requests.exceptions.RequestException:
            pass


def build():
    process = subprocess.Popen(
        [
            "bazel",
            "build",
            "--remote_cache",
            BAZEL_CACHE_SERVER,
            "-c",
            "fastbuild",
            "//:ray_pkg",
        ],
        cwd=RAY_CHECKOUT_DIR,
    )
    process.wait()


def main():
    run_cache_server(*_get_temporary_credentials())
    build()


if __name__ == "__main__":
    main()
