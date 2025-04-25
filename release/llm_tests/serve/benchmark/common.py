"""Common utilities shared between release tests"""

import argparse
import json
import logging
import os
import subprocess
from typing import Dict, List, Optional
from urllib.parse import urlparse

import anyscale
import boto3
import yaml
from anyscale import service
from anyscale.service.models import ServiceConfig, ServiceState
from constants import (
    DEFAULT_CLOUD,
    RAYLLM_RELEASE_TEST_COMPUTE_CONFIG_NAME,
)
from util import get_python_version_from_image

logger = logging.getLogger(__file__)
logging.basicConfig(
    format="%(asctime)s %(message)s",
    datefmt="%m/%d/%Y %H:%M:%S",
    level=logging.INFO,
)


def terminate_service_if_running(service_name: str, cloud_name: str):
    try:
        status = service.status(name=service_name, cloud=cloud_name)
    except RuntimeError:
        return

    if status.state != ServiceState.TERMINATED:
        logger.info(
            f"Service {service_name} is in state {status.state}. Terminating it before running the benchmark."
        )
        service.terminate(name=service_name, cloud=cloud_name)
        service.wait(name=service_name, cloud=cloud_name, state=ServiceState.TERMINATED)
        logger.info(f"Service {service_name} is now terminated.")


def create_service(image_name: str, application_config: Dict, service_name: str):
    """Creates a service."""

    service_config = ServiceConfig(
        name=service_name,
        image_uri=image_name,
        applications=[application_config],
        query_auth_token_enabled=False,
        compute_config=RAYLLM_RELEASE_TEST_COMPUTE_CONFIG_NAME,
    )

    # If the service already exists, terminate and the start a new service
    # so the new service starts immediately. Otherwise, start a new service
    # without a canary_percent.
    terminate_service_if_running(service_name=service_name, cloud_name=DEFAULT_CLOUD)

    service.deploy(config=service_config)

    # Wait for the service to start running.
    service.wait(
        service_name,
        cloud=DEFAULT_CLOUD,
        state=ServiceState.RUNNING,
        timeout_s=1200,
    )

    status = service.status(name=service_name, cloud=DEFAULT_CLOUD)

    service_metadata = {
        "api_url": status.query_url,
        "api_token": status.query_auth_token,
        "cloud_name": DEFAULT_CLOUD,
        "service_name": service_name,
        "py_version": get_python_version_from_image(image_name),
    }

    return service_metadata


def print_service_url(service_name: str):
    """Prints the serivce's query URL.
    This lets the bash script read the URL and save it.
    """

    status = anyscale.service.status(name=service_name, cloud=DEFAULT_CLOUD)
    print(status.query_url)


def _find_latest_yaml(directory):
    # List to store matched files
    matched_files = []

    # Iterate through the files in the given directory
    for filename in os.listdir(directory):
        if filename.startswith("serve_") and filename.endswith(".yaml"):
            matched_files.append(filename)

    if not matched_files:
        return None

    # Find the file with the latest timestamp by sorting the filenames
    latest_file = max(matched_files)

    return os.path.join(directory, latest_file)


def generate_config(start_args: str):
    command = ["rayllm", "gen-config"] + start_args.split()

    # Run the command and wait for it to complete
    result = subprocess.run(command, capture_output=True, text=True)

    # Check if the command was successful
    if result.returncode != 0:
        print(
            f'Command "{command}" failed with return code {result.returncode}.\n\n'
            f"Command output: {result.stdout}\n\n"
            f"Command error: {result.stderr}\n\n"
        )
        return None

    # Find the latest YAML file in the current directory
    return _find_latest_yaml(".")


def read_yaml(file_path: str) -> Dict:
    if not file_path.endswith(".yaml"):
        raise RuntimeError(
            "Must pass in a Serve config yaml file using the -f option. Got "
            f'file path "{file_path}", which does not end in ".yaml".'
        )
    if not os.path.exists(file_path):
        raise RuntimeError(f"File path {file_path} does not exist.")

    with open(file_path, "r") as f:
        return yaml.safe_load(f)


def update_service_metadata(service_metadata: dict, parsed_args: argparse.Namespace):
    service_metadata.update(
        {
            "tag": parsed_args.tag,
        }
    )
    return service_metadata


def parse_benchmark_args(parser: Optional[argparse.ArgumentParser] = None):
    def parse_comma_separated_ints(value):
        """Convert comma-separated string to list of integers."""
        try:
            return [int(x.strip()) for x in value.split(",")]
        except ValueError:
            raise argparse.ArgumentTypeError(
                f"Invalid value: {value}. Please provide comma-separated integers (e.g., 1,2,3,4)"
            ) from None

    if parser is None:
        parser = argparse.ArgumentParser()

    parser.add_argument(
        "-i",
        "--image-name",
        type=str,
        required=True,
        default="The Docker image to use when running the service.",
    )

    # Instead of passing the configs directly we pass the cli args that when passed to
    # rayllm gen-config script should create the corresponding config file
    parser.add_argument(
        "--start-args",
        type=str,
        required=True,
        default="The arguments to pass to the rayllm start script.",
    )

    parser.add_argument(
        "--prompt-tokens",
        default=256,
        help="Input size",
    )

    parser.add_argument(
        "--max-tokens",
        default=64,
        help="Output size",
    )

    parser.add_argument(
        "--stream",
        "-s",
        action="store_true",
        help="Whether streaming should be on or off",
    )

    parser.add_argument(
        "--concurrency",
        "-c",
        default="1,2,4,8,16,32",
        type=parse_comma_separated_ints,
        help="Comma-separated list of concurrency values (e.g., 1,2,3,4)",
    )

    parser.add_argument(
        "--run-time",
        default="60s",
        type=str,
        help="Runtime of each benchmarking experiment",
    )

    parser.add_argument(
        "--tag",
        "-t",
        type=str,
        help="Optional tag to add to the uploaded metadata to be able to separate experiments from each other (e.g. tp2)",
    )

    return parser


def write_to_s3(data_to_write: List[Dict], s3_path: str):
    # Parse S3 path using urllib
    parsed_url = urlparse(s3_path)
    bucket_name = parsed_url.netloc
    # Remove leading slash and get the rest of the path
    key = parsed_url.path.lstrip("/")

    # If no file extension provided, append .json
    if not any(key.endswith(ext) for ext in [".jsonl", ".json"]):
        key += ".jsonl"

    s3_client = boto3.client("s3")

    try:
        # Convert data to JSON string
        jsonl_data = "\n".join(json.dumps(record) for record in data_to_write)

        # Upload to S3
        s3_client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=jsonl_data,
            ContentType="application/x-jsonlines",  # MIME type for JSONL
        )

        logging.info(f"Successfully wrote {len(data_to_write)} records to {s3_path}")

    except Exception as e:
        logging.error(f"Failed to write to S3: {str(e)}")
        raise


def read_from_s3(s3_path: str) -> List[Dict]:
    # Parse S3 path using urllib
    parsed_url = urlparse(s3_path)
    bucket_name = parsed_url.netloc
    key = parsed_url.path.lstrip("/")

    # Initialize S3 client
    s3_client = boto3.client("s3")

    try:
        # Get the object from S3
        response = s3_client.get_object(Bucket=bucket_name, Key=key)

        # Read the data
        data = response["Body"].read().decode("utf-8")

        records = [
            json.loads(line)
            for line in data.splitlines()
            if line.strip()  # Skip empty lines
        ]

        logging.info(f"Successfully read {len(records)} records from {s3_path}")
        return records

    except Exception as e:
        logging.error(f"Failed to read from S3: {str(e)}")
        raise
