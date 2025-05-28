"""Common utilities shared between release tests"""

import json
import logging
import os
from typing import Dict, List, Any
from urllib.parse import urlparse

import boto3
import yaml

logger = logging.getLogger(__file__)
logging.basicConfig(
    format="%(asctime)s %(message)s",
    datefmt="%m/%d/%Y %H:%M:%S",
    level=logging.INFO,
)


def get_llm_config(serve_config_file: List[Dict]) -> List[Any]:
    """Get the first llm_config from serve config file."""
    with open(serve_config_file, "r") as f:
        loaded_llm_config = yaml.safe_load(f)

    applications = loaded_llm_config["applications"]
    config = applications[0]["args"]["llm_configs"][0]
    if isinstance(config, dict):
        return config

    assert isinstance(config, str)
    with open(config, "r") as f:
        loaded_llm_config = yaml.safe_load(f)

    return loaded_llm_config


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
