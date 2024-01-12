import logging
import platform
import subprocess
import sys
import tarfile
from pathlib import Path

import requests

from ray.dashboard.consts import PROMETHEUS_CONFIG_INPUT_PATH

# Configure basic logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def get_system_info():
    os_type = platform.system().lower()
    architecture = platform.machine()
    if architecture == "x86_64":
        architecture = "amd64"
    return os_type, architecture


def download_file(url, filename):
    logging.info(f"Downloading {url} to {Path(filename).absolute()}...")
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()

        total_size_in_bytes = int(response.headers.get("content-length", 0))
        total_size_in_mb = total_size_in_bytes / (1024 * 1024)  # Convert to MB

        downloaded_size_in_mb = 0
        block_size = 8192  # 8 Kibibytes

        with open(filename, "wb") as file:
            for chunk in response.iter_content(chunk_size=block_size):
                file.write(chunk)
                downloaded_size_in_mb += len(chunk) / (
                    1024 * 1024
                )  # Update the downloaded size in MB
                print(
                    f"Downloaded: {downloaded_size_in_mb:.2f} MB / "
                    f"{total_size_in_mb:.2f} MB",
                    end="\r",
                )

        print("\nDownload completed.")
        return True

    except requests.RequestException as e:
        logging.error(f"Error downloading file: {e}")
        return False


def install_prometheus(file_path):
    try:
        with tarfile.open(file_path) as tar:
            tar.extractall()
        logging.info("Prometheus installed successfully.")
        return True
    except Exception as e:
        logging.error(f"Error installing Prometheus: {e}")
        return False


def start_prometheus(prometheus_dir):

    config_file = Path(PROMETHEUS_CONFIG_INPUT_PATH)

    if not config_file.exists():
        raise FileNotFoundError(f"Prometheus config file not found: {config_file}")

    prometheus_cmd = [
        f"{prometheus_dir}/prometheus",
        "--config.file",
        str(config_file),
    ]
    try:
        process = subprocess.Popen(prometheus_cmd)
        logging.info("Prometheus has started.")
        return process
    except Exception as e:
        logging.error(f"Failed to start Prometheus: {e}")
        return None


def print_shutdown_message(process_id):
    message = (
        f"Prometheus is running with PID {process_id}.\n"
        "To stop Prometheus, use the command: "
        f"'kill {process_id}', or if you need to force stop, "
        f"use 'kill -9 {process_id}'."
    )
    print(message)

    debug_message = (
        "To list all processes running Prometheus, use the command: "
        "'ps aux | grep prometheus'."
    )
    print(debug_message)


def get_latest_prometheus_version():
    url = "https://api.github.com/repos/prometheus/prometheus/releases/latest"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        # Remove the leading 'v' from the version number
        return data["tag_name"].lstrip("v")
    except requests.RequestException as e:
        logging.error(f"Error fetching latest Prometheus version: {e}")
        return None


def main():
    os_type, architecture = get_system_info()

    prometheus_version = get_latest_prometheus_version()
    if prometheus_version is None:
        logging.error("Failed to retrieve the latest Prometheus version.")
        # Fallback to a hardcoded version
        prometheus_version = "2.48.1"

    file_name = f"prometheus-{prometheus_version}.{os_type}-{architecture}.tar.gz"
    download_url = (
        "https://github.com/prometheus/prometheus/releases/"
        f"download/v{prometheus_version}/{file_name}"
    )

    if not download_file(download_url, file_name):
        logging.error("Failed to download Prometheus.")
        sys.exit(1)

    # TODO: Verify the checksum of the downloaded file

    if not install_prometheus(file_name):
        logging.error("Installation failed.")
        sys.exit(1)

    # TODO: Add a check to see if Prometheus is already running

    process = start_prometheus(
        prometheus_dir=f"prometheus-{prometheus_version}.{os_type}-{architecture}"
    )
    if process:
        print_shutdown_message(process.pid)


if __name__ == "__main__":
    main()
