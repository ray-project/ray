import logging
import os
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

FALLBACK_PROMETHEUS_VERSION = "2.48.1"
DOWNLOAD_BLOCK_SIZE = 8192  # 8 KB
TEST_MODE_ENV_VAR = "RAY_PROMETHEUS_DOWNLOAD_TEST_MODE"


def get_system_info():
    os_type = platform.system().lower()
    architecture = platform.machine()
    if architecture == "x86_64":
        # In the Prometheus filename, it's called amd64
        architecture = "amd64"
    elif architecture == "aarch64":
        # In the Prometheus filename, it's called arm64
        architecture = "arm64"
    return os_type, architecture


def download_file(url, filename):
    logging.info(f"Downloading {url} to {Path(filename).absolute()}...")
    try:
        test_mode = os.environ.get(TEST_MODE_ENV_VAR, False)
        request_method = requests.head if test_mode else requests.get
        response = request_method(url, stream=True)
        response.raise_for_status()

        total_size_in_bytes = int(response.headers.get("content-length", 0))
        total_size_in_mb = total_size_in_bytes / (1024 * 1024)

        downloaded_size_in_mb = 0
        block_size = DOWNLOAD_BLOCK_SIZE

        with open(filename, "wb") as file:
            for chunk in response.iter_content(chunk_size=block_size):
                file.write(chunk)
                downloaded_size_in_mb += len(chunk) / (1024 * 1024)
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

    # The function assumes the Ray cluster to be monitored by Prometheus uses the
    # default configuration with "/tmp/ray" as the default root temporary directory.
    #
    # This is to support the `ray metrics launch-prometheus` command, when a Ray cluster
    # hasn't started yet and the user doesn't have a way to get a `--temp-dir`
    # anywhere. So we choose to use a hardcoded default value.

    config_file = Path(PROMETHEUS_CONFIG_INPUT_PATH)

    if not config_file.exists():
        raise FileNotFoundError(f"Prometheus config file not found: {config_file}")

    prometheus_cmd = [
        f"{prometheus_dir}/prometheus",
        "--config.file",
        str(config_file),
        "--web.enable-lifecycle",
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
        "`ray metrics shutdown-prometheus`, "
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


def get_prometheus_filename(os_type=None, architecture=None, prometheus_version=None):
    if os_type is None or architecture is None:
        os_type, architecture = get_system_info()

    if prometheus_version is None:
        prometheus_version = get_latest_prometheus_version()
        if prometheus_version is None:
            logging.warning(
                "Failed to retrieve the latest Prometheus version. Falling "
                f"back to {FALLBACK_PROMETHEUS_VERSION}."
            )
            # Fall back to a hardcoded version
            prometheus_version = FALLBACK_PROMETHEUS_VERSION

    return (
        f"prometheus-{prometheus_version}.{os_type}-{architecture}.tar.gz",
        prometheus_version,
    )


def get_prometheus_download_url(
    os_type=None, architecture=None, prometheus_version=None
):
    file_name, prometheus_version = get_prometheus_filename(
        os_type, architecture, prometheus_version
    )
    return (
        "https://github.com/prometheus/prometheus/releases/"
        f"download/v{prometheus_version}/{file_name}"
    )


def download_prometheus(os_type=None, architecture=None, prometheus_version=None):
    file_name, _ = get_prometheus_filename(os_type, architecture, prometheus_version)
    download_url = get_prometheus_download_url(
        os_type, architecture, prometheus_version
    )

    return download_file(download_url, file_name), file_name


def main():
    logging.warning("This script is not intended for production use.")

    downloaded, file_name = download_prometheus()
    if not downloaded:
        logging.error("Failed to download Prometheus.")
        sys.exit(1)

    # TODO: Verify the checksum of the downloaded file

    if not install_prometheus(file_name):
        logging.error("Installation failed.")
        sys.exit(1)

    # TODO: Add a check to see if Prometheus is already running

    assert file_name.endswith(".tar.gz")
    process = start_prometheus(
        # remove the .tar.gz extension
        prometheus_dir=file_name.rstrip(".tar.gz")
    )
    if process:
        print_shutdown_message(process.pid)


if __name__ == "__main__":
    main()
