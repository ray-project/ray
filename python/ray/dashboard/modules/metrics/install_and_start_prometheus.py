import logging
import os
import platform
import subprocess
import sys
import tarfile
from pathlib import Path

import requests

from ray._private.authentication.authentication_token_setup import (
    _get_default_token_path,
)
from ray._private.authentication.authentication_utils import (
    is_metrics_auth_skipped,
    is_token_auth_enabled,
)
from ray.dashboard.consts import PROMETHEUS_CONFIG_INPUT_PATH

FALLBACK_PROMETHEUS_VERSION = "2.48.1"
DOWNLOAD_BLOCK_SIZE = 8192  # 8 KB
TEST_MODE_ENV_VAR = "RAY_PROMETHEUS_DOWNLOAD_TEST_MODE"

# Indented to sit inside the 'ray' scrape job of PROMETHEUS_CONFIG_INPUT_PATH.
PROMETHEUS_AUTHORIZATION_TEMPLATE = """  authorization:
    type: Bearer
    {credentials_key}: '{credentials_value}'
"""


def get_prometheus_authorization_config() -> str:
    """Get the authorization block for the 'ray' scrape job.

    RAY_AUTH_TOKEN takes precedence and goes inline as `credentials`, so the
    config holding it must be written owner-only. Otherwise Prometheus reads
    the token from RAY_AUTH_TOKEN_PATH if set, else from ~/.ray/auth_token.
    The Kubernetes service-account token the loader can also use isn't
    followed: KubeRay clusters scrape with their own Prometheus.

    Returns:
        The YAML block to append to PROMETHEUS_CONFIG_INPUT_PATH, or "" when token
        auth is off or RAY_AUTH_SKIP_METRICS is set.

    Raises:
        ValueError: Token auth is on, RAY_AUTH_TOKEN isn't set, and the token
            file is missing, unreadable, empty or holds more than one line.
    """
    if not is_token_auth_enabled() or is_metrics_auth_skipped():
        return ""
    env_token = os.environ.get("RAY_AUTH_TOKEN", "").strip()
    if env_token:
        return PROMETHEUS_AUTHORIZATION_TEMPLATE.format(
            credentials_key="credentials",
            # A YAML single-quoted scalar escapes ' as ''.
            credentials_value=env_token.replace("'", "''"),
        )
    token_path = os.path.abspath(
        os.environ.get("RAY_AUTH_TOKEN_PATH") or _get_default_token_path()
    )
    read_error = ""
    try:
        file_token = Path(token_path).read_text().strip()
    except (OSError, UnicodeDecodeError) as e:
        file_token, read_error = "", f" ({e})"
    if not file_token:
        raise ValueError(
            "Token authentication is enabled, but no token was found for "
            f"Prometheus: {token_path} is missing, unreadable or empty"
            f"{read_error}. Set RAY_AUTH_TOKEN, or put the token in a file and "
            "set RAY_AUTH_TOKEN_PATH to its path."
        )
    if "\n" in file_token:
        # Ray uses the first non-blank line, but Prometheus sends the whole file.
        raise ValueError(
            f"{token_path} must hold only the token, on one line, for Prometheus "
            "to send it. Remove the other lines, or set RAY_AUTH_TOKEN."
        )
    return PROMETHEUS_AUTHORIZATION_TEMPLATE.format(
        credentials_key="credentials_file",
        credentials_value=token_path.replace("'", "''"),
    )


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


def write_prometheus_config(prometheus_dir: str, authorization_config: str) -> Path:
    """Write Ray's Prometheus config plus the authorization block, owner-only.

    The block can hold the token inline, so the file is created fresh with mode
    0600, never through a link someone else planted at the path.

    Args:
        prometheus_dir: The extracted Prometheus folder to write the config into.
        authorization_config: The block from get_prometheus_authorization_config.

    Returns:
        The path of the written config.
    """
    config_file = Path(prometheus_dir) / "ray_prometheus.yml"
    config_file.unlink(missing_ok=True)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    fd = os.open(config_file, flags, 0o600)
    with os.fdopen(fd, "w") as f:
        f.write(Path(PROMETHEUS_CONFIG_INPUT_PATH).read_text() + authorization_config)
    return config_file


def start_prometheus(prometheus_dir, authorization_config=""):

    # The function assumes the Ray cluster to be monitored by Prometheus uses the
    # default configuration with "/tmp/ray" as the default root temporary directory.
    #
    # This is to support the `ray metrics launch-prometheus` command, when a Ray cluster
    # hasn't started yet and the user doesn't have a way to get a `--temp-dir`
    # anywhere. So we choose to use a hardcoded default value.

    config_file = Path(PROMETHEUS_CONFIG_INPUT_PATH)

    if not config_file.exists():
        raise FileNotFoundError(f"Prometheus config file not found: {config_file}")

    if authorization_config:
        config_file = write_prometheus_config(prometheus_dir, authorization_config)

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
    # Configure logging only when this script is run directly
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )
    logging.warning("This script is not intended for production use.")

    try:
        authorization_config = get_prometheus_authorization_config()
    except ValueError as e:
        logging.error(e)
        sys.exit(1)

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
    try:
        process = start_prometheus(
            # remove the .tar.gz extension
            prometheus_dir=file_name.rstrip(".tar.gz"),
            authorization_config=authorization_config,
        )
    except OSError as e:
        logging.error(f"Failed to prepare the Prometheus config: {e}")
        sys.exit(1)
    if process:
        print_shutdown_message(process.pid)


if __name__ == "__main__":
    main()
