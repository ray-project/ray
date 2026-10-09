import logging
import os
import stat
import subprocess
import time
import zipfile
from dataclasses import dataclass
from pathlib import Path

import grpc
import requests
from filelock import FileLock

from ray.rllib.env import EnvContext
from ray.rllib.examples.envs.classes.multi_agent.footsies.game.proto import (
    footsies_service_pb2 as footsies_pb2,
    footsies_service_pb2_grpc as footsies_pb2_grpc,
)
from ray.util import log_once

logger = logging.getLogger(__name__)


@dataclass
class BinaryUrls:
    # Uploaded 07.28.2025
    S3_ROOT = "https://ray-example-data.s3.us-west-2.amazonaws.com/rllib/env-footsies/binaries/"

    # Zip file names
    ZIP_LINUX_SERVER = "footsies_linux_server_021725.zip"
    ZIP_LINUX_WINDOWED = "footsies_linux_windowed_021725.zip"
    ZIP_MAC_HEADLESS = "footsies_mac_headless_5709b6d.zip"
    ZIP_MAC_WINDOWED = "footsies_mac_windowed_5709b6d.zip"

    # Full URLs
    URL_LINUX_SERVER_BINARIES = S3_ROOT + ZIP_LINUX_SERVER
    URL_LINUX_WINDOWED_BINARIES = S3_ROOT + ZIP_LINUX_WINDOWED
    URL_MAC_HEADLESS_BINARIES = S3_ROOT + ZIP_MAC_HEADLESS
    URL_MAC_WINDOWED_BINARIES = S3_ROOT + ZIP_MAC_WINDOWED


class FootsiesBinary:
    def __init__(self, config: EnvContext, port: int):
        self._urls = BinaryUrls()
        self.config = config
        self.port = port
        self.binary_to_download = config["binary_to_download"]

        if self.binary_to_download == "linux_server":
            self.url = self._urls.URL_LINUX_SERVER_BINARIES
        elif self.binary_to_download == "linux_windowed":
            self.url = self._urls.URL_LINUX_WINDOWED_BINARIES
        elif self.binary_to_download == "mac_headless":
            self.url = self._urls.URL_MAC_HEADLESS_BINARIES
        elif self.binary_to_download == "mac_windowed":
            self.url = self._urls.URL_MAC_WINDOWED_BINARIES
        else:
            raise ValueError(f"Invalid target binary: {self.binary_to_download}")

        self.full_download_dir = Path(config["binary_download_dir"]).resolve()
        self.full_download_path = (
            self.full_download_dir / str.split(self.url, sep="/")[-1]
        )
        self.full_extract_dir = Path(config["binary_extract_dir"]).resolve()
        self.renamed_path = self.full_extract_dir / "footsies_binaries"

    @staticmethod
    def _add_executable_permission(binary_path: Path) -> None:
        binary_path.chmod(binary_path.stat().st_mode | stat.S_IXUSR)

    def start_game_server(self) -> int:
        """Downloads, unzips, and starts the Footsies game server binary.

        Returns footsies process PID.
        """
        self._download_game_binary()
        self._unzip_game_binary()

        if self.binary_to_download == "mac_windowed":
            game_binary_path = (
                Path(self.renamed_path) / "Contents" / "MacOS" / "FOOTSIES"
            )
        elif self.binary_to_download == "mac_headless":
            game_binary_path = Path(self.renamed_path) / "FOOTSIES"
        else:
            game_binary_path = Path(self.renamed_path) / "footsies.x86_64"

        if os.access(game_binary_path, os.X_OK):
            logger.info(
                f"Game binary has an 'executable' permission: {game_binary_path}"
            )
        else:
            self._add_executable_permission(game_binary_path)
        logger.info(f"Game binary path: {game_binary_path}")

        # The underlying game can be quite spammy. So when we are not debugging it, we can suppress the output.
        log_output = self.config.get("log_unity_output")
        if log_output is None:
            if log_once("log_unity_output_not_set"):
                logger.warning(
                    "`log_unity_output` not set in environment config, not logging output by default"
                )
            log_output = False

        if not log_output:
            stdout_dest = stderr_dest = subprocess.DEVNULL
        else:
            stdout_dest = stderr_dest = None  # Use parent's stdout/stderr

        if (
            self.binary_to_download == "linux_server"
            or self.binary_to_download == "linux_windowed"
        ):
            process = subprocess.Popen(
                [game_binary_path, "--port", str(self.port)],
                stdout=stdout_dest,
                stderr=stderr_dest,
            )
        else:
            process = subprocess.Popen(
                [
                    "arch",
                    "-x86_64",
                    game_binary_path,
                    "--port",
                    str(self.port),
                ],
                stdout=stdout_dest,
                stderr=stderr_dest,
            )

        # check if the game server is running correctly
        timeout = 2
        channel = grpc.insecure_channel(f"localhost:{self.port}")
        stub = footsies_pb2_grpc.FootsiesGameServiceStub(channel)

        # step 1: try to start the game
        while True:
            try:
                stub.StartGame(footsies_pb2.Empty())
                logger.info("Game ready!")
                break
            except grpc.RpcError as e:
                code = e.code()
                if code in (
                    grpc.StatusCode.UNAVAILABLE,
                    grpc.StatusCode.DEADLINE_EXCEEDED,
                ):
                    logger.info(f"RLlib {self.__class__.__name__}: Game not ready...")
                    time.sleep(timeout)
                    continue
                raise

        # step 2: check if the game is ready
        ready = False
        while not ready:
            try:
                ready = stub.IsReady(footsies_pb2.Empty()).value
                if not ready:
                    logger.info(f"RLlib {self.__class__.__name__}: Game not ready...")
                    time.sleep(timeout)
                    continue
                else:
                    logger.info("Game ready!")
                    break
            except grpc.RpcError as e:
                if e.code() in (
                    grpc.StatusCode.UNAVAILABLE,
                    grpc.StatusCode.DEADLINE_EXCEEDED,
                ):
                    time.sleep(timeout)
                    logger.info(f"RLlib {self.__class__.__name__}: Game not ready...")
                    continue
                raise

        channel.close()
        return process.pid

    def _download_game_binary(self):
        # As multiple actors might try to download all at the same time.
        # The file lock should force only one actor to download
        chunk_size = 1024 * 1024  # 1MB

        lock_path = self.full_download_path.parent / ".footsies-download.lock"
        with FileLock(lock_path, timeout=300):
            if self.full_download_path.exists():
                logger.info(
                    f"Game binary already exists at {self.full_download_path}, skipping download."
                )

            else:
                try:
                    with requests.get(self.url, stream=True) as response:
                        response.raise_for_status()
                        self.full_download_dir.mkdir(parents=True, exist_ok=True)
                        with open(self.full_download_path, "wb") as f:
                            for chunk in response.iter_content(chunk_size=chunk_size):
                                if chunk:
                                    f.write(chunk)
                    logger.info(
                        f"Downloaded game binary to {self.full_download_path}\n"
                        f"Binary size: {self.full_download_path.stat().st_size / 1024 / 1024:.1f} MB\n"
                    )
                except requests.exceptions.RequestException as e:
                    logger.error(f"Failed to download binary from {self.url}: {e}")

    def _unzip_game_binary(self):
        # As multiple actors might try to unzip or rename the paths at the same time.
        # The file lock should force this function to be sequential
        lock_path = self.full_download_path.parent / ".footsies-unzip.lock"
        with FileLock(lock_path, timeout=300):
            if self.renamed_path.exists():
                logger.info(
                    f"Game binary already extracted at {self.renamed_path}, skipping extraction."
                )
            else:
                self.full_extract_dir.mkdir(parents=True, exist_ok=True)
                with zipfile.ZipFile(self.full_download_path, mode="r") as zip_ref:
                    zip_ref.extractall(self.full_extract_dir)

                if self.binary_to_download == "mac_windowed":
                    self.full_download_path.with_suffix(".app").rename(
                        self.renamed_path
                    )
                else:
                    self.full_download_path.with_suffix("").rename(self.renamed_path)
                logger.info(f"Extracted game binary to {self.renamed_path}")
