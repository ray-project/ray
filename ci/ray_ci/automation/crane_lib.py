"""
Wrapper library for using the crane tool for managing container images.
https://github.com/google/go-containerregistry/blob/v0.19.0/cmd/crane/doc/crane.md

All functions return (return_code, output) tuples. On success, return_code is 0.
On failure, return_code is non-zero and output may be None (stderr is not captured).

Callers are responsible for checking return_code to detect failures rather than
relying on exceptions.
"""

import platform
import subprocess
from typing import List, Tuple

import runfiles

from ci.ray_ci.utils import logger


def _crane_binary() -> str:
    """
    Get the path to the crane binary from bazel runfiles.

    Returns:
        Path to the crane binary.

    Raises:
        ValueError: If running on unsupported platform (non-Linux or non-x86_64).
    """
    r = runfiles.Create()
    system = platform.system()
    if system != "Linux" or platform.processor() != "x86_64":
        raise ValueError(f"Unsupported platform: {system}")
    return r.Rlocation("crane_linux_x86_64/crane")


def call_crane_copy(source: str, destination: str) -> Tuple[int, str]:
    """
    Copy a container image from source to destination.

    Args:
        source: Source image reference (e.g., "registry.example.com/repo:tag").
        destination: Destination image reference.

    Returns:
        Tuple of (return_code, output). return_code is 0 on success, non-zero on
        failure. On failure, output may be None since stderr is not captured.
    """
    try:
        with subprocess.Popen(
            [
                _crane_binary(),
                "copy",
                source,
                destination,
            ],
            stdout=subprocess.PIPE,
            text=True,
        ) as proc:
            output = ""
            for line in proc.stdout:
                logger.info(line + "\n")
                output += line
            return_code = proc.wait()
            if return_code:
                raise subprocess.CalledProcessError(return_code, proc.args)
            return return_code, output
    except subprocess.CalledProcessError as e:
        return e.returncode, e.output


def call_crane_cp(tag: str, source: str, dest_repo: str) -> Tuple[int, str]:
    """
    Copy a container image to a destination repository with a specified tag.

    Args:
        tag: Tag to apply to the destination image.
        source: Source image reference.
        dest_repo: Destination repository URL (tag will be appended as ":tag").

    Returns:
        Tuple of (return_code, output). return_code is 0 on success, non-zero on
        failure. On failure, output may be None since stderr is not captured.
    """
    try:
        with subprocess.Popen(
            [
                _crane_binary(),
                "cp",
                source,
                f"{dest_repo}:{tag}",
            ],
            stdout=subprocess.PIPE,
            text=True,
        ) as proc:
            output = ""
            for line in proc.stdout:
                logger.info(line + "\n")
                output += line
            return_code = proc.wait()
            if return_code:
                raise subprocess.CalledProcessError(return_code, proc.args)
            return return_code, output
    except subprocess.CalledProcessError as e:
        return e.returncode, e.output


def call_crane_index(index_name: str, tags: List[str]) -> Tuple[int, str]:
    """
    Create a multi-architecture image index from platform-specific images.

    Args:
        index_name: Name for the resulting multi-arch index.
        tags: List of exactly 2 platform-specific image tags to combine.

    Returns:
        Tuple of (return_code, output). return_code is 0 on success, non-zero on
        failure. On failure, output may be None since stderr is not captured.
    """
    try:
        with subprocess.Popen(
            [
                _crane_binary(),
                "index",
                "append",
                "-m",
                tags[0],
                "-m",
                tags[1],
                "-t",
                index_name,
            ],
            stdout=subprocess.PIPE,
            text=True,
        ) as proc:
            output = ""
            for line in proc.stdout:
                logger.info(line + "\n")
                output += line
            return_code = proc.wait()
            if return_code:
                raise subprocess.CalledProcessError(return_code, proc.args)
            return return_code, output
    except subprocess.CalledProcessError as e:
        return e.returncode, e.output


def call_crane_manifest(tag: str) -> Tuple[int, str]:
    """
    Fetch the manifest for a container image.

    Can be used to check if an image exists (return_code 0 means it exists).

    Args:
        tag: Image reference to fetch manifest for (e.g., "registry.example.com/repo:tag").

    Returns:
        Tuple of (return_code, output). return_code is 0 if image exists, non-zero
        if image doesn't exist or fetch fails. On failure, output may be None
        since stderr is not captured.
    """
    try:
        with subprocess.Popen(
            [
                _crane_binary(),
                "manifest",
                tag,
            ],
            stdout=subprocess.PIPE,
            text=True,
        ) as proc:
            output = ""
            for line in proc.stdout:
                logger.info(line + "\n")
                output += line
            return_code = proc.wait()
            if return_code:
                raise subprocess.CalledProcessError(return_code, proc.args)
            return return_code, output
    except subprocess.CalledProcessError as e:
        return e.returncode, e.output

