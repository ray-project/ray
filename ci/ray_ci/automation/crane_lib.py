"""
Wrapper library for using the crane tool for managing container images.
https://github.com/google/go-containerregistry/blob/v0.19.0/cmd/crane/doc/crane.md

All functions return (return_code, output) tuples. On success, return_code is 0.
On failure, return_code is non-zero and output may be None (stderr is not captured).

Callers are responsible for checking return_code to detect failures rather than
relying on exceptions.
"""

import os
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


def _run_crane_command(
    args: List[str], stdin_input: str | None = None
) -> Tuple[int, str]:
    """
    Run a crane command and return the exit code and output.

    Args:
        args: Command arguments to pass to crane.
        stdin_input: Optional input to pass via stdin (e.g., for passwords).
    """
    command = [_crane_binary()] + args
    try:
        with subprocess.Popen(
            command,
            stdin=subprocess.PIPE if stdin_input else None,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=os.environ,
        ) as proc:
            if stdin_input:
                proc.stdin.write(stdin_input)
                proc.stdin.close()
            output = ""
            if proc.stdout:
                for line in proc.stdout:
                    logger.info(line.rstrip("\n"))
                    output += line
            return_code = proc.wait()
            if return_code:
                stderr = proc.stderr.read() if proc.stderr else ""
                logger.error(
                    f"Crane command `{' '.join(command)}` failed with stderr:\n{stderr}"
                )
                raise subprocess.CalledProcessError(
                    return_code, command, output, stderr
                )
            return return_code, output
    except subprocess.CalledProcessError as e:
        return e.returncode, e.output
    except FileNotFoundError:
        logger.error(f"Crane binary not found at {command[0]}")
        return 1, f"Crane binary not found at {command[0]}"


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
    return _run_crane_command(["copy", source, destination])


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
    return _run_crane_command(["cp", source, f"{dest_repo}:{tag}"])


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
    if len(tags) != 2:
        logger.error("call_crane_index requires exactly 2 tags")
        return 1, "call_crane_index requires exactly 2 tags"

    args = ["index", "append", "-m", tags[0], "-m", tags[1], "-t", index_name]
    return _run_crane_command(args)


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
    return _run_crane_command(["manifest", tag])
