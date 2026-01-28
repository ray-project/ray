"""
Wrapper library for using the crane tool for managing container images.
https://github.com/google/go-containerregistry/blob/v0.19.0/cmd/crane/doc/crane.md

Functions raise CraneError on failure.
"""

import os
import platform
import subprocess
import tarfile
import tempfile
from typing import List

import runfiles

from ci.ray_ci.utils import logger


class CraneError(Exception):
    """Exception raised when a crane operation fails."""


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


def _run_crane_command(args: List[str], stdin_input: str | None = None) -> str:
    """
    Run a crane command that produces TEXT output.

    Args:
        args: Command arguments to pass to crane.
        stdin_input: Optional input to pass via stdin (e.g., for passwords).

    Returns:
        Command stdout output.

    Raises:
        CraneError: If the command fails.
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
                assert proc.stdin is not None
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
                raise CraneError(
                    f"Crane command `{' '.join(command)}` failed "
                    f"(rc={return_code}): {stderr}"
                )
            return output
    except FileNotFoundError:
        raise CraneError(f"Crane binary not found at {command[0]}")
    except CraneError:
        raise
    except Exception as e:
        raise CraneError(
            f"Unexpected error running crane command `{' '.join(command)}`: {e}"
        )


def _extract_tar_to_dir(tar_path: str, output_dir: str) -> None:
    """
    Extract a tar file to a directory with path traversal protection.

    Args:
        tar_path: Path to the tar file to extract.
        output_dir: Directory to extract into.
    """
    os.makedirs(output_dir, exist_ok=True)
    resolved_output_dir = os.path.realpath(output_dir)

    with tarfile.open(tar_path, mode="r:*") as tf:
        for m in tf:
            member_path = os.path.join(resolved_output_dir, m.name)
            resolved_member_path = os.path.realpath(member_path)
            try:
                # Verify extracted files stay within the target directory.
                common = os.path.commonpath([resolved_output_dir, resolved_member_path])
                if common != resolved_output_dir:
                    logger.warning(f"Skipping unsafe tar member: {m.name}")
                    continue
            except ValueError:
                logger.warning(f"Skipping path on different drive: {m.name}")
                continue
            tf.extract(m, path=output_dir)


def call_crane_copy(source: str, destination: str) -> None:
    """
    Copy a container image from source to destination.

    Args:
        source: Source image reference (e.g., "registry.example.com/repo:tag").
        destination: Destination image reference.

    Raises:
        CraneError: If the copy fails.
    """
    _run_crane_command(["copy", source, destination])


def call_crane_cp(tag: str, source: str, dest_repo: str) -> None:
    """
    Copy a container image to a destination repository with a specified tag.

    Args:
        tag: Tag to apply to the destination image.
        source: Source image reference.
        dest_repo: Destination repository URL (tag will be appended as ":tag").

    Raises:
        CraneError: If the copy fails.
    """
    _run_crane_command(["cp", source, f"{dest_repo}:{tag}"])


def call_crane_index(index_name: str, tags: List[str]) -> None:
    """
    Create a multi-architecture image index from platform-specific images.

    Args:
        index_name: Name for the resulting multi-arch index.
        tags: List of exactly 2 platform-specific image tags to combine.

    Raises:
        CraneError: If the index creation fails.
        ValueError: If tags list doesn't contain exactly 2 tags.
    """
    if len(tags) != 2:
        raise ValueError("call_crane_index requires exactly 2 tags")

    args = ["index", "append", "-m", tags[0], "-m", tags[1], "-t", index_name]
    _run_crane_command(args)


def call_crane_manifest(tag: str) -> str:
    """
    Fetch the manifest for a container image.

    Args:
        tag: Image reference to fetch manifest for (e.g., "registry.example.com/repo:tag").

    Returns:
        The image manifest as a string.

    Raises:
        CraneError: If the image doesn't exist or fetch fails.
    """
    return _run_crane_command(["manifest", tag])


def call_crane_export(tag: str, output_dir: str) -> None:
    """
    Export a container image to a tar file and extract it.

    Equivalent of:
      crane export <tag> output.tar && tar -xf output.tar -C <output_dir>

    Args:
        tag: Image reference to export.
        output_dir: Directory to extract the image filesystem into.

    Raises:
        CraneError: If the export or extraction fails.
    """
    os.makedirs(output_dir, exist_ok=True)

    with tempfile.TemporaryDirectory() as tmpdir:
        tar_path = os.path.join(tmpdir, "output.tar")
        crane_cmd = [_crane_binary(), "export", tag, tar_path]
        logger.info(f"Running: {' '.join(crane_cmd)}")

        try:
            subprocess.check_call(crane_cmd, env=os.environ)
        except subprocess.CalledProcessError as e:
            raise CraneError(f"crane export failed (rc={e.returncode})")
        except FileNotFoundError:
            raise CraneError(f"Crane binary not found at {crane_cmd[0]}")

        try:
            logger.info(f"Extracting {tar_path} to {output_dir}")
            _extract_tar_to_dir(tar_path, output_dir)
        except Exception as e:
            raise CraneError(f"tar extraction failed: {e}")
