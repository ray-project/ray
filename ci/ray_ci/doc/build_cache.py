import os
import subprocess
import sys
import tempfile
from typing import Set

import boto3

ENVIRONMENT_PICKLE = "_build/doctrees/environment.pickle"

_BUILD_CACHE_S3_BUCKET = "ray-ci-results"
_BUILD_CACHE_PATH_PREFIX = "doc_build/"


class BuildCache:
    """
    BuildCache represents the build artifacts generated from the doc build process,
    massaged to be used as a cache for the next build process
    """

    def __init__(self, cache_dir: str):
        """
        Args:
            cache_dir: The directory where the build artifacts are stored
        """
        self._cache_dir = cache_dir

    def upload(self, dry_run: bool) -> None:
        """Upload the build artifacts to S3."""
        self._massage_cache(ENVIRONMENT_PICKLE)
        cache_files = self._get_cache()
        doc_tarball = self._zip_cache(cache_files)

        if dry_run:
            print(f"Skipping upload of {doc_tarball} to S3.", file=sys.stderr)
            return

        self._upload_cache(doc_tarball)

    def _massage_cache(
        self, environment_cache_file: str, python_executable: str = "python"
    ) -> None:
        """
        TODO (elliot-barn): revert when upgrading bazel python toolchain
        
        Strip build-environment-local dependencies from the Sphinx environment
        pickle so the artifacts can be reused as a global cache.

        This must run under the doc-build environment's Python -- the same
        interpreter that built the docs and wrote the pickle, not this script's
        Bazel-pinned Python. A pickled Sphinx environment can only be loaded by a
        matching Sphinx; loading a Sphinx 8.x pickle under an older Sphinx raises
        e.g. ``ModuleNotFoundError: No module named 'sphinx.util._files'``. So we
        run massage_cache.py with PYTHONPATH unset, mirroring how _build() runs
        ``make html``.

        Args:
            environment_cache_file: Path to the environment pickle, relative to
                the cache directory.
            python_executable: Interpreter to run the massage with. Defaults to the
                environment ``python``; tests override it with their own.
        """
        environment_cache_path = os.path.join(self._cache_dir, environment_cache_file)
        massage_script = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "massage_cache.py"
        )
        env = os.environ.copy()
        # Drop PYTHONPATH so the environment Python (with the Sphinx that wrote
        # the pickle) is used instead of the Bazel runfiles Python.
        env.pop("PYTHONPATH", None)
        subprocess.run(
            [python_executable, massage_script, environment_cache_path],
            env=env,
            check=True,
        )

    def _get_cache(self) -> Set[str]:
        """
        Get the list of cache files
        """
        untracked_files = (
            subprocess.check_output(
                ["git", "ls-files", "--others", "-z"],
                cwd=self._cache_dir,
            )
            .decode("utf-8")
            .split(os.linesep)
        )

        return {file for file in untracked_files if file}

    def _zip_cache(self, cache_files: Set[str]) -> str:
        """
        Create a tarball of the cache files
        """
        with tempfile.NamedTemporaryFile(mode="w+t") as temp_file:
            temp_file.write("\n".join(cache_files))
            doc_tarball = f'{os.environ["BUILDKITE_COMMIT"]}.tgz'
            doc_tarball_path = os.path.join(self._cache_dir, doc_tarball)
            subprocess.run(
                ["tar", "-cvzf", doc_tarball_path, "-T", temp_file.name],
                cwd=self._cache_dir,
            )

            return doc_tarball

    def _upload_cache(self, doc_tarball: str) -> None:
        boto3.client("s3").upload_file(
            os.path.join(self._cache_dir, doc_tarball),
            _BUILD_CACHE_S3_BUCKET,
            f"{_BUILD_CACHE_PATH_PREFIX}{doc_tarball}",
        )
