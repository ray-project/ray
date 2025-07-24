import sys

from ray._private.utils import open_log
from ray._raylet import StreamRedirector


def redirect_stdout_stderr_if_needed(
    stdout_filepath: str,
    stderr_filepath: str,
    rotation_bytes: int,
    rotation_backup_count: int,
):
    """This function sets up redirection for stdout and stderr if needed, based on the given rotation parameters.

    params:
    stdout_filepath: the filepath stdout will be redirected to; if empty, stdout will not be redirected.
    stderr_filepath: the filepath stderr will be redirected to; if empty, stderr will not be redirected.
    rotation_bytes: number of bytes which triggers file rotation.
    rotation_backup_count: the max size of rotation files.
    """

    # Setup redirection for stdout and stderr.
    if stdout_filepath:
        StreamRedirector.redirect_stdout(
            stdout_filepath,
            rotation_bytes,
            rotation_backup_count,
            False,  # tee_to_stdout
            False,  # tee_to_stderr
        )
    if stderr_filepath:
        StreamRedirector.redirect_stderr(
            stderr_filepath,
            rotation_bytes,
            rotation_backup_count,
            False,  # tee_to_stdout
            False,  # tee_to_stderr
        )

    # Setup python system stdout/stderr.
    stdout_fileno = sys.stdout.fileno()
    stderr_fileno = sys.stderr.fileno()
    # We also manually set sys.stdout and sys.stderr because that seems to
    # have an effect on the output buffering. Without doing this, stdout
    # and stderr are heavily buffered resulting in seemingly lost logging
    # statements. We never want to close the stdout file descriptor, dup2 will
    # close it when necessary and we don't want python's GC to close it.
    sys.stdout = open_log(stdout_fileno, unbuffered=True, closefd=False)
    sys.stderr = open_log(stderr_fileno, unbuffered=True, closefd=False)
