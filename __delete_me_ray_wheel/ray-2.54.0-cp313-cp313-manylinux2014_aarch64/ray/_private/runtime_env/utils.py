import asyncio
import itertools
import logging
import subprocess
import textwrap
import types
from typing import List


class SubprocessCalledProcessError(subprocess.CalledProcessError):
    """The subprocess.CalledProcessError with stripped stdout."""

    LAST_N_LINES = 50

    def __init__(self, *args, cmd_index=None, **kwargs):
        self.cmd_index = cmd_index
        super().__init__(*args, **kwargs)

    @staticmethod
    def _get_last_n_line(str_data: str, last_n_lines: int) -> str:
        if last_n_lines < 0:
            return str_data
        lines = str_data.strip().split("\n")
        return "\n".join(lines[-last_n_lines:])

    def __str__(self):
        str_list = (
            []
            if self.cmd_index is None
            else [f"Run cmd[{self.cmd_index}] failed with the following details."]
        )
        str_list.append(super().__str__())
        out = {
            "stdout": self.stdout,
            "stderr": self.stderr,
        }
        for name, s in out.items():
            if s:
                subtitle = f"Last {self.LAST_N_LINES} lines of {name}:"
                last_n_line_str = self._get_last_n_line(s, self.LAST_N_LINES).strip()
                str_list.append(
                    f"{subtitle}\n{textwrap.indent(last_n_line_str, ' ' * 4)}"
                )
        return "\n".join(str_list)


async def check_output_cmd(
    cmd: List[str],
    *,
    logger: logging.Logger,
    cmd_index_gen: types.GeneratorType = itertools.count(1),
    **kwargs,
) -> str:
    """Run command with arguments and return its output.

    If the return code was non-zero it raises a CalledProcessError. The
    CalledProcessError object will have the return code in the returncode
    attribute and any output in the output attribute.

    Args:
        cmd: The cmdline should be a sequence of program arguments or else
            a single string or path-like object. The program to execute is
            the first item in cmd.
        logger: The logger instance.
        cmd_index_gen: The cmd index generator, default is itertools.count(1).
        kwargs: All arguments are passed to the create_subprocess_exec.

    Returns:
        The stdout of cmd.

    Raises:
        CalledProcessError: If the return code of cmd is not 0.
    """

    cmd_index = next(cmd_index_gen)
    logger.info("Run cmd[%s] %s", cmd_index, repr(cmd))

    proc = None
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            **kwargs,
        )
        # Use communicate instead of polling stdout:
        #   * Avoid deadlocks due to streams pausing reading or writing and blocking the
        #     child process. Please refer to:
        #     https://docs.python.org/3/library/asyncio-subprocess.html#asyncio.asyncio.subprocess.Process.stderr
        #   * Avoid mixing multiple outputs of concurrent cmds.
        stdout, _ = await proc.communicate()
    except asyncio.exceptions.CancelledError as e:
        # since Python 3.9, when cancelled, the inner process needs to throw as it is
        # for asyncio to timeout properly https://bugs.python.org/issue40607
        raise e
    except BaseException as e:
        raise RuntimeError(f"Run cmd[{cmd_index}] got exception.") from e
    else:
        stdout = stdout.decode("utf-8")
        if stdout:
            logger.info("Output of cmd[%s]: %s", cmd_index, stdout)
        else:
            logger.info("No output for cmd[%s]", cmd_index)
        if proc.returncode != 0:
            raise SubprocessCalledProcessError(
                proc.returncode, cmd, output=stdout, cmd_index=cmd_index
            )
        return stdout
    finally:
        if proc is not None:
            # Kill process.
            try:
                proc.kill()
            except ProcessLookupError:
                pass
            # Wait process exit.
            await proc.wait()
