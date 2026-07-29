import asyncio
import itertools
import logging
import re
import subprocess
import textwrap
import types
from typing import List, Optional

# A package index can carry basic-auth credentials inline, and users routinely
# pass one that way:
#
#     pip install --index-url https://__token__:<token>@example.com/simple ...
#
# That URL is an argv element, and CalledProcessError renders the whole argv in
# its message. The message is shown to the user, written to the agent log, and
# -- since the runtime env agent began reporting structured setup failures --
# stored verbatim by whoever consumes the failure, so a token pasted into a
# runtime env would otherwise come to rest in places the user never chose.
#
# Only the userinfo is dropped: the host and path are what make the message
# useful, and neither is a secret.
_URL_USERINFO_RE = re.compile(r"(?P<scheme>[A-Za-z][A-Za-z0-9+.\-]*://)[^/\s@'\"]+@")


def redact_url_credentials(text: str) -> str:
    """Replace inline credentials in any URL in `text` with a placeholder.

    Args:
        text: Text that may embed URLs of the form scheme://userinfo@host.

    Returns:
        The same text with every userinfo component replaced by `<redacted>`.
    """
    return _URL_USERINFO_RE.sub(r"\g<scheme><redacted>@", text)


# Hard cap on customer-origin text placed on the structured-failure path.
#
# That path travels off the node to a consumer that has no offload of its own, so
# one short line is the entire budget. It buys the thing a bare exit code cannot
# -- telling two failures of the same command apart at a glance -- while the full
# text stays reachable through the log file the failure points at, the
# error-event feed, and the job's own message, all of which do have somewhere to
# put it.
MAX_SUMMARY_LINE_CHARS = 200


def summary_line(text: Optional[str]) -> Optional[str]:
    """The last non-empty line of `text`, credential-redacted and length-capped.

    The last line is taken because every producer on this path puts the
    actionable sentence there: pip's "No matching distribution found for X", the
    shell's "command not found", a traceback's "ValueError: boom". Nothing is
    parsed out of it and no meaning is derived from it -- it is carried so a
    human or agent can read it, and the cause always comes from typed fields.

    Args:
        text: Free text, typically a formatted exception or installer output.

    Returns:
        One redacted line of at most MAX_SUMMARY_LINE_CHARS characters, or None
        if `text` holds nothing. None rather than "" so the caller omits the
        field instead of reporting a blank one, which would read as a fact.
    """
    if not text:
        return None
    for raw_line in reversed(text.strip().splitlines()):
        line = redact_url_credentials(raw_line.strip())
        if not line:
            continue
        if len(line) > MAX_SUMMARY_LINE_CHARS:
            return line[: MAX_SUMMARY_LINE_CHARS - 3] + "..."
        return line
    return None


def sole_requirement(packages: Optional[List[str]]) -> Optional[str]:
    """The single requirement in `packages`, when blaming it is unambiguous.

    Returns None unless the list holds exactly one entry naming a package
    outright. A set of requirements is excluded because which member failed is
    stated only in the installer's own output, and parsing that is the pattern
    matching this whole path exists to avoid. A lone ``-r requirements.txt`` is
    excluded for the same reason: it expands to a set.

    An unset field is honest. A confidently wrong package name is worse than
    none, because it sends someone to fix the wrong dependency.
    """
    if not packages or len(packages) != 1:
        return None
    only = str(packages[0]).strip()
    if not only or only.startswith("-"):
        return None
    return only


class SubprocessCalledProcessError(subprocess.CalledProcessError):
    """The subprocess.CalledProcessError with stripped stdout."""

    LAST_N_LINES = 50

    def __init__(
        self, *args, cmd_index=None, phase=None, attributed_package=None, **kwargs
    ):
        self.cmd_index = cmd_index
        # Which setup step this command implemented and, when the command
        # installs exactly one requirement, that requirement. The runtime env
        # agent reads these as typed values; they are deliberately not part of
        # __str__, which is user-facing text.
        self.phase = phase
        self.attributed_package = attributed_package
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
        # Redact once over the whole message rather than over the argv alone:
        # an installer echoes the index URL it was given back into its own
        # output, so stdout and stderr leak the same credential the cmd does.
        return redact_url_credentials("\n".join(str_list))


async def check_output_cmd(
    cmd: List[str],
    *,
    logger: logging.Logger,
    cmd_index_gen: types.GeneratorType = itertools.count(1),
    phase: Optional[str] = None,
    attributed_package: Optional[str] = None,
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
        phase: The name of the setup step this cmd implements, e.g.
            "install_pip". Attached to the raised error so that a caller can
            report which step failed without parsing the error message.
        attributed_package: The requirement this cmd installs, when it installs
            exactly one and its name comes from the user-submitted runtime env
            (e.g. "pip==24.0"). Left unset for a cmd that installs a whole
            requirement set, because which member of the set failed is only
            visible in the installer's own output.
        **kwargs: All arguments are passed to the create_subprocess_exec.

    Returns:
        The stdout of cmd.

    Raises:
        CalledProcessError: If the return code of cmd is not 0.
    """

    cmd_index = next(cmd_index_gen)
    logger.info("Run cmd[%s] %s", cmd_index, redact_url_credentials(repr(cmd)))

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
        error = RuntimeError(f"Run cmd[{cmd_index}] got exception.")
        # The cmd never ran to completion, so there is no exit code to report;
        # the phase is the only attribution this path can carry.
        error.phase = phase
        raise error from e
    else:
        stdout = stdout.decode("utf-8")
        if stdout:
            logger.info(
                "Output of cmd[%s]: %s", cmd_index, redact_url_credentials(stdout)
            )
        else:
            logger.info("No output for cmd[%s]", cmd_index)
        if proc.returncode != 0:
            raise SubprocessCalledProcessError(
                proc.returncode,
                cmd,
                output=stdout,
                cmd_index=cmd_index,
                phase=phase,
                attributed_package=attributed_package,
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
