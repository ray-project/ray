import logging

import ray.dashboard.modules.log.log_utils as log_utils
import ray.dashboard.modules.log.log_consts as log_consts
import ray.dashboard.utils as dashboard_utils
import ray.dashboard.optional_utils as dashboard_optional_utils
import asyncio
import io
import os

from pathlib import Path

from ray.core.generated import reporter_pb2
from ray.core.generated import reporter_pb2_grpc


logger = logging.getLogger(__name__)

routes = dashboard_optional_utils.ClassMethodRouteTable


class LogAgent(dashboard_utils.DashboardAgentModule):
    def __init__(self, dashboard_agent):
        super().__init__(dashboard_agent)
        log_utils.register_mimetypes()
        routes.static("/logs", self._dashboard_agent.log_dir, show_index=True)

    async def run(self, server):
        pass

    @staticmethod
    def is_minimal_module():
        return False


# 64 KB
BLOCK_SIZE = 1 << 16


class LogAgentV1Grpc(
    dashboard_utils.DashboardAgentModule, reporter_pb2_grpc.ReporterServiceServicer
):
    def __init__(self, dashboard_agent):
        super().__init__(dashboard_agent)

    async def run(self, server):
        if server:
            reporter_pb2_grpc.add_LogServiceServicer_to_server(self, server)

    # TODO: should this return True
    @staticmethod
    def is_minimal_module():
        return False

    async def ListLogs(self, request, context):
        """
        Lists all files in the active Ray logs directory.

        NOTE: These RPCs are used by state_head.py, not log_head.py
        """
        path = Path(self._dashboard_agent.log_dir)
        if not path.exists():
            raise FileNotFoundError(
                f"Could not find log dir at path: {self._dashboard_agent.log_dir}"
                "It is unexpected. Please report an issue to Ray Github."
            )
        log_files = []
        for p in path.glob(request.glob_filter):
            log_files.append(p.name)
        return reporter_pb2.ListLogsReply(log_files=log_files)

    async def StreamLog(self, request, context):
        """
        Streams the log in real time starting from `request.lines` number of lines from
        the end of the file if `request.keep_alive == True`. Else, it terminates the
        stream once there are no more bytes to read from the log file.

        NOTE: These RPCs are used by state_head.py, not log_head.py
        """
        # NOTE: If the client side connection is closed, this handler will
        # be automatically terminated.
        lines = request.lines if request.lines else 1000

        filepath = f"{self._dashboard_agent.log_dir}/{request.log_file_name}"
        if "/" in request.log_file_name or not os.path.isfile(filepath):
            await context.send_initial_metadata(
                [[log_consts.LOG_GRPC_ERROR, log_consts.FILE_NOT_FOUND]]
            )
        else:
            with open(filepath, "rb") as f:
                await context.send_initial_metadata([])
                # If requesting the whole file, we stream it since it may be large.
                if lines == -1:
                    while not context.done():
                        bytes = f.read(BLOCK_SIZE)
                        if bytes == b"":
                            end = f.tell()
                            break
                        yield reporter_pb2.StreamLogReply(data=bytes)
                else:
                    bytes, end = tail(f, lines)
                    yield reporter_pb2.StreamLogReply(data=bytes + b"\n")
                if request.keep_alive:
                    interval = request.interval if request.interval else 1
                    f.seek(end)
                    while not context.done():
                        await asyncio.sleep(interval)
                        bytes = f.read()
                        if bytes != b"":
                            yield reporter_pb2.StreamLogReply(data=bytes)


def tail(f: io.TextIOBase, lines: int):
    """Tails the given file (in 'rb' mode)

    We assume that any "lines" parameter is not significant (<100,000 lines)
    and will result in a buffer with a small memory profile (<1MB)

    Taken from: https://stackoverflow.com/a/136368/8299684

    Examples:
    Args:
        f: text file in 'rb' mode
        lines: The number of lines to read from the end of the file.
    Returns:
        string containing the lines of the file,
        the position of the last byte read in units of bytes
    """

    total_lines_wanted = lines

    # Seek to the end of the file
    f.seek(0, 2)
    block_end_byte = f.tell()

    last_byte_read = block_end_byte
    lines_to_go = total_lines_wanted
    block_number = -1
    blocks = []

    # Read blocks into memory until we have seen at least
    # `total_lines_wanted` number of lines. Then, return a string
    # containing the last `total_lines_wanted` number of lines
    while lines_to_go > 0 and block_end_byte > 0:
        if block_end_byte - BLOCK_SIZE > 0:
            f.seek(block_number * BLOCK_SIZE, 2)
            blocks.append(f.read(BLOCK_SIZE))
        else:
            f.seek(0, 0)
            blocks.append(f.read(block_end_byte))
        lines_found = blocks[-1].count(b"\n")
        lines_to_go -= lines_found
        block_end_byte -= BLOCK_SIZE
        block_number -= 1
    all_read_text = b"".join(reversed(blocks))
    return b"\n".join(all_read_text.splitlines()[-total_lines_wanted:]), last_byte_read
