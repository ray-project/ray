import logging

import ray.dashboard.modules.log.log_utils as log_utils
import ray.dashboard.modules.log.log_consts as log_consts
import ray.dashboard.utils as dashboard_utils
import ray.dashboard.optional_utils as dashboard_optional_utils
from ray.core.generated import reporter_pb2
from ray.core.generated import reporter_pb2_grpc
import asyncio
import os

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


BLOCK_SIZE = 8192


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

    async def LogIndex(self, request, context):
        return reporter_pb2.LogIndexReply(
            log_files=os.listdir(self._dashboard_agent.log_dir)
        )

    async def StreamLog(self, request, context):
        """
        Streams the log in real time starting from `request.lines` number of lines from
        the end of the file if `request.keep_alive == True`. Else, it terminates the
        stream once there are no more bytes to read from the log file.
        """
        lines = request.lines if request.lines else 1000

        filepath = f"{self._dashboard_agent.log_dir}/{request.log_file_name}"
        if not os.path.isfile(filepath):
            await context.send_initial_metadata(
                [[log_consts.LOG_STREAM_STATUS, log_consts.FILE_NOT_FOUND]]
            )
        else:
            with open(filepath, "rb") as f:
                await context.send_initial_metadata(
                    [[log_consts.LOG_STREAM_STATUS, log_consts.OK]]
                )
                # If requesting the whole file, we stream the file since it may be large.
                if lines == -1:
                    while True:
                        bytes = f.read(BLOCK_SIZE)
                        if bytes == b"":
                            end = f.tell()
                            break
                        yield reporter_pb2.StreamLogReply(data=bytes)
                else:
                    bytes, end = tail(f, lines)
                    yield reporter_pb2.StreamLogReply(data=bytes)
                if request.keep_alive:
                    interval = request.interval if request.interval else 0.5
                    f.seek(end)
                    while True:
                        await asyncio.sleep(interval)
                        bytes = f.read()
                        if bytes != b"":
                            yield reporter_pb2.StreamLogReply(data=bytes)


def tail(f, lines=1000):
    """
    Taken from: https://stackoverflow.com/a/136368/8299684

    We assume that any "lines" parameter is not significant and will result
    in a buffer with a small memory profile (<1MB)
    """

    total_lines_wanted = lines

    BLOCK_SIZE = 8192
    f.seek(0, 2)
    block_end_byte = f.tell()
    last_byte_read = block_end_byte
    lines_to_go = total_lines_wanted
    block_number = -1
    blocks = []
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
