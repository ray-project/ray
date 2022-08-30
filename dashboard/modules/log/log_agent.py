import logging
from typing import Optional, Tuple

import ray.dashboard.modules.log.log_consts as log_consts
import ray.dashboard.utils as dashboard_utils
import ray.dashboard.optional_utils as dashboard_optional_utils
import asyncio
import grpc
import io
import os

from pathlib import Path

from ray.core.generated import reporter_pb2
from ray.core.generated import reporter_pb2_grpc

logger = logging.getLogger(__name__)
routes = dashboard_optional_utils.ClassMethodRouteTable

# 64 KB
BLOCK_SIZE = 1 << 16

# Keep-alive interval for reading the file
DEFAULT_KEEP_ALIVE_INTERVAL_SEC = 1


def _find_tail_start_from_last_lines(
    file: io.BufferedIOBase, lines_to_tail: int, block_size: int = BLOCK_SIZE
) -> Tuple[int, int]:
    """
    Find the start offset of last `lines_to_tail` lines in a file.
    If the file has no trailing `\n`, it will be treated as if a new line exists.

    Args:
        file: A binary file object
        lines_to_tail: Number of lines to tail
        block_size: Number of bytes for a single file read, tunable for
            testing purpose mainly

    Return:
        Tuple of offsets:
            offset_read_start: the start offset of the last `lines_to_tail`
            offset_file_end: the end of file offset

    Note:
        If file is appended concurrently, this function might find an offset
        that contains more than last `lines_to_tail` from the end of the file.

        This function doesn't handle concurrent overwrite to the file in another
        thread/process. E.g., if the file is truncated/overwritten, the behavior
        is undefined, depending how the file has changed.

    """

    assert (
        lines_to_tail >= 0
    ), "Non negative input required for number of lines to tail "

    file.seek(0, 2)
    # The end offset of the last block currently being read
    offset_file_end = file.tell()
    # Number of bytes that should be tailed from the end of the file
    nbytes_from_end = 0
    # Number of blocks read so far
    num_blocks_read = 0

    if lines_to_tail == 0:
        return offset_file_end, offset_file_end

    # Non new line terminating file, adjust the line count and treat the last
    # line as a new line
    file.seek(-1, 2)
    if file.read(1) != b"\n":
        lines_to_tail -= 1

    # Remaining number of lines to tail
    lines_more = lines_to_tail

    if offset_file_end > block_size:
        # More than 1 block data, then read the previous block from
        # the end offset
        read_offset = offset_file_end - block_size
    else:
        # Less than 1 block data, seek to the beginning directly
        read_offset = 0

    # So that we know how much to read on the last block (the block 0)
    prev_offset = offset_file_end

    while lines_more >= 0 and read_offset >= 0:

        # Seek to the current block start
        file.seek(read_offset, 0)

        # Read the current block (or less than block) data
        block_data = file.read(min(block_size, prev_offset - read_offset))

        num_lines = block_data.count(b"\n")
        if num_lines > lines_more:
            # This is the last block.
            # Need to find the offset of exact number of lines to tail
            # in the block.
            # Use `split` here to split away the last
            # n=lines_more lines. The length to be tailed lines will be
            # in the last un-split token:
            # E.g.,
            #   block           = 0123\n5678\nabcde\n
            #   lines_to_tail   = 1
            #   num_lines       = 3
            #
            #   block.split(num_lines - lines_to_tail)
            #   will produce splits:
            #
            #   block           = 0123\n5678\nabcde\n
            #                     -----|-----|-------
            #                                 ^
            #                           where tail should start.
            lines = block_data.split(b"\n", num_lines - lines_more)

            # len(lines[0]) + 1 for the new line character split
            nbytes_from_end += len(lines[-1])
            logger.debug(
                f"Found {lines_to_tail} lines from last {nbytes_from_end} bytes"
            )
            break

        # Need to read more blocks.
        lines_more -= num_lines
        num_blocks_read += 1
        nbytes_from_end += len(block_data)

        logger.debug(
            f"lines_more={lines_more}, nbytes_from_end={nbytes_from_end}, "
            f"read_offset={read_offset}, end={offset_file_end}"
        )

        if read_offset == 0:
            # We have read all blocks (since the start)
            if lines_more > 0:
                logger.debug(
                    f"Read all {nbytes_from_end} from {file.name}, "
                    f"but only found {lines_to_tail - lines_more} / {lines_to_tail} "
                    "lines."
                )
            break

        # Continuing with the previous block
        prev_offset = read_offset
        read_offset = max(0, read_offset - block_size)

    offset_read_start = offset_file_end - nbytes_from_end
    assert (
        offset_read_start >= 0
    ), f"Read start offset({offset_read_start}) should be non-negative"
    logger.debug(
        f"tailing {file.name}'s last {lines_to_tail} lines "
        f"from {offset_read_start} to {offset_file_end}"
    )
    return offset_read_start, offset_file_end


async def _stream_log_in_chunk(
    context: grpc.aio.ServicerContext,
    file: io.BufferedIOBase,
    start_offset: int,
    end_offset: Optional[int] = None,
    keep_alive_interval_sec: int = -1,
    block_size: int = BLOCK_SIZE,
):
    """Streaming log in chunk from start to end offset.

    Stream binary file content in chunks from start offset to an end
    offset if provided, else to the end of the file.

    Args:
        context: gRPC server side context
        file: Binary file to stream
        start_offset: File offset where streaming starts
        end_offset: If None, implying streaming til the EOF.
        keep_alive_interval_sec: Duration for which streaming will be
            retried when reaching the file end
        block_size: Number of bytes per chunk, exposed for testing

    Return:
        Async generator of StreamReply
    """
    assert "b" in file.mode, "Only binary file is supported."
    assert not (
        keep_alive_interval_sec >= 0 and end_offset is not None
    ), "Keep-alive is not allowed when specifying an end offset"

    file.seek(start_offset, 0)
    cur_offset = start_offset

    # Until gRPC is done
    while not context.done():
        # Read in block
        if end_offset is not None:
            to_read = min(end_offset - cur_offset, block_size)
        else:
            to_read = block_size

        bytes = file.read(to_read)

        if bytes == b"":
            # Stop reading
            if keep_alive_interval_sec >= 0:
                await asyncio.sleep(keep_alive_interval_sec)
                # Try reading again
                continue

            # Have read the entire file, done
            break
        logger.debug(f"Sending {len(bytes)} bytes at {cur_offset}")
        yield reporter_pb2.StreamLogReply(data=bytes)

        # Have read the requested section [start_offset, end_offset), done
        cur_offset += len(bytes)
        if end_offset is not None and cur_offset >= end_offset:
            break


class LogAgentV1Grpc(dashboard_utils.DashboardAgentModule):
    def __init__(self, dashboard_agent):
        super().__init__(dashboard_agent)

    async def run(self, server):
        if server:
            reporter_pb2_grpc.add_LogServiceServicer_to_server(self, server)

    @staticmethod
    def is_minimal_module():
        # Dashboard is only available with non-minimal install now.
        return False

    async def ListLogs(self, request, context):
        """
        Lists all files in the active Ray logs directory.

        Part of `LogService` gRPC.

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

        Part of `LogService` gRPC.

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

                # Default stream entire file
                start_offset = 0
                end_offset = None
                if lines != -1:
                    # If specified tail line number,
                    # look for the file offset with the line count
                    start_offset, end_offset = _find_tail_start_from_last_lines(
                        f, lines
                    )

                # If keep alive: following the log every 'interval'
                keep_alive_interval_sec = -1
                if request.keep_alive:
                    keep_alive_interval_sec = (
                        request.interval
                        if request.interval
                        else DEFAULT_KEEP_ALIVE_INTERVAL_SEC
                    )

                    # When following (keep_alive), it will read beyond the end
                    end_offset = None

                logger.debug(
                    f"Tailing logs from {start_offset} to {end_offset} for {lines}, "
                    f"with keep_alive={keep_alive_interval_sec}"
                )

                # Read and send the file data in chunk
                async for chunk_res in _stream_log_in_chunk(
                    context=context,
                    file=f,
                    start_offset=start_offset,
                    end_offset=end_offset,
                    keep_alive_interval_sec=keep_alive_interval_sec,
                ):
                    yield chunk_res
