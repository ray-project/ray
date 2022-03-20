import logging

import ray.dashboard.modules.log.log_utils as log_utils
import ray.dashboard.utils as dashboard_utils
import ray.dashboard.optional_utils as dashboard_optional_utils
import aiohttp.web as web
import asyncio

from typing import Iterator

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


class LogAgentV1(dashboard_utils.DashboardAgentModule):
    def __init__(self, dashboard_agent):
        super().__init__(dashboard_agent)
        log_utils.register_mimetypes()

    async def run(self, server):
        pass

    @staticmethod
    def is_minimal_module():
        return False

    @routes.get("/v1/api/logs/agent/file/{log_file_name}")
    async def get_log_lines(self, request):
        lines = int(request.query.get("lines", "1000"))
        log_file_name = request.match_info.get("log_file_name", None)

        if log_file_name is None:
            raise web.HTTPNotFound()
        else:
            with open(f"{self._dashboard_agent.log_dir}/{log_file_name}", "rb") as f:
                if lines == -1:
                    text = f.read()
                else:
                    text, _ = tail(f, lines)
            # TODO: improve by not buffering all bytes in memory and converting to
            # a StreamResponse instead?
            return web.Response(body=text)

    @routes.get("/v1/api/logs/agent/stream/{log_file_name}")
    async def stream_log_lines(self, request):
        lines = int(request.query.get("lines", "1000"))
        interval = float(request.query.get("interval", "0.5"))
        log_file_name = request.match_info.get("log_file_name", None)

        ws = web.WebSocketResponse()
        await ws.prepare(request)

        async def yield_bytes():
            try:
                with open(f"{self._dashboard_agent.log_dir}/{log_file_name}", "rb") as f:
                    if lines == -1:
                        bytes = f.read()
                        end = f.tell()
                    else:
                        bytes, end = tail(f, lines)
                    yield bytes
                    f.seek(end)

                    while True:
                        await asyncio.sleep(interval)
                        bytes = f.read()
                        if bytes != b"":
                            yield bytes

                print('websocket connection closed')

            except FileNotFoundError:
                yield b"could not find file"
            except Exception as e:
                yield f"Python Agent Logs Stream. Exception: {e}".encode()

        async for bytes in yield_bytes():
            await ws.send_bytes(bytes)

        return ws


def tail(f, lines=1000):
    """
    Inspired by: https://stackoverflow.com/a/136368/8299684

    Every block ends with a \n except for the last block.
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
