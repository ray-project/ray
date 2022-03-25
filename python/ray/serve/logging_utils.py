# Format:
# <timestamp> [host:pid] <deployment> <replica> - "METHOD" <STATUS>
#
# METHOD is either:
# GET / HTTP/1.1
#     - or -
# HANDLE method-name
#
# STATUS is either:
# <status_code>
#     - or -
# OK/EXCEPTION

import logging
from typing import Optional

logger = logging.getLogger("ray.serve")

DEFAULT_LOG_FMT = (
    "%(levelname)s %(asctime)s %(component)s %(component_id)s - %(message)s"
)


def get_component_logger(
    *,
    component: str,
    component_id: str,
    log_level: Optional[int] = logging.INFO,
    log_to_stream: bool = True,
    log_file_path: Optional[str] = None
):
    logger.setLevel(log_level)
    formatter = logging.Formatter(DEFAULT_LOG_FMT)
    if log_to_stream:
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

    if log_file_path is not None:
        file_handler = logging.FileHandler(log_file_path)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logging.LoggerAdapter(
        logger, extra={"component": component, "component_id": component_id}
    )
