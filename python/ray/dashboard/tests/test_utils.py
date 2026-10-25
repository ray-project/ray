import logging
import sys

import pytest

from ray.dashboard.utils import close_logger_file_descriptor


def test_close_logger_file_descriptor():
    logger_format = "%(message)s"
    logger = logging.getLogger("test_job_id")

    job_driver_log_path = "/tmp/ray.log"
    job_driver_handler = logging.FileHandler(job_driver_log_path)
    job_driver_formatter = logging.Formatter(logger_format)
    job_driver_handler.setFormatter(job_driver_formatter)
    logger.addHandler(job_driver_handler)

    assert job_driver_handler.stream.closed is False
    close_logger_file_descriptor(logger)
    assert job_driver_handler.stream is None


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
