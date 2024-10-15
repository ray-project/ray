import logging

from ray.dashboard.utils import close_logger_file_descriptor


def test_close_logger_file_descriptor():
    logger_format = "%(message)s"
    logger = logging.getLogger("test_job_id")

    job_driver_log_path = "/tmp/ray.log"
    job_driver_handler = logging.FileHandler(job_driver_log_path)
    job_driver_formatter = logging.Formatter(logger_format)
    job_driver_handler.setFormatter(job_driver_formatter)
    logger.addHandler(job_driver_handler)

    assert len(logger.handlers) == 1
    close_logger_file_descriptor(logger)
    assert len(logger.handlers) == 0
