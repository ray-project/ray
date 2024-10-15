from doc.source.serve.doc_code.streaming_tutorial import print


def close_logger_file_descriptor(logger_instance):
    for handler in logger_instance.handlers:
        handler.close()
        logger_instance.removeHandler(handler)

import logging

LOGGER_FORMAT = "%(asctime)s\t%(levelname)s %(filename)s:%(lineno)s -- %(message)s"
def get_job_driver_logger(job_id: str) -> logging.Logger:
    """Return job driver logger to log messages to the job driver log file.

    If this function is called for the first time, configure the logger.
    """
    job_driver_logger = logging.getLogger(f"{__name__}.driver-{job_id}")

    # Configure the logger if it's not already configured.
    if not job_driver_logger.handlers:
        job_driver_log_path = "/tmp/ray.log"
        job_driver_handler = logging.FileHandler(job_driver_log_path)
        job_driver_formatter = logging.Formatter(LOGGER_FORMAT)
        job_driver_handler.setFormatter(job_driver_formatter)
        job_driver_logger.addHandler(job_driver_handler)


def test_close_logger_file_descriptor():
    logger = get_job_driver_logger("test_job_id")
    print("Njx",logger.handlers)

test_close_logger_file_descriptor()
