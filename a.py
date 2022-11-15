import os
import logging
import logging.config


def initialise_logger(config):
    """
    Args:
        section (str): the log_configuration section for the service. Defaults to "log_configuration".

    """

    logging.config.fileConfig(fname=config, disable_existing_loggers=False)

    old_factory = logging.getLogRecordFactory()

    def my_record_factory(*args, **kwargs):
        """
        Standardized method to expose LogRecordFactory so new parameters
        that are semi static can be injected into CBD log records.

        Returns:
            [LogRecord]: Modified log record
        """
        record = old_factory(*args, **kwargs)

        # project and any other keys that may be used in index creation in elasticsearch
        # must be lowercase ( in scenario where kafka logging is used )
        record.project = "alex"
        record.microservice = "alex"
        record.hostname = "alex"
        record.local_ip = "alex"

        # all Mesos containers in production have this environment variable.
        # if it is not present, then we are not in production
        record.mesos_task_id = "alex"

        return record

    logging.setLogRecordFactory(my_record_factory)


def main():
    text = """[loggers]
    keys=root,test_logger,Test_Class
    
    [handlers]
    keys=consoleHandler

    [formatters]
    keys=consoleFormatter
    
    [logger_root]
    level=INFO
    handlers=consoleHandler
    
    [logger_test_logger]
    level=INFO
    handlers=consoleHandler
    qualname=test_logger
    propagate=0
    
    [logger_Test_Class]
    level=INFO
    handlers=consoleHandler
    qualname=Test_Class
    propagate=0
    
    [handler_consoleHandler]
    class=StreamHandler
    level=INFO
    formatter=consoleFormatter
    args=(sys.stdout,)
    
    [formatter_consoleFormatter]
    format = %(asctime)s.%(msecs)03d %(project)s %(microservice)s %(levelname)s %(hostname)s %(local_ip)s %(name)s %(filename)s %(lineno)d %(funcName)s %(module)s %(processName)s %(process)d %(threadName)s %(thread)d  %(message)s
    datefmt= %Y-%m-%d %H:%M:%S   
    """

    with open(os.path.join(os.getcwd(), "config.ini"), "w") as conffile:
        conffile.write(text)

    initialise_logger(config="config.ini")

    logging.info("RAY NOT IMPORTED")

    import ray

    logging.info("RAY IMPORTED")


if __name__ == '__main__':
    main()
