def get_logging_configurator():
    from ray._private.ray_logging.logging_config import DefaultLoggingConfigurator

    return DefaultLoggingConfigurator()
