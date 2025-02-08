def get_logging_configurator():
    from ray._private.ray_logging.logging_config import DefaultLoggingConfigurator

    return DefaultLoggingConfigurator()


# Anyscale overrides


def get_logging_configurator():  # noqa: F811
    from ray.anyscale._private.ray_logging.logging_config import (
        AnyscaleLoggingConfigurator,
    )

    return AnyscaleLoggingConfigurator()
