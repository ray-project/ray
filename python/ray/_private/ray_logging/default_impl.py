def get_dict_config_provider():
    from ray._private.ray_logging.logging_config import DefaultDictConfigProvider

    return DefaultDictConfigProvider()
