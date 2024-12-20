from ray._private.runtime_env.image_uri import ImageURIPlugin


def get_image_uri_plugin(ray_tmp_dir: str):
    return ImageURIPlugin(ray_tmp_dir)


def get_protocols_provider():
    from ray._private.runtime_env.protocol import ProtocolsProvider

    return ProtocolsProvider


# Anyscale overrides


def get_image_uri_plugin(ray_tmp_dir: str):  # noqa: F811
    from ray.anyscale._private.runtime_env.image_uri import AnyscaleImageURIPlugin

    return AnyscaleImageURIPlugin()
