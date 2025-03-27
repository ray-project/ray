import os

from ray._private.runtime_env.image_uri import ImageURIPlugin


def get_image_uri_plugin_cls():
    return ImageURIPlugin


def get_protocols_provider():
    from ray._private.runtime_env.protocol import ProtocolsProvider

    return ProtocolsProvider


# Anyscale overrides


def get_image_uri_plugin_cls():  # noqa: F811
    from ray.anyscale._private.runtime_env.image_uri import AnyscaleImageURIPlugin

    # If rayturbo runs on k8s, use the OSS plugin to launch the nested container.
    if os.environ.get("KUBERNETES_SERVICE_HOST"):
        return ImageURIPlugin

    return AnyscaleImageURIPlugin


def get_protocols_provider():  # noqa: F811
    from ray.anyscale._private.runtime_env.protocol import AnyscaleProtocolsProvider

    return AnyscaleProtocolsProvider
