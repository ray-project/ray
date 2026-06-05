from ray.serve._private.constants import RAY_SERVE_ENABLE_HA_PROXY
from ray.serve._private.haproxy import HAProxyManager
from ray.serve._private.proxy import ProxyActor

# NOTE: Please read carefully before changing!
#
# This is a common extension point, therefore it should be changed as a
# Developer API, ie the method should not be renamed, have its API modified
# w/o substantial enough justification.
#
# It lives here rather than in default_impl so the proxy actor classes can be
# imported eagerly: default_impl is imported by proxy/handle, so importing the
# proxy from default_impl would be a circular import.


def get_proxy_actor_class():
    if RAY_SERVE_ENABLE_HA_PROXY:
        return HAProxyManager

    return ProxyActor
