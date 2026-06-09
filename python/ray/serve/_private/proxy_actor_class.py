from ray.serve._private.constants import RAY_SERVE_ENABLE_HA_PROXY
from ray.serve._private.haproxy import HAProxyManager
from ray.serve._private.proxy import ProxyActor

# NOTE: common extension point (Developer API); do not rename or change the
# signature without substantial justification. It lives here rather than in
# default_impl so the proxy classes can be imported eagerly: default_impl is
# imported by proxy/handle, so importing them from default_impl is circular.


def get_proxy_actor_class():
    if RAY_SERVE_ENABLE_HA_PROXY:
        return HAProxyManager

    return ProxyActor
