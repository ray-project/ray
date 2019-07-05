"""
X-Forwarded-For Proxy Fix
=========================

This module provides a middleware that adjusts the WSGI environ based on
``X-Forwarded-`` headers that proxies in front of an application may
set.

When an application is running behind a proxy server, WSGI may see the
request as coming from that server rather than the real client. Proxies
set various headers to track where the request actually came from.

This middleware should only be applied if the application is actually
behind such a proxy, and should be configured with the number of proxies
that are chained in front of it. Not all proxies set all the headers.
Since incoming headers can be faked, you must set how many proxies are
setting each header so the middleware knows what to trust.

.. autoclass:: ProxyFix

:copyright: 2007 Pallets
:license: BSD-3-Clause
"""
import warnings


class ProxyFix(object):
    """Adjust the WSGI environ based on ``X-Forwarded-`` that proxies in
    front of the application may set.

    -   ``X-Forwarded-For`` sets ``REMOTE_ADDR``.
    -   ``X-Forwarded-Proto`` sets ``wsgi.url_scheme``.
    -   ``X-Forwarded-Host`` sets ``HTTP_HOST``, ``SERVER_NAME``, and
        ``SERVER_PORT``.
    -   ``X-Forwarded-Port`` sets ``HTTP_HOST`` and ``SERVER_PORT``.
    -   ``X-Forwarded-Prefix`` sets ``SCRIPT_NAME``.

    You must tell the middleware how many proxies set each header so it
    knows what values to trust. It is a security issue to trust values
    that came from the client rather than a proxy.

    The original values of the headers are stored in the WSGI
    environ as ``werkzeug.proxy_fix.orig``, a dict.

    :param app: The WSGI application to wrap.
    :param x_for: Number of values to trust for ``X-Forwarded-For``.
    :param x_proto: Number of values to trust for ``X-Forwarded-Proto``.
    :param x_host: Number of values to trust for ``X-Forwarded-Host``.
    :param x_port: Number of values to trust for ``X-Forwarded-Port``.
    :param x_prefix: Number of values to trust for
        ``X-Forwarded-Prefix``.
    :param num_proxies: Deprecated, use ``x_for`` instead.

    .. code-block:: python

        from werkzeug.middleware.proxy_fix import ProxyFix
        # App is behind one proxy that sets the -For and -Host headers.
        app = ProxyFix(app, x_for=1, x_host=1)

    .. versionchanged:: 0.15
        All headers support multiple values. The ``num_proxies``
        argument is deprecated. Each header is configured with a
        separate number of trusted proxies.

    .. versionchanged:: 0.15
        Original WSGI environ values are stored in the
        ``werkzeug.proxy_fix.orig`` dict. ``orig_remote_addr``,
        ``orig_wsgi_url_scheme``, and ``orig_http_host`` are deprecated
        and will be removed in 1.0.

    .. versionchanged:: 0.15
        Support ``X-Forwarded-Port`` and ``X-Forwarded-Prefix``.

    .. versionchanged:: 0.15
        ``X-Fowarded-Host`` and ``X-Forwarded-Port`` modify
        ``SERVER_NAME`` and ``SERVER_PORT``.
    """

    def __init__(
        self, app, num_proxies=None, x_for=1, x_proto=0, x_host=0, x_port=0, x_prefix=0
    ):
        self.app = app
        self.x_for = x_for
        self.x_proto = x_proto
        self.x_host = x_host
        self.x_port = x_port
        self.x_prefix = x_prefix
        self.num_proxies = num_proxies

    @property
    def num_proxies(self):
        """The number of proxies setting ``X-Forwarded-For`` in front
        of the application.

        .. deprecated:: 0.15
            A separate number of trusted proxies is configured for each
            header. ``num_proxies`` maps to ``x_for``. This method will
            be removed in 1.0.

        :internal:
        """
        warnings.warn(
            "'num_proxies' is deprecated as of version 0.15 and will be"
            " removed in version 1.0. Use 'x_for' instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.x_for

    @num_proxies.setter
    def num_proxies(self, value):
        if value is not None:
            warnings.warn(
                "'num_proxies' is deprecated as of version 0.15 and"
                " will be removed in version 1.0. Use 'x_for' instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            self.x_for = value

    def get_remote_addr(self, forwarded_for):
        """Get the real ``remote_addr`` by looking backwards ``x_for``
        number of values in the ``X-Forwarded-For`` header.

        :param forwarded_for: List of values parsed from the
            ``X-Forwarded-For`` header.
        :return: The real ``remote_addr``, or ``None`` if there were not
            at least ``x_for`` values.

        .. deprecated:: 0.15
            This is handled internally for each header. This method will
            be removed in 1.0.

        .. versionchanged:: 0.9
            Use ``num_proxies`` instead of always picking the first
            value.

        .. versionadded:: 0.8
        """
        warnings.warn(
            "'get_remote_addr' is deprecated as of version 0.15 and"
            " will be removed in version 1.0. It is now handled"
            " internally for each header.",
            DeprecationWarning,
        )
        return self._get_trusted_comma(self.x_for, ",".join(forwarded_for))

    def _get_trusted_comma(self, trusted, value):
        """Get the real value from a comma-separated header based on the
        configured number of trusted proxies.

        :param trusted: Number of values to trust in the header.
        :param value: Header value to parse.
        :return: The real value, or ``None`` if there are fewer values
            than the number of trusted proxies.

        .. versionadded:: 0.15
        """
        if not (trusted and value):
            return
        values = [x.strip() for x in value.split(",")]
        if len(values) >= trusted:
            return values[-trusted]

    def __call__(self, environ, start_response):
        """Modify the WSGI environ based on the various ``Forwarded``
        headers before calling the wrapped application. Store the
        original environ values in ``werkzeug.proxy_fix.orig_{key}``.
        """
        environ_get = environ.get
        orig_remote_addr = environ_get("REMOTE_ADDR")
        orig_wsgi_url_scheme = environ_get("wsgi.url_scheme")
        orig_http_host = environ_get("HTTP_HOST")
        environ.update(
            {
                "werkzeug.proxy_fix.orig": {
                    "REMOTE_ADDR": orig_remote_addr,
                    "wsgi.url_scheme": orig_wsgi_url_scheme,
                    "HTTP_HOST": orig_http_host,
                    "SERVER_NAME": environ_get("SERVER_NAME"),
                    "SERVER_PORT": environ_get("SERVER_PORT"),
                    "SCRIPT_NAME": environ_get("SCRIPT_NAME"),
                },
                # todo: remove deprecated keys
                "werkzeug.proxy_fix.orig_remote_addr": orig_remote_addr,
                "werkzeug.proxy_fix.orig_wsgi_url_scheme": orig_wsgi_url_scheme,
                "werkzeug.proxy_fix.orig_http_host": orig_http_host,
            }
        )

        x_for = self._get_trusted_comma(self.x_for, environ_get("HTTP_X_FORWARDED_FOR"))
        if x_for:
            environ["REMOTE_ADDR"] = x_for

        x_proto = self._get_trusted_comma(
            self.x_proto, environ_get("HTTP_X_FORWARDED_PROTO")
        )
        if x_proto:
            environ["wsgi.url_scheme"] = x_proto

        x_host = self._get_trusted_comma(
            self.x_host, environ_get("HTTP_X_FORWARDED_HOST")
        )
        if x_host:
            environ["HTTP_HOST"] = x_host
            parts = x_host.split(":", 1)
            environ["SERVER_NAME"] = parts[0]
            if len(parts) == 2:
                environ["SERVER_PORT"] = parts[1]

        x_port = self._get_trusted_comma(
            self.x_port, environ_get("HTTP_X_FORWARDED_PORT")
        )
        if x_port:
            host = environ.get("HTTP_HOST")
            if host:
                parts = host.split(":", 1)
                host = parts[0] if len(parts) == 2 else host
                environ["HTTP_HOST"] = "%s:%s" % (host, x_port)
            environ["SERVER_PORT"] = x_port

        x_prefix = self._get_trusted_comma(
            self.x_prefix, environ_get("HTTP_X_FORWARDED_PREFIX")
        )
        if x_prefix:
            environ["SCRIPT_NAME"] = x_prefix

        return self.app(environ, start_response)
