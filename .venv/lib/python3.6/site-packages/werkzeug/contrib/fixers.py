"""
Fixers
======

.. warning::
    .. deprecated:: 0.15
        ``ProxyFix`` has moved to :mod:`werkzeug.middleware.proxy_fix`.
        All other code in this module is deprecated and will be removed
        in version 1.0.

.. versionadded:: 0.5

This module includes various helpers that fix web server behavior.

.. autoclass:: ProxyFix
   :members:

.. autoclass:: CGIRootFix

.. autoclass:: PathInfoFromRequestUriFix

.. autoclass:: HeaderRewriterFix

.. autoclass:: InternetExplorerFix

:copyright: 2007 Pallets
:license: BSD-3-Clause
"""
import warnings

from ..datastructures import Headers
from ..datastructures import ResponseCacheControl
from ..http import parse_cache_control_header
from ..http import parse_options_header
from ..http import parse_set_header
from ..middleware.proxy_fix import ProxyFix as _ProxyFix
from ..useragents import UserAgent

try:
    from urllib.parse import unquote
except ImportError:
    from urllib import unquote


class CGIRootFix(object):
    """Wrap the application in this middleware if you are using FastCGI
    or CGI and you have problems with your app root being set to the CGI
    script's path instead of the path users are going to visit.

    :param app: the WSGI application
    :param app_root: Defaulting to ``'/'``, you can set this to
        something else if your app is mounted somewhere else.

    .. deprecated:: 0.15
        This middleware will be removed in version 1.0.

    .. versionchanged:: 0.9
        Added `app_root` parameter and renamed from
        ``LighttpdCGIRootFix``.
    """

    def __init__(self, app, app_root="/"):
        warnings.warn(
            "'CGIRootFix' is deprecated as of version 0.15 and will be"
            " removed in version 1.0.",
            DeprecationWarning,
            stacklevel=2,
        )
        self.app = app
        self.app_root = app_root.strip("/")

    def __call__(self, environ, start_response):
        environ["SCRIPT_NAME"] = self.app_root
        return self.app(environ, start_response)


class LighttpdCGIRootFix(CGIRootFix):
    def __init__(self, *args, **kwargs):
        warnings.warn(
            "'LighttpdCGIRootFix' is renamed 'CGIRootFix'. Both will be"
            " removed in version 1.0.",
            DeprecationWarning,
            stacklevel=2,
        )
        super(LighttpdCGIRootFix, self).__init__(*args, **kwargs)


class PathInfoFromRequestUriFix(object):
    """On windows environment variables are limited to the system charset
    which makes it impossible to store the `PATH_INFO` variable in the
    environment without loss of information on some systems.

    This is for example a problem for CGI scripts on a Windows Apache.

    This fixer works by recreating the `PATH_INFO` from `REQUEST_URI`,
    `REQUEST_URL`, or `UNENCODED_URL` (whatever is available).  Thus the
    fix can only be applied if the webserver supports either of these
    variables.

    :param app: the WSGI application

    .. deprecated:: 0.15
        This middleware will be removed in version 1.0.
    """

    def __init__(self, app):
        warnings.warn(
            "'PathInfoFromRequestUriFix' is deprecated as of version"
            " 0.15 and will be removed in version 1.0.",
            DeprecationWarning,
            stacklevel=2,
        )
        self.app = app

    def __call__(self, environ, start_response):
        for key in "REQUEST_URL", "REQUEST_URI", "UNENCODED_URL":
            if key not in environ:
                continue
            request_uri = unquote(environ[key])
            script_name = unquote(environ.get("SCRIPT_NAME", ""))
            if request_uri.startswith(script_name):
                environ["PATH_INFO"] = request_uri[len(script_name) :].split("?", 1)[0]
                break
        return self.app(environ, start_response)


class ProxyFix(_ProxyFix):
    """
    .. deprecated:: 0.15
        ``werkzeug.contrib.fixers.ProxyFix`` has moved to
        :mod:`werkzeug.middleware.proxy_fix`. This import will be
        removed in 1.0.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            "'werkzeug.contrib.fixers.ProxyFix' has moved to 'werkzeug"
            ".middleware.proxy_fix.ProxyFix'. This import is deprecated"
            " as of version 0.15 and will be removed in 1.0.",
            DeprecationWarning,
            stacklevel=2,
        )
        super(ProxyFix, self).__init__(*args, **kwargs)


class HeaderRewriterFix(object):
    """This middleware can remove response headers and add others.  This
    is for example useful to remove the `Date` header from responses if you
    are using a server that adds that header, no matter if it's present or
    not or to add `X-Powered-By` headers::

        app = HeaderRewriterFix(app, remove_headers=['Date'],
                                add_headers=[('X-Powered-By', 'WSGI')])

    :param app: the WSGI application
    :param remove_headers: a sequence of header keys that should be
                           removed.
    :param add_headers: a sequence of ``(key, value)`` tuples that should
                        be added.

    .. deprecated:: 0.15
        This middleware will be removed in 1.0.
    """

    def __init__(self, app, remove_headers=None, add_headers=None):
        warnings.warn(
            "'HeaderRewriterFix' is deprecated as of version 0.15 and"
            " will be removed in version 1.0.",
            DeprecationWarning,
            stacklevel=2,
        )
        self.app = app
        self.remove_headers = set(x.lower() for x in (remove_headers or ()))
        self.add_headers = list(add_headers or ())

    def __call__(self, environ, start_response):
        def rewriting_start_response(status, headers, exc_info=None):
            new_headers = []
            for key, value in headers:
                if key.lower() not in self.remove_headers:
                    new_headers.append((key, value))
            new_headers += self.add_headers
            return start_response(status, new_headers, exc_info)

        return self.app(environ, rewriting_start_response)


class InternetExplorerFix(object):
    """This middleware fixes a couple of bugs with Microsoft Internet
    Explorer.  Currently the following fixes are applied:

    -   removing of `Vary` headers for unsupported mimetypes which
        causes troubles with caching.  Can be disabled by passing
        ``fix_vary=False`` to the constructor.
        see: https://support.microsoft.com/en-us/help/824847

    -   removes offending headers to work around caching bugs in
        Internet Explorer if `Content-Disposition` is set.  Can be
        disabled by passing ``fix_attach=False`` to the constructor.

    If it does not detect affected Internet Explorer versions it won't touch
    the request / response.

    .. deprecated:: 0.15
        This middleware will be removed in 1.0.
    """

    # This code was inspired by Django fixers for the same bugs.  The
    # fix_vary and fix_attach fixers were originally implemented in Django
    # by Michael Axiak and is available as part of the Django project:
    #     https://code.djangoproject.com/ticket/4148

    def __init__(self, app, fix_vary=True, fix_attach=True):
        warnings.warn(
            "'InternetExplorerFix' is deprecated as of version 0.15 and"
            " will be removed in version 1.0.",
            DeprecationWarning,
            stacklevel=2,
        )
        self.app = app
        self.fix_vary = fix_vary
        self.fix_attach = fix_attach

    def fix_headers(self, environ, headers, status=None):
        if self.fix_vary:
            header = headers.get("content-type", "")
            mimetype, options = parse_options_header(header)
            if mimetype not in ("text/html", "text/plain", "text/sgml"):
                headers.pop("vary", None)

        if self.fix_attach and "content-disposition" in headers:
            pragma = parse_set_header(headers.get("pragma", ""))
            pragma.discard("no-cache")
            header = pragma.to_header()
            if not header:
                headers.pop("pragma", "")
            else:
                headers["Pragma"] = header
            header = headers.get("cache-control", "")
            if header:
                cc = parse_cache_control_header(header, cls=ResponseCacheControl)
                cc.no_cache = None
                cc.no_store = False
                header = cc.to_header()
                if not header:
                    headers.pop("cache-control", "")
                else:
                    headers["Cache-Control"] = header

    def run_fixed(self, environ, start_response):
        def fixing_start_response(status, headers, exc_info=None):
            headers = Headers(headers)
            self.fix_headers(environ, headers, status)
            return start_response(status, headers.to_wsgi_list(), exc_info)

        return self.app(environ, fixing_start_response)

    def __call__(self, environ, start_response):
        ua = UserAgent(environ)
        if ua.browser != "msie":
            return self.app(environ, start_response)
        return self.run_fixed(environ, start_response)
