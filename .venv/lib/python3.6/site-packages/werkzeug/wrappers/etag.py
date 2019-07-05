from .._compat import string_types
from .._internal import _get_environ
from ..datastructures import ContentRange
from ..datastructures import RequestCacheControl
from ..datastructures import ResponseCacheControl
from ..http import generate_etag
from ..http import http_date
from ..http import is_resource_modified
from ..http import parse_cache_control_header
from ..http import parse_content_range_header
from ..http import parse_date
from ..http import parse_etags
from ..http import parse_if_range_header
from ..http import parse_range_header
from ..http import quote_etag
from ..http import unquote_etag
from ..utils import cached_property
from ..utils import header_property
from ..wrappers.base_response import _clean_accept_ranges
from ..wsgi import _RangeWrapper


class ETagRequestMixin(object):
    """Add entity tag and cache descriptors to a request object or object with
    a WSGI environment available as :attr:`~BaseRequest.environ`.  This not
    only provides access to etags but also to the cache control header.
    """

    @cached_property
    def cache_control(self):
        """A :class:`~werkzeug.datastructures.RequestCacheControl` object
        for the incoming cache control headers.
        """
        cache_control = self.environ.get("HTTP_CACHE_CONTROL")
        return parse_cache_control_header(cache_control, None, RequestCacheControl)

    @cached_property
    def if_match(self):
        """An object containing all the etags in the `If-Match` header.

        :rtype: :class:`~werkzeug.datastructures.ETags`
        """
        return parse_etags(self.environ.get("HTTP_IF_MATCH"))

    @cached_property
    def if_none_match(self):
        """An object containing all the etags in the `If-None-Match` header.

        :rtype: :class:`~werkzeug.datastructures.ETags`
        """
        return parse_etags(self.environ.get("HTTP_IF_NONE_MATCH"))

    @cached_property
    def if_modified_since(self):
        """The parsed `If-Modified-Since` header as datetime object."""
        return parse_date(self.environ.get("HTTP_IF_MODIFIED_SINCE"))

    @cached_property
    def if_unmodified_since(self):
        """The parsed `If-Unmodified-Since` header as datetime object."""
        return parse_date(self.environ.get("HTTP_IF_UNMODIFIED_SINCE"))

    @cached_property
    def if_range(self):
        """The parsed `If-Range` header.

        .. versionadded:: 0.7

        :rtype: :class:`~werkzeug.datastructures.IfRange`
        """
        return parse_if_range_header(self.environ.get("HTTP_IF_RANGE"))

    @cached_property
    def range(self):
        """The parsed `Range` header.

        .. versionadded:: 0.7

        :rtype: :class:`~werkzeug.datastructures.Range`
        """
        return parse_range_header(self.environ.get("HTTP_RANGE"))


class ETagResponseMixin(object):
    """Adds extra functionality to a response object for etag and cache
    handling.  This mixin requires an object with at least a `headers`
    object that implements a dict like interface similar to
    :class:`~werkzeug.datastructures.Headers`.

    If you want the :meth:`freeze` method to automatically add an etag, you
    have to mixin this method before the response base class.  The default
    response class does not do that.
    """

    @property
    def cache_control(self):
        """The Cache-Control general-header field is used to specify
        directives that MUST be obeyed by all caching mechanisms along the
        request/response chain.
        """

        def on_update(cache_control):
            if not cache_control and "cache-control" in self.headers:
                del self.headers["cache-control"]
            elif cache_control:
                self.headers["Cache-Control"] = cache_control.to_header()

        return parse_cache_control_header(
            self.headers.get("cache-control"), on_update, ResponseCacheControl
        )

    def _wrap_response(self, start, length):
        """Wrap existing Response in case of Range Request context."""
        if self.status_code == 206:
            self.response = _RangeWrapper(self.response, start, length)

    def _is_range_request_processable(self, environ):
        """Return ``True`` if `Range` header is present and if underlying
        resource is considered unchanged when compared with `If-Range` header.
        """
        return (
            "HTTP_IF_RANGE" not in environ
            or not is_resource_modified(
                environ,
                self.headers.get("etag"),
                None,
                self.headers.get("last-modified"),
                ignore_if_range=False,
            )
        ) and "HTTP_RANGE" in environ

    def _process_range_request(self, environ, complete_length=None, accept_ranges=None):
        """Handle Range Request related headers (RFC7233).  If `Accept-Ranges`
        header is valid, and Range Request is processable, we set the headers
        as described by the RFC, and wrap the underlying response in a
        RangeWrapper.

        Returns ``True`` if Range Request can be fulfilled, ``False`` otherwise.

        :raises: :class:`~werkzeug.exceptions.RequestedRangeNotSatisfiable`
                 if `Range` header could not be parsed or satisfied.
        """
        from ..exceptions import RequestedRangeNotSatisfiable

        if accept_ranges is None:
            return False
        self.headers["Accept-Ranges"] = accept_ranges
        if not self._is_range_request_processable(environ) or complete_length is None:
            return False
        parsed_range = parse_range_header(environ.get("HTTP_RANGE"))
        if parsed_range is None:
            raise RequestedRangeNotSatisfiable(complete_length)
        range_tuple = parsed_range.range_for_length(complete_length)
        content_range_header = parsed_range.to_content_range_header(complete_length)
        if range_tuple is None or content_range_header is None:
            raise RequestedRangeNotSatisfiable(complete_length)
        content_length = range_tuple[1] - range_tuple[0]
        # Be sure not to send 206 response
        # if requested range is the full content.
        if content_length != complete_length:
            self.headers["Content-Length"] = content_length
            self.content_range = content_range_header
            self.status_code = 206
            self._wrap_response(range_tuple[0], content_length)
            return True
        return False

    def make_conditional(
        self, request_or_environ, accept_ranges=False, complete_length=None
    ):
        """Make the response conditional to the request.  This method works
        best if an etag was defined for the response already.  The `add_etag`
        method can be used to do that.  If called without etag just the date
        header is set.

        This does nothing if the request method in the request or environ is
        anything but GET or HEAD.

        For optimal performance when handling range requests, it's recommended
        that your response data object implements `seekable`, `seek` and `tell`
        methods as described by :py:class:`io.IOBase`.  Objects returned by
        :meth:`~werkzeug.wsgi.wrap_file` automatically implement those methods.

        It does not remove the body of the response because that's something
        the :meth:`__call__` function does for us automatically.

        Returns self so that you can do ``return resp.make_conditional(req)``
        but modifies the object in-place.

        :param request_or_environ: a request object or WSGI environment to be
                                   used to make the response conditional
                                   against.
        :param accept_ranges: This parameter dictates the value of
                              `Accept-Ranges` header. If ``False`` (default),
                              the header is not set. If ``True``, it will be set
                              to ``"bytes"``. If ``None``, it will be set to
                              ``"none"``. If it's a string, it will use this
                              value.
        :param complete_length: Will be used only in valid Range Requests.
                                It will set `Content-Range` complete length
                                value and compute `Content-Length` real value.
                                This parameter is mandatory for successful
                                Range Requests completion.
        :raises: :class:`~werkzeug.exceptions.RequestedRangeNotSatisfiable`
                 if `Range` header could not be parsed or satisfied.
        """
        environ = _get_environ(request_or_environ)
        if environ["REQUEST_METHOD"] in ("GET", "HEAD"):
            # if the date is not in the headers, add it now.  We however
            # will not override an already existing header.  Unfortunately
            # this header will be overriden by many WSGI servers including
            # wsgiref.
            if "date" not in self.headers:
                self.headers["Date"] = http_date()
            accept_ranges = _clean_accept_ranges(accept_ranges)
            is206 = self._process_range_request(environ, complete_length, accept_ranges)
            if not is206 and not is_resource_modified(
                environ,
                self.headers.get("etag"),
                None,
                self.headers.get("last-modified"),
            ):
                if parse_etags(environ.get("HTTP_IF_MATCH")):
                    self.status_code = 412
                else:
                    self.status_code = 304
            if (
                self.automatically_set_content_length
                and "content-length" not in self.headers
            ):
                length = self.calculate_content_length()
                if length is not None:
                    self.headers["Content-Length"] = length
        return self

    def add_etag(self, overwrite=False, weak=False):
        """Add an etag for the current response if there is none yet."""
        if overwrite or "etag" not in self.headers:
            self.set_etag(generate_etag(self.get_data()), weak)

    def set_etag(self, etag, weak=False):
        """Set the etag, and override the old one if there was one."""
        self.headers["ETag"] = quote_etag(etag, weak)

    def get_etag(self):
        """Return a tuple in the form ``(etag, is_weak)``.  If there is no
        ETag the return value is ``(None, None)``.
        """
        return unquote_etag(self.headers.get("ETag"))

    def freeze(self, no_etag=False):
        """Call this method if you want to make your response object ready for
        pickeling.  This buffers the generator if there is one.  This also
        sets the etag unless `no_etag` is set to `True`.
        """
        if not no_etag:
            self.add_etag()
        super(ETagResponseMixin, self).freeze()

    accept_ranges = header_property(
        "Accept-Ranges",
        doc="""The `Accept-Ranges` header. Even though the name would
        indicate that multiple values are supported, it must be one
        string token only.

        The values ``'bytes'`` and ``'none'`` are common.

        .. versionadded:: 0.7""",
    )

    def _get_content_range(self):
        def on_update(rng):
            if not rng:
                del self.headers["content-range"]
            else:
                self.headers["Content-Range"] = rng.to_header()

        rv = parse_content_range_header(self.headers.get("content-range"), on_update)
        # always provide a content range object to make the descriptor
        # more user friendly.  It provides an unset() method that can be
        # used to remove the header quickly.
        if rv is None:
            rv = ContentRange(None, None, None, on_update=on_update)
        return rv

    def _set_content_range(self, value):
        if not value:
            del self.headers["content-range"]
        elif isinstance(value, string_types):
            self.headers["Content-Range"] = value
        else:
            self.headers["Content-Range"] = value.to_header()

    content_range = property(
        _get_content_range,
        _set_content_range,
        doc="""The ``Content-Range`` header as
        :class:`~werkzeug.datastructures.ContentRange` object. Even if
        the header is not set it wil provide such an object for easier
        manipulation.

        .. versionadded:: 0.7""",
    )
    del _get_content_range, _set_content_range
