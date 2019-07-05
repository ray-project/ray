import warnings

from .._compat import integer_types
from .._compat import string_types
from .._compat import text_type
from .._compat import to_bytes
from .._compat import to_native
from ..datastructures import Headers
from ..http import dump_cookie
from ..http import HTTP_STATUS_CODES
from ..http import remove_entity_headers
from ..urls import iri_to_uri
from ..urls import url_join
from ..utils import get_content_type
from ..wsgi import ClosingIterator
from ..wsgi import get_current_url


def _run_wsgi_app(*args):
    """This function replaces itself to ensure that the test module is not
    imported unless required.  DO NOT USE!
    """
    global _run_wsgi_app
    from ..test import run_wsgi_app as _run_wsgi_app

    return _run_wsgi_app(*args)


def _warn_if_string(iterable):
    """Helper for the response objects to check if the iterable returned
    to the WSGI server is not a string.
    """
    if isinstance(iterable, string_types):
        warnings.warn(
            "Response iterable was set to a string. This will appear to"
            " work but means that the server will send the data to the"
            " client one character at a time. This is almost never"
            " intended behavior, use 'response.data' to assign strings"
            " to the response object.",
            stacklevel=2,
        )


def _iter_encoded(iterable, charset):
    for item in iterable:
        if isinstance(item, text_type):
            yield item.encode(charset)
        else:
            yield item


def _clean_accept_ranges(accept_ranges):
    if accept_ranges is True:
        return "bytes"
    elif accept_ranges is False:
        return "none"
    elif isinstance(accept_ranges, text_type):
        return to_native(accept_ranges)
    raise ValueError("Invalid accept_ranges value")


class BaseResponse(object):
    """Base response class.  The most important fact about a response object
    is that it's a regular WSGI application.  It's initialized with a couple
    of response parameters (headers, body, status code etc.) and will start a
    valid WSGI response when called with the environ and start response
    callable.

    Because it's a WSGI application itself processing usually ends before the
    actual response is sent to the server.  This helps debugging systems
    because they can catch all the exceptions before responses are started.

    Here a small example WSGI application that takes advantage of the
    response objects::

        from werkzeug.wrappers import BaseResponse as Response

        def index():
            return Response('Index page')

        def application(environ, start_response):
            path = environ.get('PATH_INFO') or '/'
            if path == '/':
                response = index()
            else:
                response = Response('Not Found', status=404)
            return response(environ, start_response)

    Like :class:`BaseRequest` which object is lacking a lot of functionality
    implemented in mixins.  This gives you a better control about the actual
    API of your response objects, so you can create subclasses and add custom
    functionality.  A full featured response object is available as
    :class:`Response` which implements a couple of useful mixins.

    To enforce a new type of already existing responses you can use the
    :meth:`force_type` method.  This is useful if you're working with different
    subclasses of response objects and you want to post process them with a
    known interface.

    Per default the response object will assume all the text data is `utf-8`
    encoded.  Please refer to :doc:`the unicode chapter </unicode>` for more
    details about customizing the behavior.

    Response can be any kind of iterable or string.  If it's a string it's
    considered being an iterable with one item which is the string passed.
    Headers can be a list of tuples or a
    :class:`~werkzeug.datastructures.Headers` object.

    Special note for `mimetype` and `content_type`:  For most mime types
    `mimetype` and `content_type` work the same, the difference affects
    only 'text' mimetypes.  If the mimetype passed with `mimetype` is a
    mimetype starting with `text/`, the charset parameter of the response
    object is appended to it.  In contrast the `content_type` parameter is
    always added as header unmodified.

    .. versionchanged:: 0.5
       the `direct_passthrough` parameter was added.

    :param response: a string or response iterable.
    :param status: a string with a status or an integer with the status code.
    :param headers: a list of headers or a
                    :class:`~werkzeug.datastructures.Headers` object.
    :param mimetype: the mimetype for the response.  See notice above.
    :param content_type: the content type for the response.  See notice above.
    :param direct_passthrough: if set to `True` :meth:`iter_encoded` is not
                               called before iteration which makes it
                               possible to pass special iterators through
                               unchanged (see :func:`wrap_file` for more
                               details.)
    """

    #: the charset of the response.
    charset = "utf-8"

    #: the default status if none is provided.
    default_status = 200

    #: the default mimetype if none is provided.
    default_mimetype = "text/plain"

    #: if set to `False` accessing properties on the response object will
    #: not try to consume the response iterator and convert it into a list.
    #:
    #: .. versionadded:: 0.6.2
    #:
    #:    That attribute was previously called `implicit_seqence_conversion`.
    #:    (Notice the typo).  If you did use this feature, you have to adapt
    #:    your code to the name change.
    implicit_sequence_conversion = True

    #: Should this response object correct the location header to be RFC
    #: conformant?  This is true by default.
    #:
    #: .. versionadded:: 0.8
    autocorrect_location_header = True

    #: Should this response object automatically set the content-length
    #: header if possible?  This is true by default.
    #:
    #: .. versionadded:: 0.8
    automatically_set_content_length = True

    #: Warn if a cookie header exceeds this size. The default, 4093, should be
    #: safely `supported by most browsers <cookie_>`_. A cookie larger than
    #: this size will still be sent, but it may be ignored or handled
    #: incorrectly by some browsers. Set to 0 to disable this check.
    #:
    #: .. versionadded:: 0.13
    #:
    #: .. _`cookie`: http://browsercookielimits.squawky.net/
    max_cookie_size = 4093

    def __init__(
        self,
        response=None,
        status=None,
        headers=None,
        mimetype=None,
        content_type=None,
        direct_passthrough=False,
    ):
        if isinstance(headers, Headers):
            self.headers = headers
        elif not headers:
            self.headers = Headers()
        else:
            self.headers = Headers(headers)

        if content_type is None:
            if mimetype is None and "content-type" not in self.headers:
                mimetype = self.default_mimetype
            if mimetype is not None:
                mimetype = get_content_type(mimetype, self.charset)
            content_type = mimetype
        if content_type is not None:
            self.headers["Content-Type"] = content_type
        if status is None:
            status = self.default_status
        if isinstance(status, integer_types):
            self.status_code = status
        else:
            self.status = status

        self.direct_passthrough = direct_passthrough
        self._on_close = []

        # we set the response after the headers so that if a class changes
        # the charset attribute, the data is set in the correct charset.
        if response is None:
            self.response = []
        elif isinstance(response, (text_type, bytes, bytearray)):
            self.set_data(response)
        else:
            self.response = response

    def call_on_close(self, func):
        """Adds a function to the internal list of functions that should
        be called as part of closing down the response.  Since 0.7 this
        function also returns the function that was passed so that this
        can be used as a decorator.

        .. versionadded:: 0.6
        """
        self._on_close.append(func)
        return func

    def __repr__(self):
        if self.is_sequence:
            body_info = "%d bytes" % sum(map(len, self.iter_encoded()))
        else:
            body_info = "streamed" if self.is_streamed else "likely-streamed"
        return "<%s %s [%s]>" % (self.__class__.__name__, body_info, self.status)

    @classmethod
    def force_type(cls, response, environ=None):
        """Enforce that the WSGI response is a response object of the current
        type.  Werkzeug will use the :class:`BaseResponse` internally in many
        situations like the exceptions.  If you call :meth:`get_response` on an
        exception you will get back a regular :class:`BaseResponse` object, even
        if you are using a custom subclass.

        This method can enforce a given response type, and it will also
        convert arbitrary WSGI callables into response objects if an environ
        is provided::

            # convert a Werkzeug response object into an instance of the
            # MyResponseClass subclass.
            response = MyResponseClass.force_type(response)

            # convert any WSGI application into a response object
            response = MyResponseClass.force_type(response, environ)

        This is especially useful if you want to post-process responses in
        the main dispatcher and use functionality provided by your subclass.

        Keep in mind that this will modify response objects in place if
        possible!

        :param response: a response object or wsgi application.
        :param environ: a WSGI environment object.
        :return: a response object.
        """
        if not isinstance(response, BaseResponse):
            if environ is None:
                raise TypeError(
                    "cannot convert WSGI application into response"
                    " objects without an environ"
                )
            response = BaseResponse(*_run_wsgi_app(response, environ))
        response.__class__ = cls
        return response

    @classmethod
    def from_app(cls, app, environ, buffered=False):
        """Create a new response object from an application output.  This
        works best if you pass it an application that returns a generator all
        the time.  Sometimes applications may use the `write()` callable
        returned by the `start_response` function.  This tries to resolve such
        edge cases automatically.  But if you don't get the expected output
        you should set `buffered` to `True` which enforces buffering.

        :param app: the WSGI application to execute.
        :param environ: the WSGI environment to execute against.
        :param buffered: set to `True` to enforce buffering.
        :return: a response object.
        """
        return cls(*_run_wsgi_app(app, environ, buffered))

    def _get_status_code(self):
        return self._status_code

    def _set_status_code(self, code):
        self._status_code = code
        try:
            self._status = "%d %s" % (code, HTTP_STATUS_CODES[code].upper())
        except KeyError:
            self._status = "%d UNKNOWN" % code

    status_code = property(
        _get_status_code, _set_status_code, doc="The HTTP Status code as number"
    )
    del _get_status_code, _set_status_code

    def _get_status(self):
        return self._status

    def _set_status(self, value):
        try:
            self._status = to_native(value)
        except AttributeError:
            raise TypeError("Invalid status argument")

        try:
            self._status_code = int(self._status.split(None, 1)[0])
        except ValueError:
            self._status_code = 0
            self._status = "0 %s" % self._status
        except IndexError:
            raise ValueError("Empty status argument")

    status = property(_get_status, _set_status, doc="The HTTP Status code")
    del _get_status, _set_status

    def get_data(self, as_text=False):
        """The string representation of the request body.  Whenever you call
        this property the request iterable is encoded and flattened.  This
        can lead to unwanted behavior if you stream big data.

        This behavior can be disabled by setting
        :attr:`implicit_sequence_conversion` to `False`.

        If `as_text` is set to `True` the return value will be a decoded
        unicode string.

        .. versionadded:: 0.9
        """
        self._ensure_sequence()
        rv = b"".join(self.iter_encoded())
        if as_text:
            rv = rv.decode(self.charset)
        return rv

    def set_data(self, value):
        """Sets a new string as response.  The value set must either by a
        unicode or bytestring.  If a unicode string is set it's encoded
        automatically to the charset of the response (utf-8 by default).

        .. versionadded:: 0.9
        """
        # if an unicode string is set, it's encoded directly so that we
        # can set the content length
        if isinstance(value, text_type):
            value = value.encode(self.charset)
        else:
            value = bytes(value)
        self.response = [value]
        if self.automatically_set_content_length:
            self.headers["Content-Length"] = str(len(value))

    data = property(
        get_data,
        set_data,
        doc="A descriptor that calls :meth:`get_data` and :meth:`set_data`.",
    )

    def calculate_content_length(self):
        """Returns the content length if available or `None` otherwise."""
        try:
            self._ensure_sequence()
        except RuntimeError:
            return None
        return sum(len(x) for x in self.iter_encoded())

    def _ensure_sequence(self, mutable=False):
        """This method can be called by methods that need a sequence.  If
        `mutable` is true, it will also ensure that the response sequence
        is a standard Python list.

        .. versionadded:: 0.6
        """
        if self.is_sequence:
            # if we need a mutable object, we ensure it's a list.
            if mutable and not isinstance(self.response, list):
                self.response = list(self.response)
            return
        if self.direct_passthrough:
            raise RuntimeError(
                "Attempted implicit sequence conversion but the"
                " response object is in direct passthrough mode."
            )
        if not self.implicit_sequence_conversion:
            raise RuntimeError(
                "The response object required the iterable to be a"
                " sequence, but the implicit conversion was disabled."
                " Call make_sequence() yourself."
            )
        self.make_sequence()

    def make_sequence(self):
        """Converts the response iterator in a list.  By default this happens
        automatically if required.  If `implicit_sequence_conversion` is
        disabled, this method is not automatically called and some properties
        might raise exceptions.  This also encodes all the items.

        .. versionadded:: 0.6
        """
        if not self.is_sequence:
            # if we consume an iterable we have to ensure that the close
            # method of the iterable is called if available when we tear
            # down the response
            close = getattr(self.response, "close", None)
            self.response = list(self.iter_encoded())
            if close is not None:
                self.call_on_close(close)

    def iter_encoded(self):
        """Iter the response encoded with the encoding of the response.
        If the response object is invoked as WSGI application the return
        value of this method is used as application iterator unless
        :attr:`direct_passthrough` was activated.
        """
        if __debug__:
            _warn_if_string(self.response)
        # Encode in a separate function so that self.response is fetched
        # early.  This allows us to wrap the response with the return
        # value from get_app_iter or iter_encoded.
        return _iter_encoded(self.response, self.charset)

    def set_cookie(
        self,
        key,
        value="",
        max_age=None,
        expires=None,
        path="/",
        domain=None,
        secure=False,
        httponly=False,
        samesite=None,
    ):
        """Sets a cookie. The parameters are the same as in the cookie `Morsel`
        object in the Python standard library but it accepts unicode data, too.

        A warning is raised if the size of the cookie header exceeds
        :attr:`max_cookie_size`, but the header will still be set.

        :param key: the key (name) of the cookie to be set.
        :param value: the value of the cookie.
        :param max_age: should be a number of seconds, or `None` (default) if
                        the cookie should last only as long as the client's
                        browser session.
        :param expires: should be a `datetime` object or UNIX timestamp.
        :param path: limits the cookie to a given path, per default it will
                     span the whole domain.
        :param domain: if you want to set a cross-domain cookie.  For example,
                       ``domain=".example.com"`` will set a cookie that is
                       readable by the domain ``www.example.com``,
                       ``foo.example.com`` etc.  Otherwise, a cookie will only
                       be readable by the domain that set it.
        :param secure: If `True`, the cookie will only be available via HTTPS
        :param httponly: disallow JavaScript to access the cookie.  This is an
                         extension to the cookie standard and probably not
                         supported by all browsers.
        :param samesite: Limits the scope of the cookie such that it will only
                         be attached to requests if those requests are
                         "same-site".
        """
        self.headers.add(
            "Set-Cookie",
            dump_cookie(
                key,
                value=value,
                max_age=max_age,
                expires=expires,
                path=path,
                domain=domain,
                secure=secure,
                httponly=httponly,
                charset=self.charset,
                max_size=self.max_cookie_size,
                samesite=samesite,
            ),
        )

    def delete_cookie(self, key, path="/", domain=None):
        """Delete a cookie.  Fails silently if key doesn't exist.

        :param key: the key (name) of the cookie to be deleted.
        :param path: if the cookie that should be deleted was limited to a
                     path, the path has to be defined here.
        :param domain: if the cookie that should be deleted was limited to a
                       domain, that domain has to be defined here.
        """
        self.set_cookie(key, expires=0, max_age=0, path=path, domain=domain)

    @property
    def is_streamed(self):
        """If the response is streamed (the response is not an iterable with
        a length information) this property is `True`.  In this case streamed
        means that there is no information about the number of iterations.
        This is usually `True` if a generator is passed to the response object.

        This is useful for checking before applying some sort of post
        filtering that should not take place for streamed responses.
        """
        try:
            len(self.response)
        except (TypeError, AttributeError):
            return True
        return False

    @property
    def is_sequence(self):
        """If the iterator is buffered, this property will be `True`.  A
        response object will consider an iterator to be buffered if the
        response attribute is a list or tuple.

        .. versionadded:: 0.6
        """
        return isinstance(self.response, (tuple, list))

    def close(self):
        """Close the wrapped response if possible.  You can also use the object
        in a with statement which will automatically close it.

        .. versionadded:: 0.9
           Can now be used in a with statement.
        """
        if hasattr(self.response, "close"):
            self.response.close()
        for func in self._on_close:
            func()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, tb):
        self.close()

    def freeze(self):
        """Call this method if you want to make your response object ready for
        being pickled.  This buffers the generator if there is one.  It will
        also set the `Content-Length` header to the length of the body.

        .. versionchanged:: 0.6
           The `Content-Length` header is now set.
        """
        # we explicitly set the length to a list of the *encoded* response
        # iterator.  Even if the implicit sequence conversion is disabled.
        self.response = list(self.iter_encoded())
        self.headers["Content-Length"] = str(sum(map(len, self.response)))

    def get_wsgi_headers(self, environ):
        """This is automatically called right before the response is started
        and returns headers modified for the given environment.  It returns a
        copy of the headers from the response with some modifications applied
        if necessary.

        For example the location header (if present) is joined with the root
        URL of the environment.  Also the content length is automatically set
        to zero here for certain status codes.

        .. versionchanged:: 0.6
           Previously that function was called `fix_headers` and modified
           the response object in place.  Also since 0.6, IRIs in location
           and content-location headers are handled properly.

           Also starting with 0.6, Werkzeug will attempt to set the content
           length if it is able to figure it out on its own.  This is the
           case if all the strings in the response iterable are already
           encoded and the iterable is buffered.

        :param environ: the WSGI environment of the request.
        :return: returns a new :class:`~werkzeug.datastructures.Headers`
                 object.
        """
        headers = Headers(self.headers)
        location = None
        content_location = None
        content_length = None
        status = self.status_code

        # iterate over the headers to find all values in one go.  Because
        # get_wsgi_headers is used each response that gives us a tiny
        # speedup.
        for key, value in headers:
            ikey = key.lower()
            if ikey == u"location":
                location = value
            elif ikey == u"content-location":
                content_location = value
            elif ikey == u"content-length":
                content_length = value

        # make sure the location header is an absolute URL
        if location is not None:
            old_location = location
            if isinstance(location, text_type):
                # Safe conversion is necessary here as we might redirect
                # to a broken URI scheme (for instance itms-services).
                location = iri_to_uri(location, safe_conversion=True)

            if self.autocorrect_location_header:
                current_url = get_current_url(environ, strip_querystring=True)
                if isinstance(current_url, text_type):
                    current_url = iri_to_uri(current_url)
                location = url_join(current_url, location)
            if location != old_location:
                headers["Location"] = location

        # make sure the content location is a URL
        if content_location is not None and isinstance(content_location, text_type):
            headers["Content-Location"] = iri_to_uri(content_location)

        if 100 <= status < 200 or status == 204:
            # Per section 3.3.2 of RFC 7230, "a server MUST NOT send a
            # Content-Length header field in any response with a status
            # code of 1xx (Informational) or 204 (No Content)."
            headers.remove("Content-Length")
        elif status == 304:
            remove_entity_headers(headers)

        # if we can determine the content length automatically, we
        # should try to do that.  But only if this does not involve
        # flattening the iterator or encoding of unicode strings in
        # the response.  We however should not do that if we have a 304
        # response.
        if (
            self.automatically_set_content_length
            and self.is_sequence
            and content_length is None
            and status not in (204, 304)
            and not (100 <= status < 200)
        ):
            try:
                content_length = sum(len(to_bytes(x, "ascii")) for x in self.response)
            except UnicodeError:
                # aha, something non-bytestringy in there, too bad, we
                # can't safely figure out the length of the response.
                pass
            else:
                headers["Content-Length"] = str(content_length)

        return headers

    def get_app_iter(self, environ):
        """Returns the application iterator for the given environ.  Depending
        on the request method and the current status code the return value
        might be an empty response rather than the one from the response.

        If the request method is `HEAD` or the status code is in a range
        where the HTTP specification requires an empty response, an empty
        iterable is returned.

        .. versionadded:: 0.6

        :param environ: the WSGI environment of the request.
        :return: a response iterable.
        """
        status = self.status_code
        if (
            environ["REQUEST_METHOD"] == "HEAD"
            or 100 <= status < 200
            or status in (204, 304)
        ):
            iterable = ()
        elif self.direct_passthrough:
            if __debug__:
                _warn_if_string(self.response)
            return self.response
        else:
            iterable = self.iter_encoded()
        return ClosingIterator(iterable, self.close)

    def get_wsgi_response(self, environ):
        """Returns the final WSGI response as tuple.  The first item in
        the tuple is the application iterator, the second the status and
        the third the list of headers.  The response returned is created
        specially for the given environment.  For example if the request
        method in the WSGI environment is ``'HEAD'`` the response will
        be empty and only the headers and status code will be present.

        .. versionadded:: 0.6

        :param environ: the WSGI environment of the request.
        :return: an ``(app_iter, status, headers)`` tuple.
        """
        headers = self.get_wsgi_headers(environ)
        app_iter = self.get_app_iter(environ)
        return app_iter, self.status, headers.to_wsgi_list()

    def __call__(self, environ, start_response):
        """Process this response as WSGI application.

        :param environ: the WSGI environment.
        :param start_response: the response callable provided by the WSGI
                               server.
        :return: an application iterator
        """
        app_iter, status, headers = self.get_wsgi_response(environ)
        start_response(status, headers)
        return app_iter
