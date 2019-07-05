from datetime import datetime
from datetime import timedelta

from .._compat import string_types
from ..datastructures import CallbackDict
from ..http import dump_age
from ..http import dump_header
from ..http import dump_options_header
from ..http import http_date
from ..http import parse_age
from ..http import parse_date
from ..http import parse_options_header
from ..http import parse_set_header
from ..utils import cached_property
from ..utils import environ_property
from ..utils import get_content_type
from ..utils import header_property
from ..wsgi import get_content_length


class CommonRequestDescriptorsMixin(object):
    """A mixin for :class:`BaseRequest` subclasses.  Request objects that
    mix this class in will automatically get descriptors for a couple of
    HTTP headers with automatic type conversion.

    .. versionadded:: 0.5
    """

    content_type = environ_property(
        "CONTENT_TYPE",
        doc="""The Content-Type entity-header field indicates the media
        type of the entity-body sent to the recipient or, in the case of
        the HEAD method, the media type that would have been sent had
        the request been a GET.""",
    )

    @cached_property
    def content_length(self):
        """The Content-Length entity-header field indicates the size of the
        entity-body in bytes or, in the case of the HEAD method, the size of
        the entity-body that would have been sent had the request been a
        GET.
        """
        return get_content_length(self.environ)

    content_encoding = environ_property(
        "HTTP_CONTENT_ENCODING",
        doc="""The Content-Encoding entity-header field is used as a
        modifier to the media-type. When present, its value indicates
        what additional content codings have been applied to the
        entity-body, and thus what decoding mechanisms must be applied
        in order to obtain the media-type referenced by the Content-Type
        header field.

        .. versionadded:: 0.9""",
    )
    content_md5 = environ_property(
        "HTTP_CONTENT_MD5",
        doc="""The Content-MD5 entity-header field, as defined in
        RFC 1864, is an MD5 digest of the entity-body for the purpose of
        providing an end-to-end message integrity check (MIC) of the
        entity-body. (Note: a MIC is good for detecting accidental
        modification of the entity-body in transit, but is not proof
        against malicious attacks.)

        .. versionadded:: 0.9""",
    )
    referrer = environ_property(
        "HTTP_REFERER",
        doc="""The Referer[sic] request-header field allows the client
        to specify, for the server's benefit, the address (URI) of the
        resource from which the Request-URI was obtained (the
        "referrer", although the header field is misspelled).""",
    )
    date = environ_property(
        "HTTP_DATE",
        None,
        parse_date,
        doc="""The Date general-header field represents the date and
        time at which the message was originated, having the same
        semantics as orig-date in RFC 822.""",
    )
    max_forwards = environ_property(
        "HTTP_MAX_FORWARDS",
        None,
        int,
        doc="""The Max-Forwards request-header field provides a
        mechanism with the TRACE and OPTIONS methods to limit the number
        of proxies or gateways that can forward the request to the next
        inbound server.""",
    )

    def _parse_content_type(self):
        if not hasattr(self, "_parsed_content_type"):
            self._parsed_content_type = parse_options_header(
                self.environ.get("CONTENT_TYPE", "")
            )

    @property
    def mimetype(self):
        """Like :attr:`content_type`, but without parameters (eg, without
        charset, type etc.) and always lowercase.  For example if the content
        type is ``text/HTML; charset=utf-8`` the mimetype would be
        ``'text/html'``.
        """
        self._parse_content_type()
        return self._parsed_content_type[0].lower()

    @property
    def mimetype_params(self):
        """The mimetype parameters as dict.  For example if the content
        type is ``text/html; charset=utf-8`` the params would be
        ``{'charset': 'utf-8'}``.
        """
        self._parse_content_type()
        return self._parsed_content_type[1]

    @cached_property
    def pragma(self):
        """The Pragma general-header field is used to include
        implementation-specific directives that might apply to any recipient
        along the request/response chain.  All pragma directives specify
        optional behavior from the viewpoint of the protocol; however, some
        systems MAY require that behavior be consistent with the directives.
        """
        return parse_set_header(self.environ.get("HTTP_PRAGMA", ""))


class CommonResponseDescriptorsMixin(object):
    """A mixin for :class:`BaseResponse` subclasses.  Response objects that
    mix this class in will automatically get descriptors for a couple of
    HTTP headers with automatic type conversion.
    """

    @property
    def mimetype(self):
        """The mimetype (content type without charset etc.)"""
        ct = self.headers.get("content-type")
        if ct:
            return ct.split(";")[0].strip()

    @mimetype.setter
    def mimetype(self, value):
        self.headers["Content-Type"] = get_content_type(value, self.charset)

    @property
    def mimetype_params(self):
        """The mimetype parameters as dict. For example if the
        content type is ``text/html; charset=utf-8`` the params would be
        ``{'charset': 'utf-8'}``.

        .. versionadded:: 0.5
        """

        def on_update(d):
            self.headers["Content-Type"] = dump_options_header(self.mimetype, d)

        d = parse_options_header(self.headers.get("content-type", ""))[1]
        return CallbackDict(d, on_update)

    location = header_property(
        "Location",
        doc="""The Location response-header field is used to redirect
        the recipient to a location other than the Request-URI for
        completion of the request or identification of a new
        resource.""",
    )
    age = header_property(
        "Age",
        None,
        parse_age,
        dump_age,
        doc="""The Age response-header field conveys the sender's
        estimate of the amount of time since the response (or its
        revalidation) was generated at the origin server.

        Age values are non-negative decimal integers, representing time
        in seconds.""",
    )
    content_type = header_property(
        "Content-Type",
        doc="""The Content-Type entity-header field indicates the media
        type of the entity-body sent to the recipient or, in the case of
        the HEAD method, the media type that would have been sent had
        the request been a GET.""",
    )
    content_length = header_property(
        "Content-Length",
        None,
        int,
        str,
        doc="""The Content-Length entity-header field indicates the size
        of the entity-body, in decimal number of OCTETs, sent to the
        recipient or, in the case of the HEAD method, the size of the
        entity-body that would have been sent had the request been a
        GET.""",
    )
    content_location = header_property(
        "Content-Location",
        doc="""The Content-Location entity-header field MAY be used to
        supply the resource location for the entity enclosed in the
        message when that entity is accessible from a location separate
        from the requested resource's URI.""",
    )
    content_encoding = header_property(
        "Content-Encoding",
        doc="""The Content-Encoding entity-header field is used as a
        modifier to the media-type. When present, its value indicates
        what additional content codings have been applied to the
        entity-body, and thus what decoding mechanisms must be applied
        in order to obtain the media-type referenced by the Content-Type
        header field.""",
    )
    content_md5 = header_property(
        "Content-MD5",
        doc="""The Content-MD5 entity-header field, as defined in
        RFC 1864, is an MD5 digest of the entity-body for the purpose of
        providing an end-to-end message integrity check (MIC) of the
        entity-body. (Note: a MIC is good for detecting accidental
        modification of the entity-body in transit, but is not proof
        against malicious attacks.)""",
    )
    date = header_property(
        "Date",
        None,
        parse_date,
        http_date,
        doc="""The Date general-header field represents the date and
        time at which the message was originated, having the same
        semantics as orig-date in RFC 822.""",
    )
    expires = header_property(
        "Expires",
        None,
        parse_date,
        http_date,
        doc="""The Expires entity-header field gives the date/time after
        which the response is considered stale. A stale cache entry may
        not normally be returned by a cache.""",
    )
    last_modified = header_property(
        "Last-Modified",
        None,
        parse_date,
        http_date,
        doc="""The Last-Modified entity-header field indicates the date
        and time at which the origin server believes the variant was
        last modified.""",
    )

    @property
    def retry_after(self):
        """The Retry-After response-header field can be used with a
        503 (Service Unavailable) response to indicate how long the
        service is expected to be unavailable to the requesting client.

        Time in seconds until expiration or date.
        """
        value = self.headers.get("retry-after")
        if value is None:
            return
        elif value.isdigit():
            return datetime.utcnow() + timedelta(seconds=int(value))
        return parse_date(value)

    @retry_after.setter
    def retry_after(self, value):
        if value is None:
            if "retry-after" in self.headers:
                del self.headers["retry-after"]
            return
        elif isinstance(value, datetime):
            value = http_date(value)
        else:
            value = str(value)
        self.headers["Retry-After"] = value

    def _set_property(name, doc=None):  # noqa: B902
        def fget(self):
            def on_update(header_set):
                if not header_set and name in self.headers:
                    del self.headers[name]
                elif header_set:
                    self.headers[name] = header_set.to_header()

            return parse_set_header(self.headers.get(name), on_update)

        def fset(self, value):
            if not value:
                del self.headers[name]
            elif isinstance(value, string_types):
                self.headers[name] = value
            else:
                self.headers[name] = dump_header(value)

        return property(fget, fset, doc=doc)

    vary = _set_property(
        "Vary",
        doc="""The Vary field value indicates the set of request-header
        fields that fully determines, while the response is fresh,
        whether a cache is permitted to use the response to reply to a
        subsequent request without revalidation.""",
    )
    content_language = _set_property(
        "Content-Language",
        doc="""The Content-Language entity-header field describes the
        natural language(s) of the intended audience for the enclosed
        entity. Note that this might not be equivalent to all the
        languages used within the entity-body.""",
    )
    allow = _set_property(
        "Allow",
        doc="""The Allow entity-header field lists the set of methods
        supported by the resource identified by the Request-URI. The
        purpose of this field is strictly to inform the recipient of
        valid methods associated with the resource. An Allow header
        field MUST be present in a 405 (Method Not Allowed)
        response.""",
    )

    del _set_property
