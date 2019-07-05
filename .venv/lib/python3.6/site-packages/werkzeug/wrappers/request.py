from .accept import AcceptMixin
from .auth import AuthorizationMixin
from .base_request import BaseRequest
from .common_descriptors import CommonRequestDescriptorsMixin
from .etag import ETagRequestMixin
from .user_agent import UserAgentMixin


class Request(
    BaseRequest,
    AcceptMixin,
    ETagRequestMixin,
    UserAgentMixin,
    AuthorizationMixin,
    CommonRequestDescriptorsMixin,
):
    """Full featured request object implementing the following mixins:

    - :class:`AcceptMixin` for accept header parsing
    - :class:`ETagRequestMixin` for etag and cache control handling
    - :class:`UserAgentMixin` for user agent introspection
    - :class:`AuthorizationMixin` for http auth handling
    - :class:`CommonRequestDescriptorsMixin` for common headers
    """


class StreamOnlyMixin(object):
    """If mixed in before the request object this will change the bahavior
    of it to disable handling of form parsing.  This disables the
    :attr:`files`, :attr:`form` attributes and will just provide a
    :attr:`stream` attribute that however is always available.

    .. versionadded:: 0.9
    """

    disable_data_descriptor = True
    want_form_data_parsed = False


class PlainRequest(StreamOnlyMixin, Request):
    """A request object without special form parsing capabilities.

    .. versionadded:: 0.9
    """
