"""
werkzeug.wrappers
~~~~~~~~~~~~~~~~~

The wrappers are simple request and response objects which you can
subclass to do whatever you want them to do.  The request object contains
the information transmitted by the client (webbrowser) and the response
object contains all the information sent back to the browser.

An important detail is that the request object is created with the WSGI
environ and will act as high-level proxy whereas the response object is an
actual WSGI application.

Like everything else in Werkzeug these objects will work correctly with
unicode data.  Incoming form data parsed by the response object will be
decoded into an unicode object if possible and if it makes sense.

:copyright: 2007 Pallets
:license: BSD-3-Clause
"""
from .accept import AcceptMixin
from .auth import AuthorizationMixin
from .auth import WWWAuthenticateMixin
from .base_request import BaseRequest
from .base_response import BaseResponse
from .common_descriptors import CommonRequestDescriptorsMixin
from .common_descriptors import CommonResponseDescriptorsMixin
from .etag import ETagRequestMixin
from .etag import ETagResponseMixin
from .request import PlainRequest
from .request import Request
from .request import StreamOnlyMixin
from .response import Response
from .response import ResponseStream
from .response import ResponseStreamMixin
from .user_agent import UserAgentMixin
