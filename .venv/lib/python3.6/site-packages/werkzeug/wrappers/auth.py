from ..http import parse_authorization_header
from ..http import parse_www_authenticate_header
from ..utils import cached_property


class AuthorizationMixin(object):
    """Adds an :attr:`authorization` property that represents the parsed
    value of the `Authorization` header as
    :class:`~werkzeug.datastructures.Authorization` object.
    """

    @cached_property
    def authorization(self):
        """The `Authorization` object in parsed form."""
        header = self.environ.get("HTTP_AUTHORIZATION")
        return parse_authorization_header(header)


class WWWAuthenticateMixin(object):
    """Adds a :attr:`www_authenticate` property to a response object."""

    @property
    def www_authenticate(self):
        """The `WWW-Authenticate` header in a parsed form."""

        def on_update(www_auth):
            if not www_auth and "www-authenticate" in self.headers:
                del self.headers["www-authenticate"]
            elif www_auth:
                self.headers["WWW-Authenticate"] = www_auth.to_header()

        header = self.headers.get("www-authenticate")
        return parse_www_authenticate_header(header, on_update)
