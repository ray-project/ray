from ..datastructures import CharsetAccept
from ..datastructures import LanguageAccept
from ..datastructures import MIMEAccept
from ..http import parse_accept_header
from ..utils import cached_property


class AcceptMixin(object):
    """A mixin for classes with an :attr:`~BaseResponse.environ` attribute
    to get all the HTTP accept headers as
    :class:`~werkzeug.datastructures.Accept` objects (or subclasses
    thereof).
    """

    @cached_property
    def accept_mimetypes(self):
        """List of mimetypes this client supports as
        :class:`~werkzeug.datastructures.MIMEAccept` object.
        """
        return parse_accept_header(self.environ.get("HTTP_ACCEPT"), MIMEAccept)

    @cached_property
    def accept_charsets(self):
        """List of charsets this client supports as
        :class:`~werkzeug.datastructures.CharsetAccept` object.
        """
        return parse_accept_header(
            self.environ.get("HTTP_ACCEPT_CHARSET"), CharsetAccept
        )

    @cached_property
    def accept_encodings(self):
        """List of encodings this client accepts.  Encodings in a HTTP term
        are compression encodings such as gzip.  For charsets have a look at
        :attr:`accept_charset`.
        """
        return parse_accept_header(self.environ.get("HTTP_ACCEPT_ENCODING"))

    @cached_property
    def accept_languages(self):
        """List of languages this client accepts as
        :class:`~werkzeug.datastructures.LanguageAccept` object.

        .. versionchanged 0.5
           In previous versions this was a regular
           :class:`~werkzeug.datastructures.Accept` object.
        """
        return parse_accept_header(
            self.environ.get("HTTP_ACCEPT_LANGUAGE"), LanguageAccept
        )
