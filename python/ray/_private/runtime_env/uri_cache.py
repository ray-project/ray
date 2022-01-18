import logging
from typing import Dict, Callable

default_logger = logging.getLogger(__name__)

DEFAULT_MAX_URI_CACHE_SIZE_BYTES = (1024**3) * 10  # 10 GB

class URICache:
    def __init__(
            self,
            delete_fn: Callable[[str, logging.Logger], int] = lambda x, y: 0,
            max_total_size_bytes: int = DEFAULT_MAX_URI_CACHE_SIZE_BYTES,
    ):
        # Maps URIs to the size in bytes of their corresponding disk contents.
        self._used_uris: Dict[str, int] = dict()
        self._unused_uris: Dict[str, int] = dict()
        self._delete_fn = delete_fn
        self.max_total_size_bytes = max_total_size_bytes

    def mark_unused(self, uri: str, logger: logging.Logger = default_logger):
        """Mark a URI as unused and okay to be deleted."""
        if uri not in self._used_uris:
            logger.debug(f"URI {uri} is already unused.")
        else:
            self._unused_uris[uri] = self._used_uris[uri]
            del self._used_uris[uri]
        logger.debug(f"Marked uri {uri} unused.")
        self._evict_if_needed(logger)
        self._check_valid()

    def mark_used(self, uri: str, logger: logging.Logger = default_logger):
        """Mark a URI as in use.  URIs in use will not be deleted."""
        if uri in self._used_uris:
            return
        elif uri in self._unused_uris:
            self._used_uris[uri] = self._unused_uris[uri]
            del self._unused_uris[uri]
        else:
            raise ValueError(f"Got request to mark URI {uri} unused, but this "
                             "URI is not present in the cache.")
        logger.debug(f"Marked URI {uri} used.")
        self._check_valid()

    def add(self,
            uri: str,
            size_bytes: int,
            logger: logging.Logger = default_logger):
        if uri in self._unused_uris:
            if size_bytes != self._unused_uris[uri]:
                logger.debug(f"Added URI {uri} with size {size_bytes}, which "
                             "doesn't match the existing size "
                             f"{self._unused_uris[uri]}.")
            del self._unused_uris[uri]
        self._used_uris[uri] = size_bytes
        self._evict_if_needed(logger)
        self._check_valid()
        logger.debug(f"Added URI {uri} with size {size_bytes}")

    def get_total_size_bytes(self) -> int:
        return sum(self._used_uris.values()) + sum(self._unused_uris.values())

    def _evict_if_needed(self, logger: logging.Logger = default_logger):
        """Evict unused URIs (if they exist) until total size <= max size."""
        while (self._unused_uris
               and self.get_total_size_bytes() > self.max_total_size_bytes):
            # TODO(architkulkarni): Evict least recently used URI instead
            arbitrary_unused_uri = next(iter(self._unused_uris))
            del self._unused_uris[arbitrary_unused_uri]
            self._delete_fn(arbitrary_unused_uri, logger)

    def _check_valid(self):
        assert self._used_uris.keys() & self._unused_uris.keys() == set()

    def __contains__(self, uri):
        return uri in self._used_uris or uri in self._unused_uris

    def __repr__(self):
        return str(self.__dict__)