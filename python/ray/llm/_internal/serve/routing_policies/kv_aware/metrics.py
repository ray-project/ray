import functools
from enum import Enum

from ray.serve import metrics


class TokenizationBypassOutcome(str, Enum):
    SUCCESS = "success"
    SEND_FAILED = "send_failed"
    MISS = "miss"
    INVALID = "invalid"


@functools.cache
def _tokenization_bypass_counter() -> metrics.Counter:
    counter = metrics.Counter(
        "serve_llm_tokenization_bypass_requests",
        description="Requests forwarding prompt tokens to skip engine tokenization.",
        tag_keys=("outcome",),
    )
    # Export every outcome at 0 so rate() counts its first event.
    for outcome in TokenizationBypassOutcome:
        counter._record(0, tags={"outcome": outcome.value})
    return counter


def init_tokenization_bypass_counter() -> None:
    """Call at replica startup, before any request records an outcome."""
    _tokenization_bypass_counter()


def record_tokenization_bypass(outcome: TokenizationBypassOutcome) -> None:
    _tokenization_bypass_counter().inc(tags={"outcome": outcome.value})
