import sys
import pytest
from retry import retry


retry_once_counter = 0


@retry((Exception), delay=0, max_delay=20, backoff=2, jitter=2, tries=2)
def retry_once():
    global retry_once_counter
    retry_once_counter += 1
    raise Exception()


no_retry_counter = 0


@retry((Exception), delay=0, max_delay=20, backoff=2, jitter=2, tries=1)
def no_retry():
    global no_retry_counter
    no_retry_counter += 1
    raise ValueError()


def test_retry():
    with pytest.raises(Exception):
        retry_once()
    assert retry_once_counter == 2


def test_no_retry():
    with pytest.raises(ValueError):
        no_retry()
    assert no_retry_counter == 1


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
