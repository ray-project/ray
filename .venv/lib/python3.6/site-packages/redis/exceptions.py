"Core exceptions raised by the Redis client"


class RedisError(Exception):
    pass


class AuthenticationError(RedisError):
    pass


class ConnectionError(RedisError):
    pass


class TimeoutError(RedisError):
    pass


class BusyLoadingError(ConnectionError):
    pass


class InvalidResponse(RedisError):
    pass


class ResponseError(RedisError):
    pass


class DataError(RedisError):
    pass


class PubSubError(RedisError):
    pass


class WatchError(RedisError):
    pass


class NoScriptError(ResponseError):
    pass


class ExecAbortError(ResponseError):
    pass


class ReadOnlyError(ResponseError):
    pass


class LockError(RedisError, ValueError):
    "Errors acquiring or releasing a lock"
    # NOTE: For backwards compatability, this class derives from ValueError.
    # This was originally chosen to behave like threading.Lock.
    pass


class LockNotOwnedError(LockError):
    "Error trying to extend or release a lock that is (no longer) owned"
    pass
