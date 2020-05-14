import errno
import socket
import sys


def patch_redis_empty_recv():
    """On Windows, socket disconnect result in errors rather than empty reads.
    redis-py does not handle these errors correctly.
    This patch translates connection resets to empty reads as in POSIX.
    """
    assert sys.platform == "win32"
    import redis

    def redis_recv(sock, *args, **kwargs):
        result = b""
        try:
            result = redis._compat.recv(sock, *args, **kwargs)
        except socket.error as ex:
            if ex.errno not in [errno.ECONNRESET, errno.ECONNREFUSED]:
                raise
        return result

    redis.connection.recv = redis_recv
