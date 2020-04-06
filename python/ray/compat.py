import errno
import socket
import sys


def patch_redis():
    if sys.platform == "win32":
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


def apply_patches():
    patch_redis()
