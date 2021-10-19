import errno
import io
import platform
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


def patch_psutil():
    """WSL's /proc/meminfo has an inconsistency where it
    nondeterministically omits a space after colons (after "SwapFree:"
    in my case).
    psutil then splits on spaces and then parses the wrong field,
    crashing on the 'int(fields[1])' expression in
    psutil._pslinux.virtual_memory().
    Workaround: We ensure there is a space following each colon.
    """
    assert (platform.system() == "Linux"
            and "Microsoft".lower() in platform.release().lower())

    try:
        import psutil._pslinux
    except ImportError:
        psutil = None
    psutil_open_binary = None
    if psutil:
        try:
            psutil_open_binary = psutil._pslinux.open_binary
        except AttributeError:
            pass
    # Only patch it if it doesn't seem to have been patched already
    if psutil_open_binary and psutil_open_binary.__name__ == "open_binary":

        def psutil_open_binary_patched(fname, *args, **kwargs):
            f = psutil_open_binary(fname, *args, **kwargs)
            if fname == "/proc/meminfo":
                with f:
                    # Make sure there's a space after colons
                    return io.BytesIO(f.read().replace(b":", b": "))
            return f

        psutil._pslinux.open_binary = psutil_open_binary_patched
