import sys


def print_debug(*args, **kwargs):
    # debug inside an actor
    print("\u001b[32m ray.serve DEBUG: \u001b[0m", end=" ")
    print(*args, **kwargs)
    sys.stdout.flush()
