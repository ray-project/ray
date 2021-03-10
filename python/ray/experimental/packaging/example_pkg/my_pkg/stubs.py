# NOTE: YOU ARE NOT ALLOWED TO IMPORT my_pkg at toplevel here, since this
# file must be importable by the driver program, which has its own runtime
# environment separate from that of this package.

# !!!
# Stub files can only import ray at top-level.
# !!!
import ray


# This actor will be instantiated within the package's defined ``runtime_env``.
@ray.remote
class MyActor:
    def __init__(self):
        from my_pkg import impl  # Lazy import.
        self.impl = impl

    def f(self):
        return self.impl.hello()


# This actor will be executed within the package's defined ``runtime_env``.
@ray.remote
def my_func():
    from my_pkg import impl  # Lazy import.
    return impl.hello()
