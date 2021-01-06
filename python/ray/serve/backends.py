from ray.serve.utils import import_class


class ImportedBackend:
    """Factory for a class that will dynamically import a backend class.

    This is intended to be used when the source code for a backend is
    installed in the worker environment but not the driver.

    Intended usage:
        >>> client = serve.connect()
        >>> client.create_backend("b", ImportedBackend("module.Class"), *args)

    This will import module.Class on the worker and proxy all relevant methods
    to it.
    """

    def __new__(cls, class_path):
        class ImportedBackend:
            def __init__(self, *args, **kwargs):
                self.wrapped = import_class(class_path)(*args, **kwargs)

            def reconfigure(self, *args, **kwargs):
                # NOTE(edoakes): we check that the reconfigure method is
                # present if the user specifies a user_config, so we need to
                # proxy it manually.
                return self.wrapped.reconfigure(*args, **kwargs)

            def __getattr__(self, attr):
                """Proxy all other methods to the wrapper class."""
                return getattr(self.wrapped, attr)

        return ImportedBackend
