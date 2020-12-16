from ray.serve.utils import import_class


class ImportedBackend:
    def __new__(cls, class_path):
        class ImportedBackend:
            """XXX: comments"""

            def __init__(self, *args, **kwargs):
                self.wrapped = import_class(class_path)(*args, **kwargs)

            def __getattr__(self, attr):
                """XXX: needed for other call methods"""
                return getattr(self.wrapped, attr)

            def reconfigure(self, *args, **kwargs):
                """XXX: we check manually for this method"""
                return self.wrapped.reconfigure(*args, **kwargs)

        return ImportedBackend
