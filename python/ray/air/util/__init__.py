# Lazy import to avoid ray init failures without pandas installed and allow
# dataset to import modules in this file.
_pandas = None


def _lazy_import_pandas():
    global _pandas
    if _pandas is None:
        import pandas

        _pandas = pandas
    return _pandas


def _lazy_import_pandas_no_raise():
    try:
        return _lazy_import_pandas()
    except BaseException:
        return None
