from collections.abc import Mapping


def row_str(row: Mapping) -> str:
    """Convert a row to string representation."""
    return str(row.as_pydict())


def row_repr(row: Mapping) -> str:
    """Convert a row to repr representation."""
    return str(row)


def row_repr_pretty(row: Mapping, p, cycle):
    """Pretty print a row."""
    from IPython.lib.pretty import _dict_pprinter_factory

    pprinter = _dict_pprinter_factory("{", "}")
    return pprinter(row, p, cycle)
