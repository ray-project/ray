"""Numeric coercion helpers shared across DataSourceV2 sizing code.

File sizes and in-memory size estimates flow through the listing manifest,
the in-memory size estimator, and the file-affinity partitioner. Those
values can be ``None`` (e.g. ``HTTPFileSystem`` doesn't report sizes) or
surface as ``NaN`` (a nullable size column), and both would make a plain
``int(...)`` / ``float(...)`` raise -- and ``NaN or 0`` stays ``NaN`` since
``NaN`` is truthy. Coerce them to a finite ``0`` here so the whole pipeline
shares one guard rather than each site reinventing it.
"""


def finite_int(value) -> int:
    """Coerce a file size to an int, mapping ``None`` and ``NaN`` to ``0``.

    ``int(None)`` raises ``TypeError`` and ``int(NaN)`` raises ``ValueError``,
    so guard both here.
    """
    if value is None or value != value:  # ``value != value`` is True only for NaN
        return 0
    return int(value)


def finite_float(value) -> float:
    """Coerce a size estimate to a float, mapping ``None``/``NaN`` to ``0.0``.

    Estimates are floats (e.g. on-disk size * encoding ratio); keep the
    fractional precision when accumulating them (e.g. into
    ``_WeightedBucket.weight``) rather than truncating each value to an int,
    which would make a running weight drift below the true total.
    """
    if value is None or value != value:  # ``value != value`` is True only for NaN
        return 0.0
    return float(value)
