"""A set of common utilities used within the environments. These are
not intended as API functions, and will not remain stable over time.
"""

color2num = dict(
    gray=30,
    red=31,
    green=32,
    yellow=33,
    blue=34,
    magenta=35,
    cyan=36,
    white=37,
    crimson=38
)


def colorize(string, color, bold=False, highlight = False):
    """Return string surrounded by appropriate terminal color codes to
    print colorized text.  Valid colors: gray, red, green, yellow,
    blue, magenta, cyan, white, crimson
    """

    # Import six here so that `utils` has no import-time dependencies.
    # We want this since we use `utils` during our import-time sanity checks
    # that verify that our dependencies (including six) are actually present.
    import six

    attr = []
    num = color2num[color]
    if highlight: num += 10
    attr.append(six.u(str(num)))
    if bold: attr.append(six.u('1'))
    attrs = six.u(';').join(attr)
    return six.u('\x1b[%sm%s\x1b[0m') % (attrs, string)
