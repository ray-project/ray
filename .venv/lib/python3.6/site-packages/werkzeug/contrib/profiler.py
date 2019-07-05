import warnings

from ..middleware.profiler import *  # noqa: F401, F403

warnings.warn(
    "'werkzeug.contrib.profiler' has moved to"
    "'werkzeug.middleware.profiler'. This import is deprecated as of"
    "version 0.15 and will be removed in version 1.0.",
    DeprecationWarning,
    stacklevel=2,
)


class MergeStream(object):
    """An object that redirects ``write`` calls to multiple streams.
    Use this to log to both ``sys.stdout`` and a file::

        f = open('profiler.log', 'w')
        stream = MergeStream(sys.stdout, f)
        profiler = ProfilerMiddleware(app, stream)

    .. deprecated:: 0.15
        Use the ``tee`` command in your terminal instead. This class
        will be removed in 1.0.
    """

    def __init__(self, *streams):
        warnings.warn(
            "'MergeStream' is deprecated as of version 0.15 and will be removed in"
            " version 1.0. Use your terminal's 'tee' command instead.",
            DeprecationWarning,
            stacklevel=2,
        )

        if not streams:
            raise TypeError("At least one stream must be given.")

        self.streams = streams

    def write(self, data):
        for stream in self.streams:
            stream.write(data)
