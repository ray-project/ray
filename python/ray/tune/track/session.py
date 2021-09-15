def TrackSession(*args, **kwargs):
    msg = "tune.track is now deprecated. Use `tune.report` instead."
    raise DeprecationWarning(msg)
