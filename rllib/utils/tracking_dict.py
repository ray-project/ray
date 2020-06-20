class UsageTrackingDict(dict):
    """Dict that tracks which keys have been accessed.

    It can also intercept gets and allow an arbitrary callback to be applied
    (i.e., to lazily convert numpy arrays to Tensors).

    We make the simplifying assumption only __getitem__ is used to access
    values.
    """

    def __init__(self, *args, **kwargs):
        dict.__init__(self, *args, **kwargs)
        self.accessed_keys = set()
        self.intercepted_values = {}
        self.get_interceptor = None

    def set_get_interceptor(self, fn):
        self.get_interceptor = fn

    def __getitem__(self, key):
        self.accessed_keys.add(key)
        value = dict.__getitem__(self, key)
        if self.get_interceptor:
            if key not in self.intercepted_values:
                self.intercepted_values[key] = self.get_interceptor(value)
            value = self.intercepted_values[key]
        return value

    def __setitem__(self, key, value):
        dict.__setitem__(self, key, value)
        if key in self.intercepted_values:
            self.intercepted_values[key] = value
