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
        self.added_keys = set()
        self.deleted_keys = set()
        self.intercepted_values = {}
        self.get_interceptor = None

    def set_get_interceptor(self, fn):
        self.get_interceptor = fn

    def copy(self):
        copy = UsageTrackingDict(**dict.copy(self))
        copy.set_get_interceptor(self.get_interceptor)
        return copy

    @property
    def data(self):
        # Make sure, if we use UsageTrackingDict wrapping a SampleBatch,
        # one can still do: sample_batch.data[some_key].
        return self

    def __getitem__(self, key):
        self.accessed_keys.add(key)
        value = dict.__getitem__(self, key)
        if self.get_interceptor:
            if key not in self.intercepted_values:
                self.intercepted_values[key] = self.get_interceptor(value)
            value = self.intercepted_values[key]
        return value

    def __delitem__(self, key):
        self.deleted_keys.add(key)
        dict.__delitem__(self, key)

    def __setitem__(self, key, value):
        if key not in self:
            self.added_keys.add(key)
        dict.__setitem__(self, key, value)
        if key in self.intercepted_values:
            self.intercepted_values[key] = value
