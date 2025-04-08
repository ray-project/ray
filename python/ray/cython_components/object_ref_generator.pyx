class DynamicObjectRefGenerator:
    def __init__(self, refs):
        # TODO(swang): As an optimization, can also store the generator
        # ObjectID so that we don't need to keep individual ref counts for the
        # inner ObjectRefs.
        self._refs = refs

    def __iter__(self):
        for ref in self._refs:
            yield ref

    def __len__(self):
        return len(self._refs)
