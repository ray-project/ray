class Record:
    """Data record in data stream"""

    def __init__(self, value):
        self.value = value
        self.stream = None

    def __repr__(self):
        return "Record(%s)".format(self.value)


class KeyRecord(Record):
    """Data record in a keyed data stream"""

    def __init__(self, key, value):
        super().__init__(value)
        self.key = key
