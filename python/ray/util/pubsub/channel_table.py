

class ChannelItem:
    def __init__(self):
        self._channel_name = None
        self._subscribers = []
        self._publishers = []

class ChannelTable:
    def __init__(self):
        """
            channel_name -> ChannelItem
        """
        self._channels = {}
