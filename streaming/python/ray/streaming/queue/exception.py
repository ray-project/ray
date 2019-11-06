class QueueInitException(Exception):
    def __init__(self, msg, abnormal_queues):
        self.abnormal_queues = abnormal_queues
        self.msg = msg


class QueueInterruptException(Exception):
    def __init__(self, msg=None):
        self.msg = msg
