
class B:
    def __init__(self):
        self.base = " B "

    def __call__(self, req):
        return self.base