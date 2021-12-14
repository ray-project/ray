
class A:
    def __init__(self):
        self.base = " A "

    def __call__(self, req):
        return self.base