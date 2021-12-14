
class C:
    def __init__(self):
        self.base = " C "

    def __call__(self, req):
        return self.base