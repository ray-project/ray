def mixin(*args):
    def mix_trainable(func):
        func.__mixins__ = args
        return func

    return mix_trainable


class TrainableMixin:
    pass
