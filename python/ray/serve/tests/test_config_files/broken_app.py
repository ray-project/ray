from ray import serve
import ray.cloudpickle as pickle


class NonserializableException(Exception):
    """This exception cannot be serialized."""

    def __reduce__(self):
        raise RuntimeError("This exception cannot be serialized!")


# Confirm that NonserializableException cannot be serialized.
try:
    pickle.dumps(NonserializableException())
except RuntimeError as e:
    assert "This exception cannot be serialized!" in repr(e)

raise NonserializableException("custom exception info")


@serve.deployment
def f():
    pass


app = f.bind()
