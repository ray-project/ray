import atexit
import threading
import weakref

class Closer(object):
    """A registry that ensures your objects get closed, whether manually,
    upon garbage collection, or upon exit. To work properly, your
    objects need to cooperate and do something like the following:

    ```
    closer = Closer()
    class Example(object):
        def __init__(self):
            self._id = closer.register(self)

        def close(self):
            # Probably worth making idempotent too!
            ...
            closer.unregister(self._id)

        def __del__(self):
            self.close()
    ```

    That is, your objects should:

    - register() themselves and save the returned ID
    - unregister() themselves upon close()
    - include a __del__ method which close()'s the object
    """

    def __init__(self, atexit_register=True):
        self.lock = threading.Lock()
        self.next_id = -1
        self.closeables = weakref.WeakValueDictionary()

        if atexit_register:
            atexit.register(self.close)

    def generate_next_id(self):
        with self.lock:
            self.next_id += 1
            return self.next_id

    def register(self, closeable):
        """Registers an object with a 'close' method.

        Returns:
            int: The registration ID of this object. It is the caller's responsibility to save this ID if early closing is desired.
        """
        assert hasattr(closeable, 'close'), 'No close method for {}'.format(closeable)

        next_id = self.generate_next_id()
        self.closeables[next_id] = closeable
        return next_id

    def unregister(self, id):
        assert id is not None
        if id in self.closeables:
            del self.closeables[id]

    def close(self):
        # Explicitly fetch all monitors first so that they can't disappear while
        # we iterate. cf. http://stackoverflow.com/a/12429620
        closeables = list(self.closeables.values())
        for closeable in closeables:
            closeable.close()
