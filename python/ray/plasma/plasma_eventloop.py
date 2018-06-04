import asyncio
from asyncio import coroutine


class PlasmaSelector(asyncio.selectors.BaseSelector):
    pass


class PlasmaAction:
    def put(self):
        pass


class PlasmaSelectorEventLoop(asyncio.BaseEventLoop):
    
    def __cinit__(self, selector=None):
        super().__init__()
        self._selector = PlasmaSelector() if selector is None else selector
    
    def _process_events(self, event_list):
        # _run_once :: event_list = self._selector.select(timeout)
        
        """Process selector events."""
        raise NotImplementedError
    
    def close(self):
        super().close()
        # TODO: disconnect???
    
    def get(self, object_ids, int timeout_ms=-1, serialization_context=None):
        pass
    
    def put(self, ):
        fut = asyncio.futures.Future(loop=self)
        # if fut.cancelled():
        #     return
        try:
            data = sock.recv(n)
        except (BlockingIOError, InterruptedError):
            self._check_closed()
            handle = asyncio.events.Handle(callback, args, self)
            try:
                key = self._selector.get_key(fd)
            except KeyError:
                self._selector.register(fd, selectors.EVENT_READ, (handle, None))
            else:
                mask, (reader, writer) = key.events, key.data
                self._selector.modify(fd, mask | selectors.EVENT_READ, (handle, writer))
                if reader is not None:
                    reader.cancel()
        
        except Exception as exc:
            fut.set_exception(exc)
        else:
            fut.set_result(data)
        
        return fut
