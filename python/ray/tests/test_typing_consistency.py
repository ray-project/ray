import inspect
import typing

from ray._private import worker


def test_typing_options_consistency():
    initial_hints = typing.get_type_hints(worker.RemoteFunctionNoArgs.options)
    initial_signature = inspect.signature(worker.RemoteFunctionNoArgs.options)
    for options_func in [
        worker.RemoteFunction0.options,
        worker.RemoteFunction1.options,
        worker.RemoteFunction2.options,
        worker.RemoteFunction3.options,
        worker.RemoteFunction4.options,
        worker.RemoteFunction5.options,
        worker.RemoteFunction6.options,
        worker.RemoteFunction7.options,
        worker.RemoteFunction8.options,
        worker.RemoteFunction9.options,
    ]:
        current_hints = typing.get_type_hints(options_func)
        current_signature = inspect.signature(options_func)
        assert current_hints == initial_hints
        assert current_signature == initial_signature
