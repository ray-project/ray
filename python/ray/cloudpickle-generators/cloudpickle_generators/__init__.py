import pickle
from itertools import chain
from types import FunctionType, GeneratorType, CoroutineType, AsyncGeneratorType

from ._core import private_frame_data, restore_frame, unset_value


def _empty_cell():
    """Create an empty cell.
    """
    if False:
        free = None

    return (lambda: free).__closure__[0]


def _make_cell(f_locals, var):
    """Create a PyCell object around a value.
    """
    value = f_locals.get(var, unset_value)
    if value is unset_value:
        # unset the name ``value`` to return an empty cell
        del value

    return (lambda: value).__closure__[0]


def _fill_generator(gen, lasti, f_locals, frame_data):
    _fill_generator_impl(gen.gi_frame, lasti, f_locals, frame_data)
    return gen


def _fill_coroutine(coro, lasti, f_locals, frame_data):
    _fill_generator_impl(coro.cr_frame, lasti, f_locals, frame_data)
    return coro


def _fill_async_generator(asyncgen, lasti, f_locals, frame_data):
    _fill_generator_impl(asyncgen.ag_frame, lasti, f_locals, frame_data)
    return asyncgen


def _fill_generator_impl(frame, lasti, f_locals, frame_data):
    """Reconstruct a generator instance.

    Parameters
    ----------
    frame : frame
        The frame of the skeleton generator, coroutine or async generator.
    lasti : int
        The last instruction executed in the generator. -1 indicates that the
        generator hasn't been started.
    f_locals : dict
        The values of the locals at current execution step.
    frame_data : tuple
        The extra frame data extracted from ``private_frame_data``.


    Returns
    -------
    gen : generator
        The filled generator instance.
    """
    code = frame.f_code
    locals_ = [f_locals.get(var, unset_value) for var in code.co_varnames]
    locals_.extend(
        _make_cell(f_locals, var)
        for var in chain(code.co_cellvars, code.co_freevars))
    restore_frame(frame, lasti, locals_, *frame_data)


def _create_skeleton_generator(gen_func):
    """Create an instance of a generator from a generator function without
    the proper stack, locals, or closure.

    Parameters
    ----------
    gen_func : function
        The function to call to create the instance.

    Returns
    -------
    skeleton_generator : generator
        The uninitialized generator instance.
    """
    code = gen_func.__code__
    kwonly_names = code.co_varnames[code.co_argcount:code.co_kwonlyargcount]
    gen = gen_func(*(None for _ in range(code.co_argcount)),
                   **{key: None
                      for key in kwonly_names})

    try:
        # manually update the qualname to fix a bug in Python 3.6 where the
        # qualname is not correct when using both *args and **kwargs
        gen.__qualname__ = gen_func.__qualname__
    except AttributeError:
        # there is no __qualname__ on generators in Python < 3.5
        pass

    return gen


def _restore_spent_generator(name, qualname):
    """Reconstruct a fully consumed generator.

    Parameters
    ----------
    name : str
        The name of the fully consumed generator.
    name : str
        The qualname of the fully consumed generator.

    Returns
    -------
    gen : generator
        A generator which has been fully consumed.
    """
    def single_generator():
        # we actually need to run the gen to ensure that gi_frame gets
        # deallocated to further match the behavior of the existing generator;
        # this is why we do not just do: ``if False: yield``
        yield

    single_generator.__name__ = name
    try:
        single_generator.__qualname__ = qualname
    except AttributeError:
        # there is no __qualname__ on generators in Python < 3.5
        pass

    gen = single_generator()
    next(gen)
    return gen


def _save_generator(self, gen):
    if gen.gi_running:
        raise ValueError('cannot save running generator')

    frame = gen.gi_frame
    _save_generator_impl(self, frame, gen, _fill_generator)


def _save_coroutine(self, coro):
    frame = coro.cr_frame
    _save_generator_impl(self, frame, coro, _fill_coroutine)


def _save_async_generator(self, asyncgen):
    frame = asyncgen.ag_frame
    _save_generator_impl(self, frame, asyncgen, _fill_async_generator)


def _save_generator_impl(self, frame, gen, filler):
    if frame is None:
        # frame is None when the generator is fully consumed; take a fast path
        self.save_reduce(
            _restore_spent_generator,
            (gen.__name__, getattr(gen, '__qualname__', None)),
            obj=gen,
        )
        return

    f_locals = frame.f_locals
    f_code = frame.f_code

    # Create a copy of generator function without the closure to serve as a box
    # to serialize the code, globals, name, and closure. Cloudpickle already
    # handles things like closures and complicated globals so just rely on
    # cloudpickle to serialize this function.
    gen_func = FunctionType(
        f_code,
        frame.f_globals,
        gen.__name__,
        (),
        (_empty_cell(), ) * len(f_code.co_freevars),
    )
    try:
        gen_func.__qualname__ = gen.__qualname__
    except AttributeError:
        # there is no __qualname__ on generators in Python < 3.5
        pass

    save = self.save
    write = self.write

    # push a function onto the stack to fill up our skeleton generator
    # or coroutine
    save(filler)

    # the start of the tuple to pass to ``_fill_generator`` (or
    # ``_fill_coroutine``, ``_fill_async_generator``)
    write(pickle.MARK)

    save(_create_skeleton_generator)
    save((gen_func, ))
    write(pickle.REDUCE)
    self.memoize(gen)

    # push the rest of the arguments to ``_fill_generator`` (or
    # ``_fill_coroutine``, ``_fill_async_generator``)
    save(frame.f_lasti)
    save(f_locals)
    save(private_frame_data(frame))

    # call ``_fill_generator`` (or ``_fill_coroutine``,
    # _fill_async_generator``)
    write(pickle.TUPLE)
    write(pickle.REDUCE)


def register_pypickler(CloudPickler):
    """Register the cloudpickle extension.
    """
    CloudPickler.dispatch[GeneratorType] = _save_generator
    CloudPickler.dispatch[CoroutineType] = _save_coroutine
    CloudPickler.dispatch[AsyncGeneratorType] = _save_async_generator


def unregister_pypickler(CloudPickler):
    """Unregister the cloudpickle extension.
    """
    if CloudPickler.dispatch.get(GeneratorType) is _save_generator:
        # make sure we are only removing the dispatch we added, not someone
        # else's
        del CloudPickler.dispatch[GeneratorType]

    if CloudPickler.dispatch.get(CoroutineType) is _save_coroutine:
        del CloudPickler.dispatch[CoroutineType]

    if (CloudPickler.dispatch.get(AsyncGeneratorType) is
            _save_async_generator):
        del CloudPickler.dispatch[AsyncGeneratorType]


def _rehydrate_generator(gen_func, lasti, f_locals, frame_data):
    gen = _create_skeleton_generator(gen_func)
    _fill_generator_impl(gen.gi_frame, lasti, f_locals, frame_data)
    return gen


def _rehydrate_coroutine(gen_func, lasti, f_locals, frame_data):
    coro = _create_skeleton_generator(gen_func)
    _fill_generator_impl(coro.cr_frame, lasti, f_locals, frame_data)
    return coro


def _rehydrate_async_generator(gen_func, lasti, f_locals, frame_data):
    asyncgen = _create_skeleton_generator(gen_func)
    _fill_generator_impl(asyncgen.ag_frame, lasti, f_locals, frame_data)
    return asyncgen


def _reduce_generator(gen):
    if gen.gi_running:
        raise ValueError('cannot save running generator')

    frame = gen.gi_frame
    if frame is None:
        # frame is None when the generator is fully consumed; take a fast path
        return _restore_spent_generator, (gen.__name__,
                                          getattr(gen, '__qualname__', None))
    return _rehydrate_generator, _reduce_generator_impl(frame, gen)


def _reduce_coroutine(coro):
    frame = coro.cr_frame
    if frame is None:
        # frame is None when the generator is fully consumed; take a fast path
        return _restore_spent_generator, (coro.__name__,
                                          getattr(coro, '__qualname__', None))
    return _rehydrate_coroutine, _reduce_generator_impl(frame, coro)


def _reduce_async_generator(asyncgen):
    frame = asyncgen.ag_frame
    if frame is None:
        # frame is None when the generator is fully consumed; take a fast path
        return _restore_spent_generator, (asyncgen.__name__,
                                          getattr(asyncgen, '__qualname__',
                                                  None))
    return _rehydrate_async_generator, _reduce_generator_impl(frame, asyncgen)


def _reduce_generator_impl(frame, gen):
    f_code = frame.f_code

    # Create a copy of generator function without the closure to serve as a box
    # to serialize the code, globals, name, and closure. Cloudpickle already
    # handles things like closures and complicated globals so just rely on
    # cloudpickle to serialize this function.
    gen_func = FunctionType(
        f_code,
        frame.f_globals,
        gen.__name__,
        (),
        (_empty_cell(), ) * len(f_code.co_freevars),
    )
    gen_func.__qualname__ = gen.__qualname__

    return gen_func, frame.f_lasti, frame.f_locals, private_frame_data(frame)
