from ray.util.check_serialize import inspect_serializability

def test_nested_closure_function_context_in_output():
    """
    Regression test for https://github.com/ray-project/ray/issues/48759.
    The qualified function name should appear in traversal output when
    a closure contains a non-serializable object.
    """
    import io
    import threading

    def outer():
        lock = threading.Lock()

        def inner():
            return lock

        return inner

    out = io.StringIO()
    serializable, _ = inspect_serializability(outer(), print_file=out)

    output = out.getvalue()
    assert not serializable
    # The function identity must be visible in the traversal output
    assert "outer.<locals>.inner" in output, (
        f"Expected function qualname in output, got:\n{output}"
    )
