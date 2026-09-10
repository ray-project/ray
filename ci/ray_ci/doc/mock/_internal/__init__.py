"""Stand-in for a library's private implementation package.

Mirrors ``ray.data._internal``: the implementation lives under a private module
while the public name is re-exported from a public module's ``__all__``. The
package is named ``_internal`` on purpose, because the check's private-name
heuristic keys on that exact segment.
"""


class MockReexportedClass:
    """Implemented privately, re-exported from ``mock_module.__all__``.

    Documenting this through its public name is correct, so the check must not
    flag its private canonical name.
    """

    pass


class MockInternalOnlyClass:
    """Implemented privately and absent from any public ``__all__``.

    The negative control: reachable as a module attribute but never exported,
    so the check must keep flagging it.
    """

    pass
