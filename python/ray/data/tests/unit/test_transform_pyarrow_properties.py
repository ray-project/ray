"""Property-based tests for ``transform_pyarrow`` schema unification.

Unlike the example-based tests in ``test_transform_pyarrow.py``, which pin
specific type combinations, these check algebraic laws over a generated space
of PyArrow types.

``unify_schemas`` computes a join on a type lattice: it merges the schemas of
blocks produced independently by parallel tasks. Because blocks arrive in
nondeterministic order, the unified schema must not depend on that order, and
must be an upper bound of every input. That gives four laws:

    commutative:  unify(a, b)         == unify(b, a)
    associative:  unify(unify(a,b),c) == unify(a, unify(b,c))
    idempotent:   unify(a, a)         == a
    order-free:   unify(perm(S))      == unify(S) for any permutation

The generated types deliberately cover the branches ``_reconcile_field``
special-cases: ``null`` (the lattice bottom), nested structs, and lists.

Hypothesis runs here are **derandomized**: a given commit always explores the
same inputs, so a failure is reproducible and is never a flake. Raising
``max_examples`` locally is a cheap way to search harder when touching
``_reconcile_field``.
"""
import itertools

import pyarrow as pa
import pytest
from hypothesis import HealthCheck, assume, given, settings, strategies as st

from ray.data._internal.arrow_ops.transform_pyarrow import unify_schemas

settings.register_profile(
    "ray_data_ci",
    max_examples=200,
    derandomize=True,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow],
)
settings.load_profile("ray_data_ci")


leaf_types = st.sampled_from(
    [
        pa.null(),
        pa.int32(),
        pa.int64(),
        pa.float64(),
        pa.string(),
        pa.bool_(),
    ]
)


def _struct_types(children):
    return st.lists(
        st.tuples(st.sampled_from(["x", "y"]), children),
        min_size=1,
        max_size=2,
        unique_by=lambda kv: kv[0],
    ).map(lambda kvs: pa.struct([pa.field(k, t) for k, t in kvs]))


field_types = st.recursive(
    leaf_types,
    lambda children: st.one_of(_struct_types(children), children.map(pa.list_)),
    max_leaves=3,
)

schemas = st.lists(
    st.tuples(st.sampled_from(["a", "b"]), field_types),
    min_size=1,
    max_size=2,
    unique_by=lambda kv: kv[0],
).map(lambda kvs: pa.schema([pa.field(k, t) for k, t in kvs]))


def _unify(schemas_, promote_types):
    """Returns ``(succeeded, schema_or_exception_name)``."""
    try:
        return True, unify_schemas(list(schemas_), promote_types=promote_types)
    except Exception as e:  # noqa: BLE001 - any failure is a failure to unify
        return False, type(e).__name__


def _normalize(dtype):
    """Normalize a type modulo struct field *order*.

    PyArrow's own ``unify_schemas`` orders struct fields by first appearance,
    so that ordering is inherited rather than a property of this code. Compare
    the semantic content instead.
    """
    if pa.types.is_struct(dtype):
        return ("struct", tuple(sorted((f.name, _normalize(f.type)) for f in dtype)))
    if pa.types.is_list(dtype) or pa.types.is_large_list(dtype):
        return ("list", _normalize(dtype.value_type))
    return str(dtype)


def _typemap(schema):
    return {f.name: _normalize(f.type) for f in schema}


@given(s1=schemas, s2=schemas, promote=st.booleans())
def test_unify_schemas_is_commutative(s1, s2, promote):
    ok_a, a = _unify([s1, s2], promote)
    ok_b, b = _unify([s2, s1], promote)
    assert ok_a == ok_b, (
        f"unify succeeded in one order but not the other\n"
        f"  unify(s1, s2) -> {'ok' if ok_a else a}\n"
        f"  unify(s2, s1) -> {'ok' if ok_b else b}\n"
        f"  s1={s1}\n  s2={s2}"
    )
    if ok_a:
        assert _typemap(a) == _typemap(b), f"s1={s1}\ns2={s2}\na={a}\nb={b}"


@given(s=schemas, promote=st.booleans())
def test_unify_schemas_is_idempotent(s, promote):
    ok, out = _unify([s, s], promote)
    assert ok, f"unify(s, s) raised {out} for s={s}"
    assert _typemap(out) == _typemap(s)


@given(s1=schemas, s2=schemas, s3=schemas, promote=st.booleans())
def test_unify_schemas_is_associative(s1, s2, s3, promote):
    ok_l, left = _unify([s1, s2], promote)
    ok_r, right = _unify([s2, s3], promote)
    assume(ok_l and ok_r)
    ok_a, a = _unify([left, s3], promote)
    ok_b, b = _unify([s1, right], promote)
    assume(ok_a and ok_b)
    assert _typemap(a) == _typemap(b), (
        f"associativity violated\n s1={s1}\n s2={s2}\n s3={s3}\n"
        f" (s1+s2)+s3 = {a}\n s1+(s2+s3) = {b}"
    )


@given(ss=st.lists(schemas, min_size=2, max_size=3), promote=st.booleans())
def test_unify_schemas_is_order_independent(ss, promote):
    """Blocks arrive from parallel tasks in nondeterministic order.

    This is the law Ray Data actually depends on, and the one that caught
    ``_reconcile_field`` reconciling ``[list<int32>, list<int64>, list<null>]``
    to whichever concrete type happened to come first.
    """
    ok0, base = _unify(ss, promote)
    for perm in itertools.permutations(ss):
        ok, out = _unify(perm, promote)
        assert ok == ok0, (
            f"unify succeeded for one permutation but not another\n"
            f"  original -> {'ok' if ok0 else base}\n"
            f"  permuted -> {'ok' if ok else out}\n"
            f"  schemas={[str(s) for s in perm]}"
        )
        if ok0:
            assert _typemap(out) == _typemap(base), (
                f"permutation changed the unified schema\n"
                f"  base={base}\n  perm={out}"
            )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
