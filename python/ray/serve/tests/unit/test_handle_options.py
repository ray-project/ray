import sys
from unittest.mock import MagicMock

import pytest

from ray.serve._private.common import DeploymentHandleSource
from ray.serve._private.handle_options import DynamicHandleOptions, InitHandleOptions
from ray.serve._private.utils import DEFAULT
from ray.serve.handle import (
    DeploymentHandle,
    _normalize_candidate_multiplexed_model_ids,
)


def test_dynamic_handle_options():
    default_options = DynamicHandleOptions()
    assert default_options.method_name == "__call__"
    assert default_options.multiplexed_model_id == ""
    assert default_options.session_id == ""
    assert default_options.stream is False

    # Test setting method name.
    only_set_method = default_options.copy_and_update(method_name="hi")
    assert only_set_method.method_name == "hi"
    assert only_set_method.multiplexed_model_id == ""
    assert only_set_method.session_id == ""
    assert only_set_method.stream is False

    # Existing options should be unmodified.
    assert default_options.method_name == "__call__"
    assert default_options.multiplexed_model_id == ""
    assert default_options.session_id == ""
    assert default_options.stream is False

    # Test setting model ID.
    only_set_model_id = default_options.copy_and_update(multiplexed_model_id="hi")
    assert only_set_model_id.method_name == "__call__"
    assert only_set_model_id.multiplexed_model_id == "hi"
    assert only_set_model_id.session_id == ""
    assert only_set_model_id.stream is False

    # Existing options should be unmodified.
    assert default_options.method_name == "__call__"
    assert default_options.multiplexed_model_id == ""
    assert default_options.session_id == ""
    assert default_options.stream is False

    # Test setting stream.
    only_set_stream = default_options.copy_and_update(stream=True)
    assert only_set_stream.method_name == "__call__"
    assert only_set_stream.multiplexed_model_id == ""
    assert only_set_stream.session_id == ""
    assert only_set_stream.stream is True

    # Existing options should be unmodified.
    assert default_options.method_name == "__call__"
    assert default_options.multiplexed_model_id == ""
    assert default_options.session_id == ""
    assert default_options.stream is False

    # Test setting session ID.
    only_set_session_id = default_options.copy_and_update(session_id="sess_abc")
    assert only_set_session_id.method_name == "__call__"
    assert only_set_session_id.multiplexed_model_id == ""
    assert only_set_session_id.session_id == "sess_abc"
    assert only_set_session_id.stream is False

    # Existing options should be unmodified.
    assert default_options.method_name == "__call__"
    assert default_options.multiplexed_model_id == ""
    assert default_options.session_id == ""
    assert default_options.stream is False

    # Test setting multiple.
    set_multiple = default_options.copy_and_update(method_name="hi", stream=True)
    assert set_multiple.method_name == "hi"
    assert set_multiple.multiplexed_model_id == ""
    assert set_multiple.session_id == ""
    assert set_multiple.stream is True


def test_init_handle_options():
    default_options = InitHandleOptions.create()
    assert default_options._prefer_local_routing is False
    assert default_options._source == DeploymentHandleSource.UNKNOWN

    default1 = InitHandleOptions.create(_prefer_local_routing=DEFAULT.VALUE)
    assert default1._prefer_local_routing is False
    assert default1._source == DeploymentHandleSource.UNKNOWN

    default2 = InitHandleOptions.create(_source=DEFAULT.VALUE)
    assert default2._prefer_local_routing is False
    assert default2._source == DeploymentHandleSource.UNKNOWN

    prefer_local = InitHandleOptions.create(
        _prefer_local_routing=True, _source=DEFAULT.VALUE
    )
    assert prefer_local._prefer_local_routing is True
    assert prefer_local._source == DeploymentHandleSource.UNKNOWN

    proxy_options = InitHandleOptions.create(_source=DeploymentHandleSource.PROXY)
    assert proxy_options._prefer_local_routing is False
    assert proxy_options._source == DeploymentHandleSource.PROXY


def test_dynamic_handle_options_candidate_multiplexed_model_ids():
    default_options = DynamicHandleOptions()
    assert default_options.candidate_multiplexed_model_ids == ()

    only_set_candidates = default_options.copy_and_update(
        candidate_multiplexed_model_ids=("v7", "v6")
    )
    assert only_set_candidates.candidate_multiplexed_model_ids == ("v7", "v6")
    assert only_set_candidates.multiplexed_model_id == ""
    assert only_set_candidates.method_name == "__call__"

    # Existing options should be unmodified.
    assert default_options.candidate_multiplexed_model_ids == ()


@pytest.mark.parametrize(
    "candidate_multiplexed_model_ids,expected",
    [
        (["v7", "v6", "v5"], ("v7", "v6", "v5")),
        ([], ()),
        # Already-frozen input is accepted so internal callers can pass it back.
        (("v7",), ("v7",)),
    ],
)
def test_normalize_candidate_multiplexed_model_ids(
    candidate_multiplexed_model_ids, expected
):
    assert (
        _normalize_candidate_multiplexed_model_ids(candidate_multiplexed_model_ids)
        == expected
    )


def test_options_rejects_both_multiplexed_model_id_fields():
    handle = DeploymentHandle("D", "app")

    with pytest.raises(ValueError, match="Only one of"):
        handle.options(
            multiplexed_model_id="v7",
            candidate_multiplexed_model_ids=["v6", "v5"],
        )


@pytest.mark.parametrize(
    "first,second,expected_model_id,expected_candidates",
    [
        (
            {"candidate_multiplexed_model_ids": ["v7", "v6"]},
            {"multiplexed_model_id": "v5"},
            "v5",
            (),
        ),
        (
            {"multiplexed_model_id": "v5"},
            {"candidate_multiplexed_model_ids": ["v7", "v6"]},
            "",
            ("v7", "v6"),
        ),
        # An empty list carries no model ID, so it clears the candidates without
        # disturbing a single ID set earlier.
        (
            {"multiplexed_model_id": "v5"},
            {"candidate_multiplexed_model_ids": []},
            "v5",
            (),
        ),
        # An unrelated option leaves both fields alone.
        (
            {"candidate_multiplexed_model_ids": ["v7", "v6"]},
            {"method_name": "other"},
            "",
            ("v7", "v6"),
        ),
    ],
)
def test_options_chaining_keeps_multiplexed_fields_exclusive(
    first, second, expected_model_id, expected_candidates
):
    # A stub router marks the handle initialized so `options()` doesn't need a
    # running Serve instance.
    handle = DeploymentHandle("D", "app", _router=MagicMock())

    options = handle.options(**first).options(**second).handle_options

    assert options.multiplexed_model_id == expected_model_id
    assert options.candidate_multiplexed_model_ids == expected_candidates


@pytest.mark.parametrize(
    "candidate_multiplexed_model_ids,expected_error",
    [
        ("v7", TypeError),  # a bare string is not a list of IDs
        (object(), TypeError),
        ([1], TypeError),
        ([""], ValueError),
        (["v7", "v7"], ValueError),
    ],
)
def test_normalize_candidate_multiplexed_model_ids_rejects_invalid(
    candidate_multiplexed_model_ids, expected_error
):
    with pytest.raises(expected_error):
        _normalize_candidate_multiplexed_model_ids(candidate_multiplexed_model_ids)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
