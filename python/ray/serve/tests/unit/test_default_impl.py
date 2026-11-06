import sys

import pytest

from ray.serve._private.default_impl import get_request_metadata
from ray.serve._private.handle_options import DynamicHandleOptions, InitHandleOptions


@pytest.fixture
def init_options() -> InitHandleOptions:
    return InitHandleOptions.create()


@pytest.mark.parametrize(
    "multiplexed_model_id,candidate_multiplexed_model_ids,expected_model_id",
    [
        ("v7", (), "v7"),
        ("", ("v7", "v6", "v5"), "v7"),
        ("", ("only",), "only"),
        ("", (), ""),
        ("explicit", ("v7", "v6"), "explicit"),
    ],
    ids=[
        "single_id_is_passed_through",
        "candidates_resolve_to_first_entry",
        "one_candidate_behaves_like_a_single_id",
        "no_multiplexing_options_set",
        "explicit_id_wins_over_candidates",
    ],
)
def test_multiplexed_model_id_resolution(
    init_options,
    multiplexed_model_id,
    candidate_multiplexed_model_ids,
    expected_model_id,
):
    """`get_request_metadata` resolves a candidate list to a single model ID.

    The replica loads exactly one model, so routers that don't understand candidate
    lists must still see a usable `multiplexed_model_id`. An explicitly set ID is
    never overwritten.
    """
    handle_options = DynamicHandleOptions(
        multiplexed_model_id=multiplexed_model_id,
        candidate_multiplexed_model_ids=candidate_multiplexed_model_ids,
    )

    metadata = get_request_metadata(init_options, handle_options)

    assert metadata.multiplexed_model_id == expected_model_id
    assert metadata.candidate_multiplexed_model_ids == candidate_multiplexed_model_ids


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
