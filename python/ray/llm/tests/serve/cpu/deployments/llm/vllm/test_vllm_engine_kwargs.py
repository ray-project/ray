import sys

import pytest

from ray.llm._internal.serve.engines.vllm.vllm_engine import (
    _normalize_vllm_engine_kwargs,
)


@pytest.mark.parametrize(
    "kv_transfer_config, expected_role",
    [
        ({"kv_connector": "OffloadingConnector"}, "kv_both"),
        (
            {"kv_connector": "OffloadingConnector", "kv_role": "kv_producer"},
            "kv_producer",
        ),
    ],
)
def test_offloading_connector_kv_role_default(kv_transfer_config, expected_role):
    """vLLM rejects a kv_transfer_config that names a connector without a role.
    OffloadingConnector defaults to kv_both and keeps an explicit role."""
    normalized = _normalize_vllm_engine_kwargs(
        {"kv_transfer_config": kv_transfer_config}
    )
    assert normalized["kv_transfer_config"].kv_role == expected_role
    # The caller's dict is not mutated.
    assert kv_transfer_config.get("kv_role") in (None, "kv_producer")


def test_kv_role_default_kept_without_config_type(monkeypatch):
    """The default applies even when the field's config type can't be resolved."""
    from ray.llm._internal.serve.engines.vllm import vllm_engine

    monkeypatch.setattr(vllm_engine, "_get_vllm_config_type", lambda _: None)
    normalized = _normalize_vllm_engine_kwargs(
        {"kv_transfer_config": {"kv_connector": "OffloadingConnector"}}
    )
    assert normalized["kv_transfer_config"]["kv_role"] == "kv_both"


def test_other_connectors_still_require_kv_role():
    with pytest.raises(ValueError, match="kv_role"):
        _normalize_vllm_engine_kwargs(
            {"kv_transfer_config": {"kv_connector": "NixlConnector"}}
        )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
