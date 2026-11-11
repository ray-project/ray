import json
import sys

import pytest

from ray.dashboard.modules.metrics.metrics_head import (
    DEFAULT_PROMETHEUS_HEADERS,
    parse_prom_headers,
)


def test_router_prefill_panel_keeps_each_handle_separate():
    from ray.dashboard.modules.metrics.grafana_dashboard_factory import (
        generate_serve_deployment_grafana_dashboard,
        generate_serve_llm_grafana_dashboard,
    )

    dashboard, _ = generate_serve_deployment_grafana_dashboard()
    assert all(p["title"] != "KV Router" for p in json.loads(dashboard)["panels"])
    dashboard, _ = generate_serve_llm_grafana_dashboard()
    panels = json.loads(dashboard)["panels"]
    row = next(p for p in panels if p["title"] == "KV Router")
    assert row["type"] == "row"
    assert row["collapsed"] is True
    assert all(p["title"] != "Uncached prefill tokens per router" for p in panels)
    panel = next(
        p for p in row["panels"] if p["title"] == "Uncached prefill tokens per router"
    )
    assert panel["stack"] is False
    assert panel["description"] == (
        "Current estimated global uncached prefill tokens reported by each router handle."
    )
    target = panel["targets"][0]
    assert target["expr"].startswith("ray_serve_router_uncached_prefill_tokens{")
    assert 'deployment=~"$deployment"' in target["expr"]
    assert "$workerid" not in target["expr"]
    assert "$vllm_model_name" not in target["expr"]
    assert "or vector(0)" not in target["expr"]
    assert "{{handle}}" in target["legendFormat"]
    assert "{{actor_id}}" in target["legendFormat"]


@pytest.mark.parametrize(
    "headers, raises_error",
    [
        (DEFAULT_PROMETHEUS_HEADERS, False),
        ('{"H1": "V1", "H2": "V2"}', False),
        ('[["H1", "V1"], ["H2", "V2"], ["H2", "V3"]]', False),
        ('{"H1": "V1", "H2": ["V1", "V2"]}', True),
        ("not_json", True),
    ],
)
def test_parse_prom_headers(headers, raises_error):
    if raises_error:
        with pytest.raises(ValueError):
            parse_prom_headers(headers)
    else:
        result = parse_prom_headers(headers)
        assert result == json.loads(headers)


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
