import json
import sys

import pytest

from ray.dashboard.modules.metrics.metrics_head import (
    DEFAULT_PROMETHEUS_HEADERS,
    parse_prom_headers,
)


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
