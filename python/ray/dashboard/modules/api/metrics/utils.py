import json

from ray.dashboard.modules.api.metrics.consts import PROMETHEUS_HEADERS_ENV_VAR


# parse_prom_headers will make sure the input is in one of the following formats:
# 1. {"H1": "V1", "H2": "V2"}
# 2. [["H1", "V1"], ["H2", "V2"], ["H2", "V3"]]
def parse_prom_headers(prometheus_headers):
    parsed = json.loads(prometheus_headers)
    if isinstance(parsed, dict):
        if all(isinstance(k, str) and isinstance(v, str) for k, v in parsed.items()):
            return parsed
    if isinstance(parsed, list):
        if all(len(e) == 2 and all(isinstance(v, str) for v in e) for e in parsed):
            return parsed
    raise ValueError(
        f"{PROMETHEUS_HEADERS_ENV_VAR} should be a JSON string in one of the formats:\n"
        + "1) An object with string keys and string values.\n"
        + "2) an array of string arrays with 2 string elements each.\n"
        + 'For example, {"H1": "V1", "H2": "V2"} and\n'
        + '[["H1", "V1"], ["H2", "V2"], ["H2", "V3"]] are valid.'
    )


class PrometheusQueryError(Exception):
    def __init__(self, status, message):
        self.message = (
            "Error fetching data from prometheus. "
            f"status: {status}, message: {message}"
        )
        super().__init__(self.message)
