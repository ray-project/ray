import os
from typing import Dict, Optional

import openai

DEFAULT_PROBE_HEADERS = {"User-Agent": "rayllm-prober", "Rayllm-Trace": "enable"}

API_KEY = "fake_api_key"
BASE_URL = os.environ["OPENAI_API_BASE"]
openai_client = openai.Client(
    api_key=API_KEY,
    base_url=BASE_URL,
    default_headers=DEFAULT_PROBE_HEADERS,
)


def create_async_client(
    api_key: Optional[str] = None,
    base_url: Optional[str] = None,
    headers: Optional[Dict] = None,
):
    client_headers = DEFAULT_PROBE_HEADERS
    if headers:
        # Merge any headers passed in the create call with the default headers
        client_headers = {
            **DEFAULT_PROBE_HEADERS,
            **headers,
        }
    return openai.AsyncClient(
        api_key=api_key or API_KEY,
        base_url=base_url or BASE_URL,
        default_headers=client_headers,
    )
