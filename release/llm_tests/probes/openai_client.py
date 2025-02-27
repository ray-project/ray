import os
from typing import Dict, Optional

import openai

DEFAULT_PROBE_HEADERS = {"User-Agent": "rayllm-prober", "Rayllm-Trace": "enable"}

openai_client = openai.Client(
    api_key=os.environ["OPENAI_API_KEY"],
    base_url=os.environ["OPENAI_API_BASE"],
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
        api_key=api_key or os.environ["OPENAI_API_KEY"],
        base_url=base_url or os.environ["OPENAI_API_BASE"],
        default_headers=client_headers,
    )
