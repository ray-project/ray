import functools
from typing import Any, Optional

from starlette.requests import Request

from ray.llm._internal.serve.routing_policies.kv_aware.constants import (
    KV_TOKEN_KEY_HEADER,
)
from ray.llm._internal.serve.routing_policies.kv_aware.token_channel import (
    TokenStore,
    decode_prompt_token_ids,
)


async def inject_prompt_token_ids(
    request: Any,
    raw_request: Optional[Request],
    store: TokenStore,
) -> None:
    if raw_request is None:
        return
    token_key = raw_request.headers.get(KV_TOKEN_KEY_HEADER)
    if not token_key:
        return

    entry = await store.pop(token_key)
    if entry is None:
        return

    kv_transfer_params = getattr(request, "kv_transfer_params", None)
    if not isinstance(kv_transfer_params, dict):
        kv_transfer_params = {}
        request.kv_transfer_params = kv_transfer_params
    kv_transfer_params["prompt_token_ids"] = decode_prompt_token_ids(entry.payload)


def _install_prompt_token_forwarding(
    serving: Any,
    method_name: str,
    store: TokenStore,
) -> None:
    if serving is None:
        return
    orig = getattr(serving, method_name, None)
    if orig is None or getattr(orig, "_kv_token_channel_wrapped", False):
        return

    @functools.wraps(orig)
    async def wrapped(request: Any, raw_request: Optional[Request] = None, *args, **kw):
        effective_raw_request = kw.get("raw_request", raw_request)
        await inject_prompt_token_ids(request, effective_raw_request, store)
        if "raw_request" in kw and raw_request is None:
            return await orig(request, *args, **kw)
        return await orig(request, raw_request, *args, **kw)

    wrapped._kv_token_channel_wrapped = True
    setattr(serving, method_name, wrapped)


def install_prompt_token_forwarding(
    state: Any,
    store: TokenStore,
) -> None:
    for attr_name, method_name in (
        ("openai_serving_chat", "create_chat_completion"),
        ("openai_serving_completion", "create_completion"),
    ):
        _install_prompt_token_forwarding(
            getattr(state, attr_name, None),
            method_name,
            store,
        )
