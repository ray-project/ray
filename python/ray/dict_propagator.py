from typing import Any, cast, Dict

from opentelemetry import propagators  # type: ignore
from opentelemetry.context.context import Context
from opentelemetry.trace.propagation.textmap import DictGetter


def inject_current_context() -> Dict[Any, Any]:
    context_dict: Dict[Any, Any] = {}
    propagators.inject(dict.__setitem__, context_dict)
    return context_dict


def extract(context_dict: Dict[Any, Any]) -> Context:
    return cast(Context, propagators.extract(DictGetter(), context_dict))
