"""
This file contains exceptional cases which are covering for bugs or
other _temporary_ issues.

Each function or behavior here MUST have an attached issue ticket
"""
import os

import pytest

XFAIL_MODE = os.getenv("PROBES_BUGS_XFAIL_MODE", "xfail")


def mark_xfail_or_skip(message):
    if XFAIL_MODE == "skip":
        return pytest.mark.skip(message)
    else:
        return pytest.mark.xfail(reason=message)


def xfail_or_skip(message):
    if XFAIL_MODE == "skip":
        return pytest.skip(message)
    else:
        return pytest.xfail(reason=message)


def maybe_mark_param(obj, fn, message):
    if fn(obj):
        return mark_param(obj, message)

    return obj


def maybe_skip_or_xfail(obj, fn, message):
    if fn(obj):
        return xfail_or_skip(message)

    return obj


def mark_param(param, message):
    return pytest.param(param, marks=mark_xfail_or_skip(message))


# No ticket
def is_flaky_for_exchange_rate(model_id):
    """
    (Avnishn) The exchange rate test case checks that given a prompt about
    getting an exchange rate, that when given the choice to select from the
    tools [get_weather, calculate_mortgage_payment, get_article_details,
    get_weather, get_directions,], and no tool, that the model will select no
    tool. Mistral is flakey at selecting no tool and this is an artifact of
    the model, and not the functionallity of our function calling mode.

    Therefore I am disabling this test case for now, till we deploy function
    calling for a model that has stronger performance at this type of task.
    """
    return "Mistral-7B-Instruct-v0.1" in model_id


# AE-614
def is_json_probe_broken(model_id):
    """
    This should be fixed with a faster JSON-calling implementation or allowing
    longer timeouts
    """
    return "Mixtral-8x7B" in model_id


# AE-614
def is_fn_probe_broken(model_id):
    return "Mixtral-8x7B" in model_id


def xfail_if_fn_probe_broken(model_id):
    """
    This should be fixed with a faster function-calling implementation or allowing longer timeouts
    """
    return maybe_mark_param(
        model_id,
        is_fn_probe_broken,
        f"AE-614: {model_id} is timing-out function calling",
    )


def xfail_if_json_probe_broken(model_id):
    return maybe_mark_param(
        model_id, is_json_probe_broken, "AE-614: {model_id} is timing-out JSON calling"
    )
