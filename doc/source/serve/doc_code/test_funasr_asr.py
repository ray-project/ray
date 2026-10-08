import asyncio
import importlib.util
import sys
import types
from pathlib import Path
from unittest.mock import patch

import pytest


MODULE_PATH = Path(__file__).with_name("funasr_asr.py")


def _identity_decorator(*args, **kwargs):
    def decorator(obj):
        return obj

    return decorator


def _deployment_decorator(*args, **kwargs):
    def decorator(cls):
        cls.bind = classmethod(lambda cls, *args, **kwargs: None)
        return cls

    return decorator


class _FastAPI:
    def post(self, *args, **kwargs):
        return _identity_decorator()


def _parameter(default=..., **kwargs):
    return default


def _load_funasr_asr():
    fastapi = types.ModuleType("fastapi")
    fastapi.FastAPI = _FastAPI
    fastapi.File = _parameter
    fastapi.Form = _parameter
    fastapi.HTTPException = Exception
    fastapi.UploadFile = object

    fastapi_responses = types.ModuleType("fastapi.responses")
    fastapi_responses.PlainTextResponse = str

    serve = types.ModuleType("ray.serve")
    serve.batch = _identity_decorator
    serve.deployment = _deployment_decorator
    serve.get_multiplexed_model_id = lambda: ""
    serve.ingress = _identity_decorator
    serve.multiplexed = _identity_decorator

    ray = types.ModuleType("ray")
    ray.serve = serve

    ray_serve_handle = types.ModuleType("ray.serve.handle")
    ray_serve_handle.DeploymentHandle = object

    module_spec = importlib.util.spec_from_file_location(
        "_funasr_asr_under_test", MODULE_PATH
    )
    module = importlib.util.module_from_spec(module_spec)
    fake_modules = {
        "fastapi": fastapi,
        "fastapi.responses": fastapi_responses,
        "ray": ray,
        "ray.serve": serve,
        "ray.serve.handle": ray_serve_handle,
    }
    with patch.dict(sys.modules, fake_modules):
        module_spec.loader.exec_module(module)
    return module


class _RecordingModel:
    def __init__(self):
        self.calls = []

    def generate(self, **kwargs):
        language = kwargs["language"]
        if not isinstance(language, str):
            raise TypeError(f"unhashable type: '{type(language).__name__}'")

        audio_batch = [Path(audio_path).read_bytes() for audio_path in kwargs["input"]]
        self.calls.append((language, audio_batch))
        return [
            {
                "text": audio.decode(),
                "sentence_info": [{"text": audio.decode()}],
            }
            for audio in audio_batch
        ]


def _make_asr(module, model):
    asr = module.FunASRModel.__new__(module.FunASRModel)
    asr._default_model = "test-model"

    async def get_model(model_name):
        assert model_name == "test-model"
        return model

    def postprocess(text):
        return text

    asr._get_model = get_model
    asr._postprocess = postprocess
    return asr


def test_transcribe_uses_scalar_auto_language_for_single_request():
    module = _load_funasr_asr()
    recording_model = _RecordingModel()
    asr = _make_asr(module, recording_model)

    responses = asyncio.run(
        asr.transcribe(
            audio_bytes=[b"default"],
            filenames=["default.wav"],
            languages=[None],
            response_formats=["json"],
        )
    )

    assert recording_model.calls == [("auto", [b"default"])]
    assert responses == [{"text": "default"}]


def test_transcribe_groups_mixed_languages_and_restores_request_order():
    module = _load_funasr_asr()
    recording_model = _RecordingModel()
    asr = _make_asr(module, recording_model)

    responses = asyncio.run(
        asr.transcribe(
            audio_bytes=[b"first", b"second", b"third", b"fourth"],
            filenames=["first.wav", "second.wav", "third.wav", "fourth.wav"],
            languages=["en", "zh", "en", None],
            response_formats=["json", "verbose_json", "json", "verbose_json"],
        )
    )

    assert recording_model.calls == [
        ("en", [b"first", b"third"]),
        ("zh", [b"second"]),
        ("auto", [b"fourth"]),
    ]
    assert responses == [
        {"text": "first"},
        {
            "text": "second",
            "segments": [{"text": "second"}],
            "model": "test-model",
        },
        {"text": "third"},
        {
            "text": "fourth",
            "segments": [{"text": "fourth"}],
            "model": "test-model",
        },
    ]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
