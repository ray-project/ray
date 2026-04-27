"""Regression tests for lazy imports in ``ray.llm._internal.batch``.

The ``stages`` and ``processor`` packages re-export classes whose defining
modules pull in heavy optional dependencies (``transformers``, ``vllm``,
``sglang``, ``mistral_common``). They are wired up via PEP 562
``__getattr__`` so that, e.g., importing ``HttpRequestProcessorConfig`` does
not drag the entire ML stack into ``sys.modules``.

These tests run in a fresh Python subprocess so that ``sys.modules`` is
guaranteed to be clean -- otherwise the modules-under-test could already be
loaded by an earlier test.
"""

import subprocess
import sys
import textwrap

import pytest


def _run_in_subprocess(script: str) -> str:
    """Run ``script`` in a clean Python subprocess and return stdout."""
    result = subprocess.run(
        [sys.executable, "-c", textwrap.dedent(script)],
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout


# Module names that the lightweight HTTP processor must NOT pull in.
# Each one is loaded by a different stage / processor module, so seeing any
# of them after importing the HTTP processor means the lazy wiring broke.
_HEAVY_MODULES = (
    "transformers",
    "tokenizers",
    "huggingface_hub",
    "mistral_common",
    "vllm.transformers_utils",
    # Stage submodules that pull in the heavy deps above.
    "ray.llm._internal.batch.stages.tokenize_stage",
    "ray.llm._internal.batch.stages.chat_template_stage",
    "ray.llm._internal.batch.stages.vllm_engine_stage",
    "ray.llm._internal.batch.stages.sglang_engine_stage",
    "ray.llm._internal.batch.stages.prepare_multimodal_stage",
    "ray.llm._internal.batch.stages.prepare_image_stage",
    "ray.llm._internal.batch.stages.serve_deployment_stage",
    # Processor submodules whose top-level statements import heavy deps
    # directly (e.g. ``import transformers`` in sglang_engine_proc.py).
    "ray.llm._internal.batch.processor.sglang_engine_proc",
    "ray.llm._internal.batch.processor.vllm_engine_proc",
    "ray.llm._internal.batch.processor.serve_deployment_proc",
)


def test_http_request_processor_does_not_import_heavy_deps():
    """HTTP-only processor imports must not load transformers/vllm/etc."""
    out = _run_in_subprocess(
        f"""
        import sys
        from ray.llm._internal.batch import HttpRequestProcessorConfig  # noqa: F401

        heavy = {_HEAVY_MODULES!r}
        loaded = [m for m in heavy if m in sys.modules]
        print(','.join(loaded))
        """
    )
    loaded = [m for m in out.strip().split(",") if m]
    assert loaded == [], (
        "Importing HttpRequestProcessorConfig must not pull in heavy ML "
        f"dependencies, but the following modules ended up loaded: {loaded}"
    )


def test_http_request_stage_only_loads_its_own_submodule():
    """``from ...stages import HttpRequestStage`` must only load that stage."""
    out = _run_in_subprocess(
        """
        import sys
        from ray.llm._internal.batch.stages import HttpRequestStage  # noqa: F401

        stage_modules = sorted(
            m for m in sys.modules
            if m.startswith('ray.llm._internal.batch.stages.')
            and m != 'ray.llm._internal.batch.stages.base'
            and m != 'ray.llm._internal.batch.stages.configs'
            and m != 'ray.llm._internal.batch.stages.common'
        )
        print(','.join(stage_modules))
        """
    )
    loaded = [m for m in out.strip().split(",") if m]
    assert loaded == [
        "ray.llm._internal.batch.stages.http_request_stage"
    ], f"Expected only http_request_stage to load, got: {loaded}"


@pytest.mark.parametrize(
    "name,submodule",
    [
        ("HttpRequestStage", "http_request_stage"),
        ("TokenizeStage", "tokenize_stage"),
        ("DetokenizeStage", "tokenize_stage"),
        ("ChatTemplateStage", "chat_template_stage"),
        ("PrepareImageStage", "prepare_image_stage"),
        ("PrepareMultimodalStage", "prepare_multimodal_stage"),
        ("ServeDeploymentStage", "serve_deployment_stage"),
        ("SGLangEngineStage", "sglang_engine_stage"),
        ("vLLMEngineStage", "vllm_engine_stage"),
    ],
)
def test_stage_lazy_attr_resolves(name, submodule):
    """Each lazy stage attr resolves to the class from the right submodule."""
    import ray.llm._internal.batch.stages as stages

    cls = getattr(stages, name)
    assert cls.__name__ == name
    assert cls.__module__ == f"ray.llm._internal.batch.stages.{submodule}"


@pytest.mark.parametrize(
    "name,submodule",
    [
        ("HttpRequestProcessorConfig", "http_request_proc"),
        ("ServeDeploymentProcessorConfig", "serve_deployment_proc"),
        ("SGLangEngineProcessorConfig", "sglang_engine_proc"),
        ("vLLMEngineProcessorConfig", "vllm_engine_proc"),
    ],
)
def test_processor_lazy_attr_resolves(name, submodule):
    """Each lazy processor-config attr resolves to the right class."""
    import ray.llm._internal.batch.processor as processor

    cls = getattr(processor, name)
    assert cls.__name__ == name
    assert cls.__module__ == f"ray.llm._internal.batch.processor.{submodule}"


def test_unknown_attr_raises_attribute_error():
    """``__getattr__`` must raise ``AttributeError`` for unknown names so
    that ``hasattr`` and other attribute-introspection paths behave correctly.
    """
    import ray.llm._internal.batch.processor as processor
    import ray.llm._internal.batch.stages as stages

    with pytest.raises(AttributeError):
        processor.DefinitelyNotAProcessor  # noqa: B018
    with pytest.raises(AttributeError):
        stages.DefinitelyNotAStage  # noqa: B018


def test_dir_lists_lazy_attrs():
    """``dir(pkg)`` must list the lazy attributes (for IDE completion etc.)."""
    import ray.llm._internal.batch.processor as processor
    import ray.llm._internal.batch.stages as stages

    for name in (
        "HttpRequestProcessorConfig",
        "ServeDeploymentProcessorConfig",
        "SGLangEngineProcessorConfig",
        "vLLMEngineProcessorConfig",
    ):
        assert name in dir(processor)
    for name in (
        "HttpRequestStage",
        "TokenizeStage",
        "DetokenizeStage",
        "ChatTemplateStage",
        "PrepareImageStage",
        "PrepareMultimodalStage",
        "ServeDeploymentStage",
        "SGLangEngineStage",
        "vLLMEngineStage",
    ):
        assert name in dir(stages)


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
