import pytest

try:
    from vllm import LLM, SamplingParams  # noqa: F401
except ImportError:
    pytest.skip("vllm not installed", allow_module_level=True)


def test_vllm():
    sampling_params = SamplingParams(temperature=0.8, top_p=0.95)

    llm = LLM(model="facebook/opt-125m")
    outputs = llm.generate(["Hello, my name is"], sampling_params)
    assert len(outputs) == 1


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
