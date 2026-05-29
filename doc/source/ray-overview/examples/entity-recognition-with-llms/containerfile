FROM anyscale/ray-llm:2.44.1-py311-cu124

RUN python3 -m pip install --no-cache-dir \
    "xgrammar==0.1.11" \
    "pynvml==12.0.0" \
    "hf_transfer==0.1.9" \
    "tensorboard==2.19.0" \
    "git+https://github.com/hiyouga/LLaMA-Factory.git@ac8c6fdd3ab7fb6372f231f238e6b8ba6a17eb16#egg=llamafactory"

# Fast upload/download
ENV HF_HUB_ENABLE_HF_TRANSFER=1
