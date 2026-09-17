# Dedicated image for the Ray Serve LLM custom-vLLM-model docs test. It layers
# the custom reward-model plugin on top of the standard LLM GPU test image so
# the plugin is available on every node without touching the shared image.
ARG DOCKER_IMAGE_BASE=cr.ray.io/rayproject/llmgpubuild
FROM $DOCKER_IMAGE_BASE

COPY . .

# The plugin's entry point registers Qwen3CustomRewardModel in every vLLM process.
RUN pip install --no-deps doc/source/llm/doc_code/serve/custom_vllm
