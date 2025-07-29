---
orphan: true
---

(serve-deepseek-tutorial)=

# Serve DeepSeek

This example shows how to deploy DeepSeek R1 or V3 with Ray Serve LLM.

## Installation

To run this example, install the following:

```bash
pip install "ray[llm]==2.46.0"
```

Note: Deploying DeepSeek-R1 requires at least 720GB of free disk space per worker node to store model weights.

## Deployment

### Quick Deployment

For quick deployment and testing, save the following code to a file named `deepseek.py`,
and run `python3 deepseek.py`.

```python
from ray import serve
from ray.serve.llm import LLMConfig, build_openai_app

llm_config = LLMConfig(
    model_loading_config={
        "model_id": "deepseek",
        "model_source": "deepseek-ai/DeepSeek-R1",
    },
    deployment_config={
        "autoscaling_config": {
            "min_replicas": 1,
            "max_replicas": 1,
        }
    },
    # Change to the accelerator type of the node
    accelerator_type="H100",
    runtime_env={"env_vars": {"VLLM_USE_V1": "1"}},
    # Customize engine arguments as needed (e.g. vLLM engine kwargs)
    engine_kwargs={
        "tensor_parallel_size": 8,
        "pipeline_parallel_size": 2,
        "gpu_memory_utilization": 0.92,
        "dtype": "auto",
        "max_num_seqs": 40,
        "max_model_len": 16384,
        "enable_chunked_prefill": True,
        "enable_prefix_caching": True,
    },
)

# Deploy the application
llm_app = build_openai_app({"llm_configs": [llm_config]})
serve.run(llm_app)
```

### Production Deployment

For production deployments, save the following to a YAML file named `deepseek.yaml`
and run `serve run deepseek.yaml`.

```yaml
applications:
- args:
    llm_configs:
      - model_loading_config:
          model_id: "deepseek"
          model_source: "deepseek-ai/DeepSeek-R1"
        accelerator_type: "H100"
        deployment_config:
          autoscaling_config:
            min_replicas: 1
            max_replicas: 1
        runtime_env:
          env_vars:
            VLLM_USE_V1: "1"
        engine_kwargs:
          tensor_parallel_size: 8
          pipeline_parallel_size: 2
          gpu_memory_utilization: 0.92
          dtype: "auto"
          max_num_seqs: 40
          max_model_len: 16384
          enable_chunked_prefill: true
          enable_prefix_caching: true
  import_path: ray.serve.llm:build_openai_app
  name: llm_app
  route_prefix: "/"
```

## Configuration

You may need to adjust configurations in the above code based on your setup, specifically:

* `accelerator_type`: for NVIDIA GPUs, DeepSeek requires Hopper GPUs or later ones. 
Therefore, you can specify `H200`, `H100`, `H20` etc. based on your hardware.
* `tensor_parallel_size` and `pipeline_parallel_size`: DeepSeek requires a single node of 8xH200,
or two nodes of 8xH100. The typical setup of using H100 is setting `tensor_parallel_size` to `8`
and `pipeline_parallel_size` to `2` as in the code example. When using H200, you can set
`tensor_parallel_size` to `8` and leave out the `pipeline_parallel_size` parameter
(it is `1` by default).
* `model_source`: although you could specify a HuggingFace model ID like `deepseek-ai/DeepSeek-R1` 
in the code example, it is recommended to pre-download the model because it is huge.
You can download it to the local file system (e.g., `/path/to/downloaded/model`)
or to a remote object store (e.g., `s3://my-bucket/path/to/downloaded/model`),
and specify it as `model_source`. It is recommended to download it to a remote object store,
using {ref}`Ray model caching utilities <model_cache>`. 
Note that if you have two nodes and would like to download to local file system,
you need to download the model to the same path on both nodes.


## Testing the Service

You can query the deployed model using the following request and get the corresponding response.

::::{tab-set}
:::{tab-item} Request
```bash
curl -X POST http://localhost:8000/v1/chat/completions \
     -H "Content-Type: application/json" \
     -H "Authorization: Bearer fake-key" \
     -d '{
           "model": "deepseek",
           "messages": [{"role": "user", "content": "Hello!"}]
         }'
```
:::

:::{tab-item} Response
```bash
{"id":"deepseek-68b5d5c5-fd34-42fc-be26-0a36f8457ffe","object":"chat.completion","created":1743646776,"model":"deepseek","choices":[{"index":0,"message":{"role":"assistant","reasoning_content":null,"content":"Hello! How can I assist you today? 😊","tool_calls":[]},"logprobs":null,"finish_reason":"stop","stop_reason":null}],"usage":{"prompt_tokens":6,"total_tokens":18,"completion_tokens":12,"prompt_tokens_details":null},"prompt_logprobs":null}
```
:::
::::

Another example request and response:

::::{tab-set}
:::{tab-item} Request
```bash
curl -X POST http://localhost:8000/v1/chat/completions \
    -H "Content-Type: application/json" \
    -H "Authorization: Bearer fake-key" \
    -d '{
        "model": "deepseek",
        "messages": [{"role": "user", "content": "The future of AI is"}]
        }'
```
:::

:::{tab-item} Response
```bash
{"id":"deepseek-b81ff9be-3ffc-4811-80ff-225006eff27c","object":"chat.completion","created":1743646860,"model":"deepseek","choices":[{"index":0,"message":{"role":"assistant","reasoning_content":null,"content":"The future of AI is multifaceted and holds immense potential across various domains. Here are some key aspects that are likely to shape its trajectory:\n\n1. **Advanced Automation**: AI will continue to automate routine and complex tasks across industries, increasing efficiency and productivity. This includes everything from manufacturing and logistics to healthcare and finance.\n\n2. **Enhanced Decision-Making**: AI systems will provide deeper insights and predictive analytics, aiding in better decision-making processes for businesses, governments, and individuals.\n\n3. **Personalization**: AI will drive more personalized experiences in areas such as shopping, education, and entertainment, tailoring services and products to individual preferences and behaviors.\n\n4. **Healthcare Revolution**: AI will play a significant role in diagnosing diseases, personalizing treatment plans, and even predicting health issues before they become critical, potentially transforming the healthcare industry.\n\n5. **Ethical and Responsible AI**: As AI becomes more integrated into society, there will be a growing focus on developing ethical guidelines and frameworks to ensure AI is used responsibly and transparently, addressing issues like bias, privacy, and security.\n\n6. **Human-AI Collaboration**: The future will see more seamless collaboration between humans and AI, with AI augmenting human capabilities rather than replacing them. This includes areas like creative industries, where AI can assist in generating ideas and content.\n\n7. **AI in Education**: AI will personalize learning experiences, adapt to individual learning styles, and provide real-time feedback, making education more accessible and effective.\n\n8. **Robotics and Autonomous Systems**: Advances in AI will lead to more sophisticated robots and autonomous systems, impacting industries like transportation (e.g., self-driving cars), agriculture, and home automation.\n\n9. **AI and Sustainability**: AI will play a crucial role in addressing environmental challenges by optimizing resource use, improving energy efficiency, and aiding in climate modeling and conservation efforts.\n\n10. **Regulation and Governance**: As AI technologies advance, there will be increased efforts to establish international standards and regulations to govern their development and use, ensuring they benefit society as a whole.\n\n11. **Quantum Computing and AI**: The integration of quantum computing with AI could revolutionize data processing capabilities, enabling the solving of complex problems that are currently intractable.\n\n12. **AI in Creative Fields**: AI will continue to make strides in creative domains such as music, art, and literature, collaborating with human creators to push the boundaries of innovation and expression.\n\nOverall, the future of AI is both promising and challenging, requiring careful consideration of its societal impact and the ethical implications of its widespread adoption.","tool_calls":[]},"logprobs":null,"finish_reason":"stop","stop_reason":null}],"usage":{"prompt_tokens":9,"total_tokens":518,"completion_tokens":509,"prompt_tokens_details":null},"prompt_logprobs":null}
```
:::
::::

## Deploying with KubeRay

Create a KubeRay cluster using the {ref}`Ray Serve LLM KubeRay guide <kuberay-rayservice-llm-example>` with sufficient GPU resources for DeepSeek R1. For example, two 8xH100 nodes.

Deploy DeepSeek-R1 as a RayService with the following configuration file:

```bash
kubectl apply -f https://raw.githubusercontent.com/ray-project/kuberay/master/ray-operator/config/samples/ray-service.deepseek.yaml
```

## Troubleshooting

### Multi-Node GPU Issues

Since DeepSeek typically requires multi-node GPU deployment, you may encounter issues specific to multi-node GPU serving. Common problems include:

* **NCCL initialization failures**: Especially on H100 instances due to outdated `aws-ofi-plugin` versions
* **Pipeline parallelism hangs**: When `pipeline_parallel_size > 1`, the model serving may hang due to resource conflicts

For comprehensive troubleshooting of multi-node GPU serving issues, refer to {ref}`Troubleshooting multi-node GPU serving on KubeRay <serve-multi-node-gpu-troubleshooting>`.
