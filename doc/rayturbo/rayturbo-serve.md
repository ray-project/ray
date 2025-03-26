<!--- These will get pulled into anyscale docs -->
# Ray Serve and RayTurbo Serve

[Ray Serve](https://docs.ray.io/en/latest/serve/index.html) is an open source library for scalable inference APIs.

- Framework-agnostic: Works with PyTorch, TensorFlow, Keras, Scikit-Learn models, or custom Python logic
- Specializes in model composition and multi-model serving in a single service
- LLM-optimized: Streaming responses, dynamic request batching, multi-node/GPU deployment
- Built on [Ray Core](https://docs.ray.io/en/latest/ray-core/walkthrough.html) for distributed scaling across machines
- Provides flexible scheduling support and performance optimizations

RayTurbo Serve provides improved production readiness and developer experience over open source Ray Serve, along with performance optimizations for large-scale workloads and cost savings through replica compaction and spot support such as:

- Fast autoscaling and model loading
- High QPS serving
- Replica compaction
- Zero-downtime incremental rollouts
- Observability
- Multi-AZ services
- Containerized runtime environments

## Fast autoscaling and model loading

RayTurbo Serve's [fast model loading capabilities](https://docs.anyscale.com/platform/services/fast-loading) and startup time optimizations improve auto-scaling and cluster startup capabilities. In certain [experiments](https://www.anyscale.com/blog/autoscale-large-ai-models-faster), the end-to-end scaling time for Llama-3-70B is 5.1x faster on Anyscale compared to open source Ray.

## High QPS serving

RayTurbo provides an optimized version of Ray Serve to achieve up to 54% higher QPS and up-to 3x streaming tokens per second for high traffic serving use-cases.

## Replica compaction

RayTurbo Serve migrates replicas into fewer nodes where possible to reduce resource fragmentation and improve hardware utilization. Replica compaction is enabled by default. Learn more [in this blog](https://www.anyscale.com/blog/new-feature-replica-compaction).

## Zero-downtime incremental rollouts

RayTurbo allows you to perform [incremental rollouts](https://docs.anyscale.com/platform/services/update-a-service#resource-constrained-updates) and canary upgrades for robust production service management. Unlike KubeRay and open source Ray Serve, RayTurbo performs upgrades with rollback procedures without requiring 2x the hardware capacity.

## Observability

RayTurbo provides custom metric dashboards, [log search](https://docs.anyscale.com/monitoring/accessing-logs), [tracing](https://docs.anyscale.com/monitoring/tracing), and [alerting](https://docs.anyscale.com/monitoring/custom-dashboards-and-alerting), for comprehensive observability into your production services. It also has the ability to [export](https://docs.anyscale.com/monitoring/exporting-logs) logs, metrics, and traces to your observability tooling like Datadog, etc.

## Multi-AZ services

RayTurbo enables availability-zone aware scheduling of Ray Serve replicas to provide higher redundancy to availability zone failures.

## Containerized runtime environments

RayTurbo configures [different container images](https://docs.anyscale.com/platform/services/multi-app#multiple-applications-in-different-containers) for different Ray Serve deployments allowing you to prepare dependencies needed per model separately. It comes with all the fast container optimizations included in fast auto-scaling as well as improved security posture over open source Ray Serve, since it doesn’t require installing Podman and running with root permissions.

## APIs

See the detailed guides for RayTurbo:

1. [Tracing](https://docs.anyscale.com/monitoring/tracing)

2. [Fast model loading](https://docs.anyscale.com/platform/services/fast-loading)

```python

from ray.anyscale.safetensors.torch import load_file

# IMPORTANT: Initialize the model with *empty weights*.
# When using your own `torch.nn.Module`, you can use torch.nn.utils.skip_init, see:
# https://pytorch.org/tutorials/prototype/skip_param_init.html
with init_empty_weights():
    model = MistralForCausalLM(
        MistralConfig.from_pretrained("mistralai/Mistral-7B-Instruct-v0.1", torch_dtype=torch.float16)
    )

# Download the model weights directly from the remote location "model_weights_uri" to the GPU.
state_dict: Dict[str, torch.Tensor] = load_file(model_weights_uri, device="cuda")

# Populate the weights in the model class.
self._model.load_state_dict(state_dict, assign=True)
```
