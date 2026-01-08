# Agentic Batch Inference



This tutorial explores common use cases for agentic applications in offline settings using [Ray Data](https://docs.ray.io/en/latest/data/data.html), [Ray Serve](https://docs.ray.io/en/latest/serve/index.html), and [Ray Serve LLM](https://docs.ray.io/en/latest/serve/llm/index.html), covering fundamental patterns such as parallelization and conditional routing, as well as advanced orchestration techniques like multi-turn rollouts and scaling individual components. Ray’s modular and composable libraries make it straightforward to build and orchestrate sophisticated agentic workflows.


## Common Agentic Application Patterns

### Parallelization
Parallelization refers to the pattern which a task is decomposed into multiple independent subtasks that can be processed concurrently. Each subtask is sent to a separate agent with its own prompt or goal, and the results are combined once all parallel executions complete.
<div align="center">
  <img src="https://air-example-data.s3.us-west-2.amazonaws.com/rayllm-ossci/doc-assets/agentic_batch_inference_parallelization.png" width=600>
</div>
Complete example: [agentic_batch_parallel.py](https://github.com/ray-project/ray/blob/master/doc/source/ray-overview/examples/agentic-batch-inference/content/agentic_batch_parallel.py)  
   Demonstrates a how to deploy and run agents in parallel in offline batch workloads.

### Conditional Routing
Conditional routing incorporates a first-stage model evaluates the incoming input and decides how it should be handled. Based on this decision, the request is forwarded to a specialized agent or model that's best suited for the task.
<div align="center">
  <img src="https://air-example-data.s3.us-west-2.amazonaws.com/rayllm-ossci/doc-assets/agentic_batch_inference_routing.png" width=600>
</div>
Complete example: [agentic_batch_conditional_routing.py](https://github.com/ray-project/ray/blob/master/doc/source/ray-overview/examples/agentic-batch-inference/content/agentic_batch_conditional_routing.py)
   Shows how to build to create conditional logic with agents in offline batch workloads.

### Multi-Turn Rollout
Finally, with Ray Data and Ray Serve LLM, you can orchestrate complex agentic patterns easily. Multi-turn rollout is a widely used pattern where an agent iteratively reasons over a task across multiple steps, deciding at each turn whether to invoke tools, request additional information, or continue reasoning. At every step, the agent produces a structured action, such as a tool call, executes it, and incorporates the result before proceeding to the next turn. This process continues until a stopping condition is reached.

<div align="center">
  <img src="https://air-example-data.s3.us-west-2.amazonaws.com/rayllm-ossci/doc-assets/agentic_batch_inference_multiturn_rollout.png" width=600>
</div>
Complete example: [agentic_batch_multi_turn_rollout.py](https://github.com/ray-project/ray/blob/master/doc/source/ray-overview/examples/agentic-batch-inference/content/agentic_batch_multi_turn_rollout.py)  
   Guides you through creating agents, complex multi-turn rollout with tool calling workflows, and integrating with self-hosted MCP servers in offline batch workloads.

The aforementioned patterns are just basic building blocks. You can mix, match, or modify them to design more advanced or custom agentic workflows that fit your application needs.
