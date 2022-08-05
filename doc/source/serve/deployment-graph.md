(serve-deployment-graph)=

# Deployment Graph

## Intro to Deployment Graphs

Ray Serve’s Deployment Graph offers a simple API to compose your Ray Serve deployments together by declaring how to route a request through them. This is particularly useful when using ML model composition or mixing business logic and model inference in your application. Encapsulating each of your models and each of your business logic steps in independent deployments gives you added flexibility to update or scale individual deployments.

To understand deployment graph key concepts, follow [this guide](serve-model-composition).

## Deployment Graph Walkthrough

A typical production serve deployment does not exist in isolation. Normally, it's a collection of deployments. To study these composite and end-to-end deployments, follow [this end-to-end tutorial](./deployment-graph/deployment_graph_e2e_tutorial.md).

(serve-deployment-graph-patterns)=
## Patterns

There are various common deployments patterns used models in production. These patterns allow you to compose a single deployment graph. Below are a few examples how to construct them:

- [Chain nodes with same class and different args](deployment-graph/chain_nodes_same_class_different_args.md)
- [Combine two nodes with passing same input in parallel](deployment-graph/combine_two_nodes_with_passing_input_parallel.md)
- [Control flow based on user inputs](deployment-graph/control_flow_based_on_user_inputs.md)
- [Visualize DAG during development](deployment-graph/visualize_dag_during_development.md)
- [Http endpoint for dag graph](deployment-graph/http_endpoint_for_dag_graph.md)
