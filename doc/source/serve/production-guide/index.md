(serve-in-production)=

# Production Guide

The recommended way to run Ray Serve in production is on Kubernetes using the [KubeRay](kuberay-quickstart) [RayService] custom resource.
The RayService custom resource automatically handles important production requirements such as health checking, status reporting, failure recovery, and upgrades.
If you're not running on Kubernetes, you can also run Ray Serve on a Ray cluster directly using the Serve CLI.

This section will walk you through a quickstart of how to generate a Serve config file and deploy it using the Serve CLI.
For more details, you can check out the other pages in the production guide:
- Understand the [Serve config file format](serve-in-production-config-file).
- Understand how to [deploy on VMs using the Serve CLI](serve-in-production-deploying).
- Understand how to [deploy on Kubernetes using KubeRay](serve-in-production-kubernetes).
- Understand how to [monitor running Serve applications](serve-monitoring).

(serve-in-production-example)=

## Working example: FruitStand application

Throughout the production guide, we will use the following Serve application as a working example.
The application takes in requests containing a list of two values, a fruit name and an amount, and returns the total price for the batch of fruits.

```{literalinclude} ../doc_code/production_fruit_example.py
:language: python
:start-after: __fruit_example_begin__
:end-before: __fruit_example_end__
```

Save this code locally in `fruit.py` to follow along.
In development, we would likely use the `serve run` command to iteratively run, develop, and repeat (see the [Development Workflow](serve-dev-workflow) for more information).
When we're ready to go to production, we will generate a structured [config file](serve-in-production-config-file) that acts as the single source of truth for the application.

This config file can be generated using `serve build`:
```
$ serve build fruit:deployment_graph -o fruit_config.yaml
```

The generated version of this file contains an `import_path`, `runtime_env`, and configuration options for each deployment in the application.
A minimal version of the config looks as follows (save this config locally in `fruit_config.yaml` to follow along):

```yaml
import_path: fruit:deployment_graph

runtime_env: {}

host: 0.0.0.0

port: 8000

deployments:

- name: MangoStand
  num_replicas: 2

- name: OrangeStand
  num_replicas: 1

- name: PearStand
  num_replicas: 1

- name: FruitMarket
  num_replicas: 2

- name: DAGDriver
  num_replicas: 1
```

You can use `serve deploy` to deploy the application to a local Ray cluster and `serve status` to get the status at runtime:

```console
# Start a local Ray cluster.
ray start --head

# Deploy the FruitStand application to the local Ray cluster.
serve deploy fruit_config.yaml
2022-08-16 12:51:22,043 SUCC scripts.py:180 --
Sent deploy request successfully!
 * Use `serve status` to check deployments' statuses.
 * Use `serve config` to see the running app's config.

$ serve status
app_status:
  status: RUNNING
  message: ''
  deployment_timestamp: 1660672282.0406542
deployment_statuses:
- name: MangoStand
  status: HEALTHY
  message: ''
- name: OrangeStand
  status: HEALTHY
  message: ''
- name: PearStand
  status: HEALTHY
  message: ''
- name: FruitMarket
  status: HEALTHY
  message: ''
- name: DAGDriver
  status: HEALTHY
  message: ''
```

You can test the application using `curl`:

```console
$ curl -H "Content-Type: application/json" -d '["PEAR", 2]' "http://localhost:8000/"
8
```

To update the application, modify the config file and use `serve deploy` again.

## Next Steps

This section provided a quickstart on how to generate and use a Serve config file.
For a deeper dive into how to deploy, update, and monitor Serve applications, see the following pages:
- Learn the details of the [Serve config file format](serve-in-production-config-file).
- Learn how to [deploy on VMs using the Serve CLI](serve-in-production-deploying).
- Learn how to [deploy on Kubernetes using KubeRay](serve-in-production-kubernetes).
- Learn how to [monitor running Serve applications](serve-monitoring).

[KubeRay]: https://ray-project.github.io/kuberay/
[RayService]: https://ray-project.github.io/kuberay/guidance/rayservice/
