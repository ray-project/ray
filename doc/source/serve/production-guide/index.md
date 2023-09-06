(serve-in-production)=

# Production Guide

The recommended way to run Ray Serve in production is on Kubernetes using the [KubeRay](kuberay-quickstart) [RayService] custom resource.
The RayService custom resource automatically handles important production requirements such as health checking, status reporting, failure recovery, and upgrades.
If you're not running on Kubernetes, you can also run Ray Serve on a Ray cluster directly using the Serve CLI.

This section will walk you through a quickstart of how to generate a Serve config file and deploy it using the Serve CLI.
For more details, you can check out the other pages in the production guide:
- Understand the [Serve config file format](serve-in-production-config-file).
- Understand how to [deploy on Kubernetes using KubeRay](serve-in-production-kubernetes).
- Understand how to [monitor running Serve applications](serve-monitoring).

For deploying on VMs instead of Kubernetes, see [Deploy on VM](serve-in-production-deploying).

(serve-in-production-example)=

## Working example: Text Summarization and Translation Application

Throughout the production guide, we will use the following Serve application as a working example.
The application takes in a string of text in english, then summarizes and translates it into French (default), German, or Romanian.

```{literalinclude} ../doc_code/production_guide/text_ml.py
:language: python
:start-after: __example_start__
:end-before: __example_end__
```

Save this code locally in `text_ml.py` to follow along.
In development, we would likely use the `serve run` command to iteratively run, develop, and repeat (see the [Development Workflow](serve-dev-workflow) for more information).
When we're ready to go to production, we will generate a structured [config file](serve-in-production-config-file) that acts as the single source of truth for the application.

This config file can be generated using `serve build`:
```
$ serve build text_ml:app -o serve_config.yaml
```

The generated version of this file contains an `import_path`, `runtime_env`, and configuration options for each deployment in the application.
A minimal version of the config looks as follows (save this config locally in `serve_config.yaml` to follow along):

```yaml
proxy_location: EveryNode

http_options:
  host: 0.0.0.0
  port: 8000

applications:
- name: default
  route_prefix: /
  import_path: text_ml:app
  runtime_env: {}
  deployments:
  - name: Translator
    num_replicas: 1
  - name: Summarizer
    num_replicas: 1
```

You can use `serve deploy` to deploy the application to a local Ray cluster and `serve status` to get the status at runtime:

```console
# Start a local Ray cluster.
ray start --head

# Deploy the text ML application to the local Ray cluster.
serve deploy serve_config.yaml
2022-08-16 12:51:22,043 SUCC scripts.py:180 --
Sent deploy request successfully!
 * Use `serve status` to check deployments' statuses.
 * Use `serve config` to see the running app's config.

$ serve status
proxies:
  cef533a072b0f03bf92a6b98cb4eb9153b7b7c7b7f15954feb2f38ec: HEALTHY
applications:
  default:
    status: RUNNING
    message: ''
    last_deployed_time_s: 1694041157.2211847
    deployments:
      Translator:
        status: HEALTHY
        replica_states:
          RUNNING: 1
        message: ''
      Summarizer:
        status: HEALTHY
        replica_states:
          RUNNING: 1
        message: ''
```

You can test the application using `curl`:

```console
$ curl -H "Content-Type: application/json" -d '"It was the best of times, it was the worst of times, it was the age of wisdom, it was the age of foolishness, it was the epoch of belief"' "http://localhost:8000/"
c'était le meilleur des temps, c'était le pire des temps .
```

To update the application, modify the config file and use `serve deploy` again.

## Next Steps

For a deeper dive into how to deploy, update, and monitor Serve applications, see the following pages:
- Learn the details of the [Serve config file format](serve-in-production-config-file).
- Learn how to [deploy on Kubernetes using KubeRay](serve-in-production-kubernetes).
- Learn how to [build custom Docker images](serve-custom-docker-images) to use with KubeRay.
- Learn how to [monitor running Serve applications](serve-monitoring).

[KubeRay]: https://ray-project.github.io/kuberay/
[RayService]: https://ray-project.github.io/kuberay/guidance/rayservice/
