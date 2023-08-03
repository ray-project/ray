(serve-develop-and-deploy)=

# Develop and deploy an ML application

The flow for developing a Ray Serve application locally and deploying it in production covers the following steps:

* Converting a Machine Learning model into a Ray Serve application
* Testing the application locally
* Building Serve config files for production deployment
* Deploying applications using a config file

## Convert a model into a Ray Serve application

This example uses a text-translation model:

```{literalinclude} ../serve/doc_code/getting_started/models.py
:start-after: __start_translation_model__
:end-before: __end_translation_model__
:language: python
```

The Python file, called `model.py`, uses the `Translator` class to translate English text to French.

- The `self.model` variable inside the `Translator`'s `__init__` method
  stores a function that uses the [t5-small](https://huggingface.co/t5-small)
  model to translate text.
- When `self.model` is called on English text, it returns translated French text
  inside a dictionary formatted as `[{"translation_text": "..."}]`.
- The `Translator`'s `translate` method extracts the translated text by indexing into the dictionary.

Copy and paste the script and run it locally. It translates `"Hello world!"`
into `"Bonjour Monde!"`.

```console
$ python model.py

Bonjour Monde!
```

Converting this model into a Ray Serve application with FastAPI requires three changes:
1. Import Ray Serve and Fast API dependencies
2. Add decorators for Serve deployment with FastAPI: `@serve.deployment` and `@serve.ingress(app)`
3. `bind` the `Translator` deployment to the arguments that are passed into its constructor

For other HTTP options, see [Set Up FastAPI and HTTP](serve-set-up-fastapi-http). 

```{literalinclude} ../serve/doc_code/develop_and_deploy/model_deployment_with_fastapi.py
:start-after: __deployment_start__
:end-before: __deployment_end__
:language: python
```

Note that the code configures parameters for the deployment, such as `num_replicas` and `ray_actor_options`. These parameters help configure the number of copies of the deployment and the resource requirements for each copy. In this case, we set up 2 replicas of the model that take 0.2 CPUs and 0 GPUs each. For a complete guide on the configurable parameters on a deployment, see [Configure a Serve deployment](serve-configure-deployment).

## Test a Ray Serve application locally

To test locally, run the script with the `serve run` CLI command. This command takes in an import path formatted as `module:application`. Run the command from a directory containing a local copy of the script saved as `model.py`, so it can import the application:

```console
$ serve run model:translator_app
```

This command runs the `translator_app` application and then blocks streaming logs to the console. You can kill it with `Ctrl-C`, which tears down the application.

Now test the model over HTTP. Reach it at the following default URL:

```
http://127.0.0.1:8000/
```

Send a POST request with JSON data containing the English text. This client script requests a translation for "Hello world!":

```{literalinclude} ../serve/doc_code/develop_and_deploy/model_deployment_with_fastapi.py
:start-after: __client_function_start__
:end-before: __client_function_end__
:language: python
```

While a Ray Serve application is deployed, use the `serve status` CLI command to check the status of the application and deployment. For more details on the output format of `serve status`, see [Inspect Serve in production](serve-in-production-inspecting).

```console
$ serve status
name: default
app_status:
  status: RUNNING
  message: ''
  deployment_timestamp: 1687415211.531879
deployment_statuses:
- name: default_Translator
  status: HEALTHY
  message: ''
```

## Build Serve config files for production deployment

To deploy Serve applications in production, you need to generate a Serve config YAML file. A Serve config file is the single source of truth for the cluster, allowing you to specify system-level configuration and your applications in one place. It also allows you to declaratively update your applications. The `serve build` CLI command takes as input the import path and saves to an output file using the `-o` flag. You can specify all deployment parameters in the Serve config files.

```console
$ serve build model:translator_app -o config.yaml
```

The serve build command adds a default application name that can be modified. The resulting Serve config file is:

```
# This file was generated using the `serve build` command on Ray v2.5.1.
  
proxy_location: EveryNode

http_options:

  host: 0.0.0.0

  port: 8000

applications:

- name: app1

  route_prefix: /

  import_path: model:translator_app

  runtime_env: {}

  deployments:

  - name: Translator
    num_replicas: 2
    ray_actor_options:
      num_cpus: 0.2
      num_gpus: 0.0
```

You can also use the Serve config file with `serve run` for local testing. For example:

```console
$ serve run config.yaml
```

```console
$ serve status
name: app1
app_status:
  status: RUNNING
  message: ''
  deployment_timestamp: 1687630567.0700073
deployment_statuses:
- name: app1_Translator
  status: HEALTHY
  message: ''
```

For more details, see [Serve Config Files](serve-in-production-config-file).

## Deploy Ray Serve in production

Deploy the Ray Serve application in production on Kubernetes using the [KubeRay] operator. Copy the YAML file generated in the previous step directly into the Kubernetes configuration. KubeRay supports zero-downtime upgrades, status reporting, and fault tolerance for your production application. See [Deploying on Kubernetes](serve-in-production-kubernetes) for more information. For production usage, consider implementing the recommended practice of setting up [head node fault tolerance](serve-e2e-ft-guide-gcs).

## Monitor Ray Serve

Use the Ray Dashboard to get a high-level overview of your Ray Cluster and Ray Serve application's states. The Ray Dashboard is available both during local testing and on a remote cluster in production. Ray Serve provides some in-built metrics and logging as well as utilities for adding custom metrics and logs in your application. For production deployments, exporting logs and metrics to your observability platforms is recommended. See [Monitoring](serve-monitoring) for more details. 

[KubeRay]: https://ray-project.github.io/kuberay/
