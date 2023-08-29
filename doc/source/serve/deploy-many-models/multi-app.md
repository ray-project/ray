(serve-multi-application)=
# Deploy Multiple Applications

In Ray 2.4+, deploying multiple independent Serve applications is supported. This user guide walks through how to generate a multi-application config file and deploy it using the Serve CLI, and monitor your applications using the CLI and the Ray Serve dashboard.

## Context
### Background 
With the introduction of multi-application Serve, we walk you through the new concept of applications and when you should choose to deploy a single application versus multiple applications per cluster. 

An application consists of one or more deployments. The deployments in an application are tied into a direct acyclic graph through [model composition](serve-model-composition). An application can be called via HTTP at the specified route prefix, and the ingress deployment handles all such inbound traffic. Due to the dependence between deployments in an application, one application is a unit of upgrade. 

### When to Use Multiple Applications
Since one application is a unit of upgrade, having multiple applications allows you to deploy multiple models or groups of models that communicate with each other, while still being able to upgrade each application independently.

If you have many independent models each behind different endpoints, and you want to be able to easily add, delete, or upgrade these models, then you should use multiple applications. Each model should then be deployed as a separate application. On the other hand, if you have ML logic and business logic distributed among separate deployments that all need to be executed for a single request, then you should use model composition to build a single application consisting of multiple deployments.


## Get Started

Define a Serve application:
```{literalinclude} ../doc_code/image_classifier_example.py
:language: python
:start-after: __serve_example_begin__
:end-before: __serve_example_end__
```

Copy this to a file named `image_classifier.py`.

Define a second Serve application:
```{literalinclude} ../doc_code/translator_example.py
:language: python
:start-after: __serve_example_begin__
:end-before: __serve_example_end__
```
Copy this to a file named `text_translator.py`.

Generate a multi-application config file that contains both of these two applications and save it to `config.yaml`.

```
serve build --multi-app image_classifier:app text_translator:app -o config.yaml
```

This generates the following config:
```yaml
proxy_location: EveryNode

http_options:
  host: 0.0.0.0
  port: 8000

applications:
- name: app1
  route_prefix: /classify
  import_path: image_classifier:app
  runtime_env: {}
  deployments:
  - name: downloader
  - name: ImageClassifier

- name: app2
  route_prefix: /translate
  import_path: text_translator:app
  runtime_env: {}
  deployments:
  - name: Translator
```

:::{note} 
The names for each application are auto-generated as `app1`, `app2`, etc. To give custom names to the applications, modify the config file before moving on to the next step.
:::

### Deploy the Applications
To deploy the applications, be sure to start a Ray cluster first.

```console
$ ray start --head

$ serve deploy config.yaml
> Sent deploy request successfully!
```

Query the applications at their respective endpoints, `/classify` and `/translate`.
```pycon
>>> requests.post("http://localhost:8000/classify", json={"image_url": "https://cdn.britannica.com/41/156441-050-A4424AEC/Grizzly-bear-Jasper-National-Park-Canada-Alberta.jpg"}).text
'brown bear, bruin, Ursus arctos'

>>> requests.post("http://localhost:8000/translate", json={"text": "Hello, the weather is quite fine today!"}).text
'Hallo, das Wetter ist heute ziemlich gut!'
```

### Check Status
Check the status of the applications by running `serve status`.

```console
$ serve status
proxies:
  2e02a03ad64b3f3810b0dd6c3265c8a00ac36c13b2b0937cbf1ef153: HEALTHY
applications:
  classify:
    status: RUNNING
    message: ''
    last_deployed_time_s: 1693267064.0735464
    deployments:
      downloader:
        status: HEALTHY
        replica_states:
          RUNNING: 1
        message: ''
      ImageClassifier:
        status: HEALTHY
        replica_states:
          RUNNING: 1
        message: ''
  translate:
    status: RUNNING
    message: ''
    last_deployed_time_s: 1693267064.0735464
    deployments:
      Translator:
        status: HEALTHY
        replica_states:
          RUNNING: 1
        message: ''
```

### Send requests between applications
You can also make calls between applications without going through HTTP. Take the classifier and translator app above as an example. We can modify the `__call__` method of the `ImageClassifier` to check for another parameter in the HTTP request, and send requests to the translator application.

```
@serve.deployment
class ImageClassifier:
    ...
    async def __call__(self, req: starlette.requests.Request):
        req = await req.json()
        result = await self.classify(req["image_url"])

        if req.get("should_translate") is True:
            handle = serve.get_app_handle("app2")
            return handle.translate.remote(result)
        
        return result
```

Then, sending requests to the classifier application with the `should_translate` flag set to True:
```pycon
>>> requests.post("http://localhost:8000/classify", json={"image_url": "https://cdn.britannica.com/41/156441-050-A4424AEC/Grizzly-bear-Jasper-National-Park-Canada-Alberta.jpg", "should_translate": False}).text
'Braunbär, Bruin, Ursus arctos'
```


### Inspect Deeper

For more visibility into the applications running on the cluster, go to the Ray Serve dashboard at [`http://localhost:8265/#/serve`](http://localhost:8265/#/serve).

You can see all applications that are deployed on the Ray cluster:

![applications](https://raw.githubusercontent.com/ray-project/images/master/docs/serve/multi-app/applications-dashboard.png)

The list of deployments under each application:

![deployments](https://raw.githubusercontent.com/ray-project/images/master/docs/serve/multi-app/deployments-dashboard.png)

As well as the list of replicas for each deployment:

![replicas](https://raw.githubusercontent.com/ray-project/images/master/docs/serve/multi-app/replica-dashboard.png)

For more details on the Ray Serve dashboard, see the [Serve dashboard documentation](dash-serve-view).

### Development Workflow with `serve run`
You can also use the CLI command `serve run` to run and test your application easily, either locally or on a remote cluster. 
```console
$ serve run config.yaml
> 2023-04-04 11:00:05,901 INFO scripts.py:327 -- Deploying from config file: "config.yaml".
> 2023-04-04 11:00:07,505 INFO worker.py:1613 -- Started a local Ray instance. View the dashboard at http://127.0.0.1:8265
> 2023-04-04 11:00:09,012 SUCC scripts.py:393 -- Submitted deploy config successfully.
```

You can then query the applications in the same way:
```pycon
>>> requests.post("http://localhost:8000/classify", json={"image_url": "https://cdn.britannica.com/41/156441-050-A4424AEC/Grizzly-bear-Jasper-National-Park-Canada-Alberta.jpg"}).text
'brown bear, bruin, Ursus arctos'

>>> requests.post("http://localhost:8000/translate", json={"text": "Hello, the weather is quite fine today!"}).text
'Hallo, das Wetter ist heute ziemlich gut!'
```

The command `serve run` blocks the terminal, which allows logs from Serve to stream to the console. This helps you test and debug your applications easily. If you want to change your code, you can hit Ctrl-C to interrupt the command and shutdown Serve and all its applications, then rerun `serve run`.

:::{note}
`serve run` only has support for running multi-application config files. If you want to run applications directly without a config file `serve run` can only run one application at a time.
:::


## Adding, Deleting, and Updating Applications
You can add or remove entries under the `applications` field to add or remove applications from the cluster. This will not affect other applications on the cluster. To update an application, modify the config options in the corresponding entry under the `applications` field.

:::{note}
The update behavior for an application when a config is resubmitted is the same as the old single-application behavior. For how an application reacts to different config changes, see [Updating a Serve Application](serve-inplace-updates).
:::

## Sending requests to applications using Serve handle
Sometimes, you may want to send a request to an application without using HTTP. For instance, if you want to invoke one application from within another Serve application, it would be inefficient to send the request through HTTP.

For this situation, you can use the Serve API `serve.get_app_handle` to get a handle to any live Serve application. This handle can be used to directly execute a request on an application. For instance


## Multi-Application Config

Use the config from the above tutorial as an example. In a multi-application config, the first section is for cluster-level config options:
```yaml
proxy_location: EveryNode
http_options:
  host: 0.0.0.0
  port: 8000
```

Then, specify a list of applications to deploy to the Ray cluster. Each application must have a unique name and route prefix.
```yaml
applications:
- name: app1
  route_prefix: /classify
  import_path: image_classifier:app
  runtime_env: {}
  deployments:
  - name: downloader
  - name: ImageClassifier

- name: app2
  route_prefix: /translate
  import_path: text_translator:app
  runtime_env: {}
  deployments:
  - name: Translator
```

(serve-config-migration)=
### Migrating from a Single-Application Config

Migrating the single-application config `ServeApplicationSchema` to the multi-application config format `ServeDeploySchema` is straightforward. Each entry under the  `applications` field matches the old, single-application config format. To convert a single-application config to the multi-application config format:
* Copy the entire old config to an entry under the `applications` field.
* Remove `host` and `port` from the entry and move them under the `http_options` field.
* Name the application.
* If you haven't already, set the application-level `route_prefix` to the route prefix of the ingress deployment in the application. In a multi-application config, you should set route prefixes at the application level instead of for the ingress deployment in each application.
* When needed, add more applications.

For more details on the multi-application config format, see the documentation for [`ServeDeploySchema`](serve-rest-api-config-schema).

:::{note} 
You must remove `host` and `port` from the application entry. In a multi-application config, specifying cluster-level options within an individual application isn't applicable, and is not supported.
:::
