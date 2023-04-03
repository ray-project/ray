# Deploying Multiple Serve Applications

In Ray 2.4+, deploying multiple independent Serve applications is supported. This user guide walks through how to generate a multi-application config file and deploy it using the Serve CLI, and monitor your applications using the CLI and the Ray Serve dashboard.

## When to Use Multiple Applications
It can be useful to deploy multiple independent applications on one Ray cluster. However, it is also important to keep in mind the downsides of doing so.

We suggest you deploy multiple applications per cluster if:
* You want to save cost by sharing resources between applications.
* You have many small independent models, and you want to be able to easily add and delete models often.

However, we suggest you limit to one application per cluster if:
* Your applications are large and require a lot of resources. Deploying multiple applications per cluster can cause competition of resources in this case.
* You want better isolation between applications. Although applications are independent of one another, isolation can't be guaranteed. Also, if you need to change the configuration of the cluster for one application, all other applications will also be affected. 


## Get Started

Define a Serve application:
```{literalinclude} doc_code/basic_calculator.py
:language: python
:start-after: __serve_example_begin__
:end-before: __serve_example_end__
```

Copy this to a file named `calculator.py`.

Define a second Serve application:
```{literalinclude} doc_code/basic_greet.py
:language: python
:start-after: __serve_example_begin__
:end-before: __serve_example_end__
```
Copy this to a file named `greet.py`.

Generate a multi-application config file that contains both of these two applications and save it to `config.yaml`.

```
serve build --multi-app calculator:app greet:app -o config.yaml
```

This generates the following config:
```yaml
proxy_location: EveryNode
http_options:
  host: 0.0.0.0
  port: 8000

applications:

- name: app1
  route_prefix: /calculator
  import_path: calculator:app
  runtime_env: {}
  deployments:
  - name: Multiplier
  - name: Adder
  - name: Router
  - name: DAGDriver

- name: app2
  route_prefix: /greet
  import_path: greet:app
  runtime_env: {}
  deployments:
  - name: greet
  - name: DAGDriver
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

Query the applications at their respective endpoints, `/calculator` and `/greet`.
```pycon
>>> requests.post("http://localhost:8000/calculator", json=["ADD", 5]).json()
7

>>> requests.post("http://localhost:8000/greet", json="Bob").json()
'Good morning Bob!'
```

### Check Status
Check the status of the applications by running `serve status`.

```console
$ serve status
name: app1
app_status:
  status: RUNNING
  message: ''
  deployment_timestamp: 1679969226.923282
deployment_statuses:
- name: app1_Multiplier
  status: HEALTHY
  message: ''
- name: app1_Adder
  status: HEALTHY
  message: ''
- name: app1_Router
  status: HEALTHY
  message: ''
- name: app1_DAGDriver
  status: HEALTHY
  message: ''

---

name: app2
app_status:
  status: RUNNING
  message: ''
  deployment_timestamp: 1679969226.923282
deployment_statuses:
- name: app2_greet
  status: HEALTHY
  message: ''
- name: app2_DAGDriver
  status: HEALTHY
  message: ''
```

:::{note} 
Notice that in the output of `serve status`, the prefix of each deployment name is the application name. At runtime, all deployments will have their corresponding application prepended to their names.
:::

### Inspect Deeper

For more visibility into the applications running on the cluster, go to the Ray Serve dashboard at [`http://localhost:8265/#/serve`](http://localhost:8265/#/serve).

You can see all applications that are deployed on the Ray cluster, the list of deployments under each application, as well as the list of replicas for each deployment.

![applications](https://raw.githubusercontent.com/ray-project/images/master/docs/serve/multi-app/applications-dashboard.png)

![deployments](https://raw.githubusercontent.com/ray-project/images/master/docs/serve/multi-app/deployments-dashboard.png)

![replicas](https://raw.githubusercontent.com/ray-project/images/master/docs/serve/multi-app/replica-dashboard.png)

For more details on the Ray Serve dashboard, see the [Serve dashboard documentation](dash-serve-view).

## Adding, Deleting, and Updating Applications
The config submitted to the cluster describes the target state for Ray Serve. Consequently, Ray Serve will add, remove, or update based on the list of applications in the config and the config options set for each application.
* To add an application, add a new entry under the `applications` field.
* To delete an application, remove the corresponding entry under the `applications` field.
* To update an application, modify the config options in the corresponding entry under the `applications` field.
Note that the update behavior for each application when a config is resubmitted is the same as the old single-application behavior. For how an application reacts to different config changes, see [updating a Serve application](serve-in-production-updating).

## New Multi-Application Config

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
  route_prefix: /calculator
  import_path: calculator:app
  runtime_env: {}
  deployments:
  - name: Multiplier
  - name: Adder
  - name: Router
  - name: DAGDriver

- name: app2
  route_prefix: /greet
  import_path: greet:app
  runtime_env: {}
  deployments:
  - name: greet
  - name: DAGDriver
```

(serve-config-migration)=
### Migrating from a Single-Application Config

Migrating the single-application config `ServeApplicationSchema` to the multi-application config format `ServeDeploySchema` is straightforward. Each entry under the  `applications` field matches the old, single-application config format. To convert a single-application config to the multi-application config format:
* Copy the entire old config to an entry under the `applications` field.
* Remove `host` and `port` from the entry and move them under the `http_options` field.
* Name the application.
* If you haven't already, set the application-level `route_prefix` to the route prefix of the ingress deployment in the application.
* When needed, add more applications.

For more details on the multi-application config format, see the documentation for [`ServeDeploySchema`](serve-rest-api-config-schema).

:::{note} 
You must remove `host` and `port` from the application entry. In a multi-application config, specifying cluster-level options within an individual application isn't applicable, and is not supported.
:::
