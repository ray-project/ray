# Deploying Multiple Serve Applications

In Ray 2.4+, deploying multiple independent Serve applications is supported. This user guide will walk you through how to generate a multi-application config file and deploy it using the Serve CLI, and monitor your applications using the CLI and the Ray Serve dashboard.

## Get Started

Define a Serve application:
```{literalinclude} doc_code/basic_calculator.py
:language: python
```

Copy this to a file `calculator.py`.

Define a second Serve application:
```{literalinclude} doc_code/basic_greet.py
:language: python
```
Copy this to a file `greet.py`.

Generate a multi-application config file that contains both of these two applications and save it to `config.yaml`.

```
serve build --multi-app calculator:app greet:app -o config.yaml
```

This should generate the following config:
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
The names for each application are auto-generated as `app1`, `app2`, etc. If you want to give custom names for the applications, modify the config file before moving on to the next step.
:::

### Deploy it
Now let's deploy the applications. Make sure to start a Ray cluster first.

```console
$ ray start --head

$ serve deploy config.yaml
> Sent deploy request successfully!
```

Query the applications at their respective endpoints `/calculator` and `/greet`.
```pycon
>>> requests.post("http://localhost:8000/calculator", json=["ADD", 5]).json()
7

>>> requests.post("http://localhost:8000/greet", json="Bob").json()
'Good morning Bob!'
```

### Check the status
You can check the status of the applications by running `serve status`.

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
Notice that in the output of `serve status`, each deployment name has the application name as a prefix. At runtime, all deployments will have the application to which they belong prepended to their names.
:::

## Inspecting Deeper

If you want more visibility into the applications running on the cluster, go to the Ray Serve dashboard at [`http://localhost:8265/#/serve`](http://localhost:8265/#/serve).

### Serve Overview
The top level view shows cluster-level information about Serve:

![serve-overview](https://raw.githubusercontent.com/ray-project/images/master/docs/serve/multi-app/system-level-options-dashboard.png)

As well as all applications that are deployed on the Ray cluster:

![applications](https://raw.githubusercontent.com/ray-project/images/master/docs/serve/multi-app/applications-dashboard.png)

### Application and its deployments
Click into the first application, `app1`. You will see an overview of the application:

![application-overview](https://raw.githubusercontent.com/ray-project/images/master/docs/serve/multi-app/application-overview-dashboard.png)

As well as the list of deployments under that application:

![deploymentsd](https://raw.githubusercontent.com/ray-project/images/master/docs/serve/multi-app/deployments-dashboard.png)

### Replicas
Click the drop down arrow for the first deployment, `Adder`. You will see the list of replicas for that deployment:

![replicas](https://raw.githubusercontent.com/ray-project/images/master/docs/serve/multi-app/replica-dashboard.png)

Clicking into one of those replicas will then show you an overview of the replica as well as a list of actor tasks run on the replica actor:

![replica-overview](https://raw.githubusercontent.com/ray-project/images/master/docs/serve/multi-app/replica-overview-dashboard.png)

## New Multi-Application Config

Let's use the config in the tutorial above as an example. In a multi-application config, the first section is cluster-level config options:
```yaml
proxy_location: EveryNode
http_options:
  host: 0.0.0.0
  port: 8000
```

Then, there is a list of applications to deploy to the Ray cluster. Each application must have a unique name and route prefix.
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
### Migrating from Single-Application Config

The multi-application config format `ServeDeploySchema` is easy to migrate to from the single-application config `ServeApplicationSchema`. Each entry under the  `applications` field matches the old, single-application config format. So, to convert an old config to the new config format:
* Copy the entire old config to an entry under the `applications` field.
* Remove `host` and `port` from the entry and move it under the `http_options` field.
* Name the application.
* If you haven't already, set the application-level `route_prefix` to the route prefix of the ingress deployment in the application.
* When needed, add more applications!

For more details on the multi-application config format, see the documentation for [`ServeDeploySchema`](serve-rest-api-config-schema).

:::{note} 
It is required to remove `host` and `port` from the application entry. Within a multi-application config, specifying cluster-level options within an individual application doesn't make sense, and is thus not allowed.
:::