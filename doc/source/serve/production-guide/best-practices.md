(serve-best-practices)=

# Best practices in production

This section helps you:

* Understand best practices when operating Serve in production
* Learn more about managing Serve with the Serve CLI
* Configure your HTTP requests when querying Serve

## CLI best practices

This section summarizes the best practices for deploying to production using the Serve CLI:

* Use `serve run` to manually test and improve your Serve application locally.
* Use `serve build` to create a Serve config file for your Serve application.
    * For development, put your Serve application's code in a remote repository and manually configure the `working_dir` or `py_modules` fields in your Serve config file's `runtime_env` to point to that repository.
    * For production, put your Serve application's code in a custom Docker image instead of a `runtime_env`. See [this tutorial](serve-custom-docker-images) to learn how to create custom Docker images and deploy them on KubeRay.
* Use `serve status` to track your Serve application's health and deployment progress. See [the monitoring guide](serve-in-production-inspecting) for more info.
* Use `serve config` to check the latest config that your Serve application received. This is its goal state. See [the monitoring guide](serve-in-production-inspecting) for more info.
* Make lightweight configuration updates (e.g., `num_replicas` or `user_config` changes) by modifying your Serve config file and redeploying it with `serve deploy`.

(serve-best-practices-http-requests)=

## Client-side HTTP requests

Most examples in these docs use straightforward `get` or `post` requests using Python's `requests` library, such as:

```{literalinclude} ../doc_code/requests_best_practices.py
:start-after: __prototype_code_start__
:end-before: __prototype_code_end__
:language: python
```

This pattern is useful for prototyping, but it isn't sufficient for production. In production, HTTP requests should use:

* Retries: Requests may occasionally fail due to transient issues (e.g., slow network, node failure, power outage, spike in traffic, etc.). Retry failed requests a handful of times to account for these issues.
* Exponential backoff: To avoid bombarding the Serve application with retries during a transient error, apply an exponential backoff on failure. Each retry should wait exponentially longer than the previous one before running. For example, the first retry may wait 0.1s after a failure, and subsequent retries wait 0.4s (4 x 0.1), 1.6s, 6.4s, 25.6s, etc. after the failure.
* Timeouts: Add a timeout to each retry to prevent requests from hanging. The timeout should be longer than the application's latency to give your application enough time to process requests. Additionally, set an [end-to-end timeout](serve-performance-e2e-timeout) in the Serve application, so slow requests don't bottleneck replicas.

```{literalinclude} ../doc_code/requests_best_practices.py
:start-after: __production_code_start__
:end-before: __production_code_end__
:language: python
```

## Load shedding

When a request is sent to a cluster, it's first received by the Serve proxy, which then forwards it to a replica for handling using a {mod}`DeploymentHandle <ray.serve.handle.DeploymentHandle>`.
Replicas can handle up to a configurable number of requests at a time. Configure the number using the `max_ongoing_requests` option.
If all replicas are busy and cannot accept more requests, the request is queued in the {mod}`DeploymentHandle <ray.serve.handle.DeploymentHandle>` until one becomes available.

Under heavy load, {mod}`DeploymentHandle <ray.serve.handle.DeploymentHandle>` queues can grow and cause high tail latency and excessive load on the system.
To avoid instability, it's often preferable to intentionally reject some requests to avoid these queues growing indefinitely.
This technique is called "load shedding," and it allows the system to gracefully handle excessive load without spiking tail latencies or overloading components to the point of failure.

You can configure load shedding for your Serve deployments using the `max_queued_requests` parameter to the {mod}`@serve.deployment <ray.serve.deployment>` decorator.
This controls the maximum number of requests that each {mod}`DeploymentHandle <ray.serve.handle.DeploymentHandle>`, including the Serve proxy, will queue.
Once the limit is reached, enqueueing any new requests immediately raises a {mod}`BackPressureError <ray.serve.exceptions.BackPressureError>`.
HTTP requests will return a `503` status code (service unavailable).

The following example defines a deployment that emulates slow request handling and has `max_ongoing_requests` and `max_queued_requests` configured.

```{literalinclude} ../doc_code/load_shedding.py
:start-after: __example_deployment_start__
:end-before: __example_deployment_end__
:language: python
```

To test the behavior, send HTTP requests in parallel to emulate multiple clients.
Serve accepts `max_ongoing_requests` and `max_queued_requests` requests, and rejects further requests with a `503`, or service unavailable, status.

```{literalinclude} ../doc_code/load_shedding.py
:start-after: __client_test_start__
:end-before: __client_test_end__
:language: python
```

```bash
2024-02-28 11:12:22,287 INFO worker.py:1744 -- Started a local Ray instance. View the dashboard at http://127.0.0.1:8265
(ProxyActor pid=21011) INFO 2024-02-28 11:12:24,088 proxy 127.0.0.1 proxy.py:1140 - Proxy actor 15b7c620e64c8c69fb45559001000000 starting on node ebc04d744a722577f3a049da12c9f83d9ba6a4d100e888e5fcfa19d9.
(ProxyActor pid=21011) INFO 2024-02-28 11:12:24,089 proxy 127.0.0.1 proxy.py:1357 - Starting HTTP server on node: ebc04d744a722577f3a049da12c9f83d9ba6a4d100e888e5fcfa19d9 listening on port 8000
(ProxyActor pid=21011) INFO:     Started server process [21011]
(ServeController pid=21008) INFO 2024-02-28 11:12:24,199 controller 21008 deployment_state.py:1614 - Deploying new version of deployment SlowDeployment in application 'default'. Setting initial target number of replicas to 1.
(ServeController pid=21008) INFO 2024-02-28 11:12:24,300 controller 21008 deployment_state.py:1924 - Adding 1 replica to deployment SlowDeployment in application 'default'.
(ProxyActor pid=21011) WARNING 2024-02-28 11:12:27,141 proxy 127.0.0.1 544437ef-f53a-4991-bb37-0cda0b05cb6a / router.py:96 - Request dropped due to backpressure (num_queued_requests=2, max_queued_requests=2).
(ProxyActor pid=21011) WARNING 2024-02-28 11:12:27,142 proxy 127.0.0.1 44dcebdc-5c07-4a92-b948-7843443d19cc / router.py:96 - Request dropped due to backpressure (num_queued_requests=2, max_queued_requests=2).
(ProxyActor pid=21011) WARNING 2024-02-28 11:12:27,143 proxy 127.0.0.1 83b444ae-e9d6-4ac6-84b7-f127c48f6ba7 / router.py:96 - Request dropped due to backpressure (num_queued_requests=2, max_queued_requests=2).
(ProxyActor pid=21011) WARNING 2024-02-28 11:12:27,144 proxy 127.0.0.1 f92b47c2-6bff-4a0d-8e5b-126d948748ea / router.py:96 - Request dropped due to backpressure (num_queued_requests=2, max_queued_requests=2).
(ProxyActor pid=21011) WARNING 2024-02-28 11:12:27,145 proxy 127.0.0.1 cde44bcc-f3e7-4652-b487-f3f2077752aa / router.py:96 - Request dropped due to backpressure (num_queued_requests=2, max_queued_requests=2).
(ServeReplica:default:SlowDeployment pid=21013) INFO 2024-02-28 11:12:28,168 default_SlowDeployment 8ey9y40a e3b77013-7dc8-437b-bd52-b4839d215212 / replica.py:373 - __CALL__ OK 2007.7ms
(ServeReplica:default:SlowDeployment pid=21013) INFO 2024-02-28 11:12:30,175 default_SlowDeployment 8ey9y40a 601e7b0d-1cd3-426d-9318-43c2c4a57a53 / replica.py:373 - __CALL__ OK 4013.5ms
(ServeReplica:default:SlowDeployment pid=21013) INFO 2024-02-28 11:12:32,183 default_SlowDeployment 8ey9y40a 0655fa12-0b44-4196-8fc5-23d31ae6fcb9 / replica.py:373 - __CALL__ OK 3987.9ms
(ServeReplica:default:SlowDeployment pid=21013) INFO 2024-02-28 11:12:34,188 default_SlowDeployment 8ey9y40a c49dee09-8de1-4e7a-8c2f-8ce3f6d8ef34 / replica.py:373 - __CALL__ OK 3960.8ms
Request finished with status code 200.
Request finished with status code 200.
Request finished with status code 200.
Request finished with status code 200.
```
