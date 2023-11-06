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

## Best practices for HTTP requests

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
