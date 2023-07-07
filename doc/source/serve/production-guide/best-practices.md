(serve-best-practices)=

# Best practices

This section summarizes the best practices when deploying to production using the Serve CLI:

* Use `serve run` to manually test and improve your deployment graph locally.
* Use `serve build` to create a Serve config file for your deployment graph.
    * Put your deployment graph's code in a remote repository and manually configure the `working_dir` or `py_modules` fields in your Serve config file's `runtime_env` to point to that repository.
* Use `serve deploy` to deploy your graph and its deployments to your Ray cluster. After the deployment is finished, you can start serving traffic from your cluster.
* Use `serve status` to track your Serve application's health and deployment progress.
* Use `serve config` to check the latest config that your Serve application received. This is its goal state.
* Make lightweight configuration updates (e.g. `num_replicas` or `user_config` changes) by modifying your Serve config file and redeploying it with `serve deploy`.
* Make heavyweight code updates (e.g. `runtime_env` changes) by starting a new Ray cluster, updating your Serve config file, and deploying the file with `serve deploy` to the new cluster. Once the new deployment is finished, switch your traffic to the new cluster.

