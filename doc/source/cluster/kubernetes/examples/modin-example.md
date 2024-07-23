(kuberay-modin-example)=

# Use Modin with Ray on Kubernetes

This example runs a modified version of the [Using Modin with the NYC Taxi Dataset](https://github.com/modin-project/modin/blob/4e7afa7ea59c7a160ed504f39652ff23b4d49be3/examples/jupyter/Modin_Taxi.ipynb) example from the Modin official repository using RayJob on Kubernetes.

## Step 1: Install KubeRay operator

Follow steps 1 and 2 from [RayCluster QuickStart](kuberay-raycluster-quickstart) guide to install KubeRay operator.

## Step 2: Run the Modin example with RayJob

Create a RayJob that runs the Modin example using the following command:

```sh
kubectl apply -f https://raw.githubusercontent.com/ray-project/kuberay/master/ray-operator/config/samples/ray-job.modin.yaml
```

## Step 3: Check the output

Run the following command to check the output:

```sh
kubectl logs -l=job-name=rayjob-sample

# [Example output]
# 2024-07-05 10:01:00,945 INFO worker.py:1446 -- Using address 10.244.0.4:6379 set in the environment variable RAY_ADDRESS
# 2024-07-05 10:01:00,945 INFO worker.py:1586 -- Connecting to existing Ray cluster at address: 10.244.0.4:6379...
# 2024-07-05 10:01:00,948 INFO worker.py:1762 -- Connected to Ray cluster. View the dashboard at 10.244.0.4:8265
# Modin Engine: Ray
# FutureWarning: DataFrame.applymap has been deprecated. Use DataFrame.map instead.
# Time to compute isnull: 0.065887747972738
# Time to compute rounded_trip_distance: 0.34410698304418474
# 2024-07-05 10:01:23,069 SUCC cli.py:60 -- -----------------------------------
# 2024-07-05 10:01:23,069 SUCC cli.py:61 -- Job 'rayjob-sample-zt8wj' succeeded
# 2024-07-05 10:01:23,069 SUCC cli.py:62 -- -----------------------------------
```
