(kuberay-troubleshootin-guides)=

# Troubleshooting guide

This document addresses common inquiries.
If you don't find an answer to your question here, please don't hesitate to connect with us via our [community channels](https://github.com/ray-project/kuberay#getting-involved).

# Contents

- [Upgrade KubeRay](#upgrade-kuberay)
- [Worker init container](#worker-init-container)
- [Cluster domain](#cluster-domain)
- [RayService](#rayservice)
- [Other questions](#questions)

## Upgrade KubeRay

If you have issues upgrading KubeRay, refer to the [upgrade guide](#kuberay-upgrade-guide).
Most issues are about the CRD version.

(worker-init-container)=
## Worker init container

The KubeRay operator injects a default [init container](https://kubernetes.io/docs/concepts/workloads/pods/init-containers/) into every worker Pod.
This init container is responsible for waiting until the Global Control Service (GCS) on the head Pod is ready before establishing a connection to the head.
The init container will use `ray health-check` to check the GCS server status continuously.

The default worker init container may not work for all use cases, or users may want to customize the init container.

### 1. Init container troubleshooting

Some common causes for the worker init container to stuck in `Init:0/1` status are:

* The GCS server process has failed in the head Pod. Please inspect the log directory `/tmp/ray/session_latest/logs/` in the head Pod for errors related to the GCS server.
* The `ray` executable is not included in the `$PATH` for the image, so the init container will fail to run `ray health-check`.
* The `CLUSTER_DOMAIN` environment variable is not set correctly. See the section [cluster domain](#cluster-domain) for more details.
* The worker init container shares the same ***ImagePullPolicy***, ***SecurityContext***, ***Env***, ***VolumeMounts***, and ***Resources*** as the worker Pod template. Sharing these settings is possible to cause a deadlock. See [#1130](https://github.com/ray-project/kuberay/issues/1130) for more details.

If the init container remains stuck in `Init:0/1` status for 2 minutes, we will stop redirecting the output messages to `/dev/null` and instead print them to the worker Pod logs.
To troubleshoot further, you can inspect the logs using `kubectl logs`.

### 2. Disable the init container injection

If you want to customize the worker init container, you can disable the init container injection and add your own.
To disable the injection, set the `ENABLE_INIT_CONTAINER_INJECTION` environment variable in the KubeRay operator to `false` (applicable from KubeRay v0.5.2).
Please refer to [#1069](https://github.com/ray-project/kuberay/pull/1069) and the [KubeRay Helm chart](https://github.com/ray-project/kuberay/blob/ddb5e528c29c2e1fb80994f05b1bd162ecbaf9f2/helm-chart/kuberay-operator/values.yaml#L83-L87) for instructions on how to set the environment variable.
Once disabled, you can add your custom init container to the worker Pod template.

(cluster-domain)=
## Cluster domain

In KubeRay, we use Fully Qualified Domain Names (FQDNs) to establish connections between workers and the head.
The FQDN of the head service is `${HEAD_SVC}.${NAMESPACE}.svc.${CLUSTER_DOMAIN}`.
The default [cluster domain](https://kubernetes.io/docs/tasks/administer-cluster/dns-custom-nameservers/#introduction) is `cluster.local`, which works for most Kubernetes clusters.
However, it's important to note that some clusters may have a different cluster domain.
You can check the cluster domain of your Kubernetes cluster by checking `/etc/resolv.conf` in a Pod.

To set a custom cluster domain, adjust the `CLUSTER_DOMAIN` environment variable in the KubeRay operator.
Helm chart users can make this modification [here](https://github.com/ray-project/kuberay/blob/ddb5e528c29c2e1fb80994f05b1bd162ecbaf9f2/helm-chart/kuberay-operator/values.yaml#L88-L91).
For more information, please refer to [#951](https://github.com/ray-project/kuberay/pull/951) and [#938](https://github.com/ray-project/kuberay/pull/938) for more details.

(rayservice)=
## RayService

RayService is a Custom Resource Definition (CRD) designed for Ray Serve. In KubeRay, creating a RayService will first create a RayCluster and then
create Ray Serve applications once the RayCluster is ready. If the issue pertains to the data plane, specifically your Ray Serve scripts
or Ray Serve configurations (`serveConfigV2`), troubleshooting may be challenging. See [rayservice-troubleshooting](kuberay-raysvc-troubleshoot) for more details.

(questions)=
## Questions

### Why are changes to the RayCluster or RayJob CR not taking effect?

Currently, only modifications to the `replicas` field in `RayCluster/RayJob` CR are supported. Changes to other fields may not take effect or could lead to unexpected results.
