(security)=

# Security 

Ray is an easy-to-use framework to run arbitrary code across one or more nodes in a Ray Cluster. Ray provides fault-tolerance, optimized scheduling, task orchestration, and auto-scaling to run a given workload.

To achieve performant and distributed workloads, Ray components require intra-cluster communication. This communication includes central tenets like distributed memory and node-heartbeats, as well as auxiliary functions like metrics and logs. Ray leverages gRPC for a majority of this communication.

Ray offers additional services to improve the developer experience. These services include Ray Dashboard (to allow for cluster introspection and debugging), Ray Jobs (hosted alongside the Dashboard, which services Ray Job submissions), and Ray Client (to allow for local, interactive development with a remote cluster). These services provide complete access to the Ray Cluster and the underlying compute resources.

:::{admonition} Ray allows any clients to run arbitrary code. Be extremely careful about what is allowed to access your Ray Cluster
:class: caution

If you expose these services (Ray Dashboard, Ray Jobs, Ray Client), anybody
who can access the associated ports can execute arbitrary code on your Ray Cluster. This can happen:
* Explicitly: By submitting a Ray Job, or using the Ray Client
* Indirectly: By calling the Dashboard REST APIs of these services
* Implicitly: Ray extensively uses cloudpickle for serialization of arbitrary python objects. See [the pickle documentation](https://docs.python.org/3/library/pickle.html) for more details on Pickle's security model.

The Ray Dashboard, Ray Jobs and Ray Client are developer tools that you should
only use with the necessary access controls in place to restrict access to trusted parties only.
:::

## Personas

When considering the security responsibilities of running Ray, think about the different personas interacting with Ray.
* **Ray Developers** write code that relies on Ray. They either run a single-node Ray Cluster locally or multi-node Clusters remotely on provided compute infrastructure.
* **Platform providers** provide the compute environment on which **Developers** run Ray.
* **Users** interact with the output of Ray-powered applications.

## Best practices
**Security and isolation must be enforced outside of the Ray Cluster.** Ray expects to run in a safe network environment and to act upon trusted code. Developers and platform providers must maintain the following invariants to ensure the safe operation of Ray Clusters.

### Deploy Ray Clusters in a controlled network environment
* Network traffic between core Ray components and additional Ray components should always be in a controlled, isolated network. Access to additional services should be gated with strict network controls and/or external authentication/authorization proxies.
* gRPC communication can be encrypted with TLS, but it is not a replacement for network isolation.
* Platform providers are responsible for ensuring that Ray runs in sufficiently controlled network environments and that developers can access features like Ray Dashboard in a secure manner.
### Only execute trusted code within Ray
* Ray faithfully executes code that is passed to it – Ray doesn’t differentiate between a tuning experiment, a rootkit install, or an S3 bucket inspection.
* Ray developers are responsible for building their applications with this understanding in mind.
### Enforce isolation outside of Ray with multiple Ray Clusters
* If workloads require isolation from each other, use separate, isolated Ray Clusters. Ray can schedule multiple distinct Jobs in a single Cluster, but doesn't attempt to enforce isolation between them. Similarly, Ray doesn't implement access controls for developers interacting with a given cluster.
* Ray developers are responsible for determining which applications need to be separated and platform providers are responsible for providing this isolation.
