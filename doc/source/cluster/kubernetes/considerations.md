(considerations-build-ml-platform)=

## Considerations for Building a Ray ML Platform

Ray is a core part of ML platforms and offers foundational capabilities for parallel workload execution like scheduling, management of stateless and stateful tasks, and a distributed object store. However, a production ML platform requires many other capabilities that are beyond the scope of Ray such as:

1. *Resource management* How do you provision the right resources efficiently across workloads and teams of users.
1. *Integrated developer tooling* How do you provide users of the system the right tools for using the ML platform
1. *Observability* How do you proactively and retroactively view and understand the system
1. *Governance* How do you ensure security and track costs of a platform.

The effort to implement and operate these capabilities in production is signficant

## Choosing a Ray Operator 

We recommend using an implementation of the Kubernetes [operator pattern](https://kubernetes.io/docs/concepts/extend-kubernetes/operator/) when deploying Ray in production. Available options for operators are [KubeRay](https://github.com/ray-project/kuberay) (self-managed) and [Anyscale operator](https://docs.anyscale.com/administration/cloud-deployment/kubernetes/) (managed service).


When deciding between a self-managed ML platform using KubeRay and other open-source tooling versus a managed service like Anyscale, there are important trade-offs to consider. The self-managed approach offers greater control, allowing teams to customize infrastructure, integrate with existing systems, and fine-tune configurations to their needs. It may also reduce vendor lock-in and provide cost advantages at scale if operated efficiently. However, it comes with significant operational overhead, including cluster management, autoscaling, observability, and security hardening. See [Uber's Journey to Ray on Kubernetes: Ray Setup](https://www.uber.com/blog/ubers-journey-to-ray-on-kubernetes-ray-setup/). In contrast, Anyscale’s managed service abstracts much of this complexity by providing a production-ready environment with native support for Ray, enterprise-grade features, and integrated developer experience. See [How Canva Built a Modern AI Platform Using Anyscale](https://www.anyscale.com/resources/case-study/how-canva-built-a-modern-ai-platform-using-anyscale)The choice ultimately depends on team expertise, resource constraints, and strategic priorities.
