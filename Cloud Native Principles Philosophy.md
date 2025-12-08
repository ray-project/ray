# Cloud Native Principles

Authors: [Sumeet Khirwal](mailto:sumeet@anyscale.com) [Sridhar Konduri](mailto:sridhar@anyscale.com)  
Date: Nov 28, 2025

Ref:   
[Cloud Native Principles by Google](https://cloud.google.com/blog/products/application-development/5-principles-for-cloud-native-architecture-what-it-is-and-how-to-master-it)  
[What is Cloud Native by Amazon ?](https://aws.amazon.com/what-is/cloud-native/)

The goal of this document is to define Cloud Native Principles that we will be following at Anyscale.

Understanding what is the meaning of cloud native. We can summarise cloud native as per following points : 

1. Software Engineering approach to manage building and deployments of software on Cloud Infrastructure  
2. [CNCF](https://www.cncf.io/) is responsible for boosting development of Open Source Cloud Native applications  
3. Goal is to increase speed, agility, scalability, and resilience in software development and delivery

When we talk about being cloud native, It is driven by following principles primarily : 

1. Automation  
2. Scale  
3. Resiliency  
4. Security  
5. Extensibility / Future Proofing

# Automation : Everything should be automated 

This is a very focused mindset that we should have while architecting any Cloud Native solution. Automation can be simplified as “Zero Intervention by Engineers”.

**What are the key areas for automation ?**

1. **CICD**

When we talk about CICD, it implies how we take all changes being done across the team to production. CICD has various phases : 

1. Integration of all the deliveries by engineers across the team into one product.   
   At Anyscale, This is represented by Development/Premerge to Master  
   2. Ensuring that we have stable build post integration. 

   At Anyscale, This is represented by deployment to Staging

   3. To be added  
2. **Observability & Debugging**  
   1. Having right metrics in place to know the state of system  
   2. Automation via alerts to know if something is not right  
   3. Take automated actions based on alerts wherever possible  
3. **Self Service Operations**  
   1. No actions in production systems should be manual. Example :   
      1. Tenant Offboarding should be automated  
      2. Db Surgeries to be automated  
4. **Infrastructure**  
   We should use right tools like Terraform for automating deployments  
5. **Autoscaling**  
   1. Implement metric-based auto-scaling (HPA for services, managed scaling for infra/observability)  
   2. No manual intervention needed (Ex : PVC should also autoscale)

# Scale : System without limitations

This is one of the key guiding principles of any cloud native solution. Easier said than done, There are certain rules to keep in mind to ensure we can scale : 

1. **State Management** : The most common mistake we make is not knowing how to manage state in a Cloud Native Solution. Certain things to keep in mind :   
   1. Every service should be stateless. No In Memory State should be present.  
   2. Use external databases or caches  
2. **Managed Services** : Prefer to use managed services over self hosted for 2 reasons :   
   1. Managed Service Providers have one job to do which is focus on how to scale an infra like Kafka or Postgres. They are SME (Subject Matter Experts) like we are for Ray.  
   2. Reduce Operational Overhead. Operation Cost in a larger frame of reference is low compared to building a team around it.  
3. **Avoid Coupling between Microservices**  
   1. If you have 2 microservices which are interacting with each other, there should be no coupling between them.  
   2. Individual services should have their own identity and define their responsibility. We will talk about “How to define a Microservice” later

# Resiliency : Self Healing Systems

Every system fails at a certain point. But handling failovers and being resilient is important in the Cloud Native paradigm. Certain rules to ensure building a resilient system : 

1. **External Dependencies** : Identifying the point of failures in a system and adding support for how we handle failure scenarios. Key pointers to remember  :   
   1. External Dependencies that are temporarily unavailable. There can be 2 kinds of external dependencies : Hard Dependencies like Databases or Soft Dependencies like Cache.   
      1. Do not block operations on Soft Dependencies like Cache  
      2. Have proper retry mechanism for Hard Dependencies by defining SLAs which includes timeouts and an upper limit on number of retries  
   2. Define following  errors that are possible in your service :   
      1. Recoverable errors like throttling  
      2. Non recoverable errors like Invalid Token. Alert in such scenarios. Provide a mechanism to recover from “Non Recoverable Errors” manually.  
2. **Idempotency** : Pods/Services can restart in the middle of an operation due to errors like Node going down. Ensure idempotency if the same request or message is processed again, the same outcome is produced.  
3. **Breaking down operations** : We should break down a larger operation into smaller operations / transactions. And have retry on each of the operations.  
4. Implement circuit breakers for the services which are temporarily unavailable.  
5. **Prioritise Asynchronous over Synchronous Architecture**   
   1. Event Driven Architecture gives flexibility to build a highly fault tolerant system  
   2. Trade off will be that we have an eventually consistent system

# Security : Practice Defence in Depth

With the shift into the cloud world, Security has taken the center stage. Few things to consider from security landscape : 

1. Network Layer Security  
   1. Perimeter Security using Firewall as first line of defense  
   2. Application Firewalls  
2. Application Level Security  
   1. End Point Security  
   2. Authentication and Authorization  
3. Data Layer Security  
   1. Data Encryption  
   2. Secret Management  
   3. Data Isolation by Tenant  
   4. Limit access to Customer data  
4. Deployment Layer Security  
   1. Isolate 2 tenants to limit breach / contain issues

In a cloud native architecture, multiple customers share the same application, infra and resources. Isolation becomes a key strategy that needs to be defined based on table below : 

| Layer | Cloud-Native Isolation Techniques |
| :---- | :---- |
| **Application Logic** | Use a **Tenant ID** in all requests and application logic to strictly partition data access. |
| **Container/Compute** | Use **Kubernetes Namespaces** to logically separate and limit resources for different tenant workloads. |
| **Networking** | Implement **Network Policies** (in Kubernetes) and **VPC/Subnet Segmentation** to restrict communication between tenant-specific resources. |
| **Data/Storage** | \* **Silo Model:** Dedicated database/storage (highest isolation). \* **Bridge Model:** Shared database instance with dedicated **schemas** per tenant. \* **Pool Model:** Shared database/tables with strict **row-level security (RLS)** using the Tenant ID (highest cost-efficiency). |
|  |  |
|  |  |
| **Security** | Utilize **Identity and Access Management (IAM)** and **Role-Based Access Control (RBAC)** to ensure users can only access resources belonging to their specific tenant. |

# Extensibility : Architecture for Future Proofing

Adopt “Always be architecting” mindset  
The cloud is evolving at a rapid rate and so are the technologies around it. We should keep upgrading the technologies and the architecture as well.

Note: Multi Tenancy is a by-product of Cloud Native Architecture

# Principles to follow

**P1.1**	Design services and all modifications to support horizontal scaling.  
**P1.2**	Automate everything from code commit to deployment  
**P1.3**	Ensure the integrated product is always in a working, deployable state. Using a Staging         environment as a mandatory deployment step validates stability.  
**P1.4**	Measure Everything Relevant and use metrics to define the system's health  
**P1.5**	Automation via alerts and automated actions based on those alerts (Alerting/Self-Healing)  
**P2.1**	Statelessness is Mandatory. Decouple compute from state. Avoid In-Memory State. Externalizing State to dedicated, scalable stores  
**P2.2**	Prefer Managed Services (Operational Efficiency). Buy, Don't Build (or Manage) commodity infrastructure.  
**P2.3**	Enforce Autonomy and Isolation between services  
**P3.1**	Systems must treat all external dependencies (databases, caches, third-party APIs) as potential points of failure and implement specific defense mechanisms  
**P3.2**	Idempotency. Guarantee that re-processing the same request or message (due to system restarts, node failures, or retries) produces the exact same outcome  
**P3.3**	Break down large operations into smaller, discrete, and independently retriable operations/transactions  
**P3.4**	Implement Circuit Breakers to rapidly detect and stop calling services that are temporarily unavailable or severely degraded  
**P3.5**	Leverage Event-Driven Architecture to decouple services in time. Instead of direct, blocking calls, services communicate via persistent message queues/brokers.  
**P4.1**	Adopt Zero Trust via Strict Identity Verification. Ensure every user and service accessing resources is authenticated and authorized.  
**P4.2**	Isolate Tenant Resources at Every Architectural Layer. Achieve logical and physical separation to prevent cross-tenant data access, security breaches, and "noisy neighbor" performance issues.  
**P4.3**	Use dedicated Secret Management solutions to inject credentials into workloads at runtime, eliminating the practice of storing secrets in code or configuration files