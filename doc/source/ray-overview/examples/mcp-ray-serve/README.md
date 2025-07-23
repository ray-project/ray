# Deploy MCP servers

<div align="left">
<a target="_blank" href="https://console.anyscale.com/"><img src="https://img.shields.io/badge/🚀 Run_on-Anyscale-9hf"></a>&nbsp;
<a href="https://github.com/ray-project/ray" role="button"><img src="https://img.shields.io/static/v1?label=&amp;message=View%20On%20GitHub&amp;color=586069&amp;logo=github&amp;labelColor=2f363d"></a>&nbsp;
</div>

This repository provides end-to-end examples for deploying and scaling Model Context Protocol (MCP) servers using Ray Serve and Anyscale Service, covering both streamable HTTP and stdio transport types:

- [**`01-Deploy_custom_mcp_in_streamable_http_with_ray_serve.ipynb`**](https://github.com/ray-project/ray/blob/master/doc/source/ray-overview/examples/mcp-ray-serve/01%20Deploy_custom_mcp_in_streamable_http_with_ray_serve.ipynb): Deploys a custom Weather MCP server in streamable HTTP mode behind FastAPI + Ray Serve, illustrating autoscaling, load‑balancing, and end‑to‑end testing on Anyscale.
- [**`02-Build_mcp_gateway_with_existing_ray_serve_apps.ipynb`**](https://github.com/ray-project/ray/blob/master/doc/source/ray-overview/examples/mcp-ray-serve/02%20Build_mcp_gateway_with_existing_ray_serve_apps.ipynb): Shows how to stand up a single MCP gateway that multiplexes requests to multiple pre‑existing Ray Serve apps under one unified `/mcp` endpoint, requiring no code changes in the underlying services.
- [**`03-Deploy_single_mcp_stdio_docker_image_with_ray_serve.ipynb`**](https://github.com/ray-project/ray/blob/master/doc/source/ray-overview/examples/mcp-ray-serve/03%20Deploy_single_mcp_stdio_docker_image_with_ray_serve.ipynb): Wraps a stdio‑only MCP Docker image, for example Brave Search, with Ray Serve so it exposes `/tools` and `/call` HTTP endpoints and scales horizontally without rebuilding the image. 
- [**`04-Deploy_multiple_mcp_stdio_docker_images_with_ray_serve.ipynb`**](https://github.com/ray-project/ray/blob/master/doc/source/ray-overview/examples/mcp-ray-serve/04%20Deploy_multiple_mcp_stdio_docker_images_with_ray_serve.ipynb): Extends the previous pattern to run several stdio‑based MCP images side‑by‑side, using fractional‑CPU deployments and a router to direct traffic to the right service. 
- [**`05-(Optional)_Build_docker_image_for_mcp_server.ipynb`**](https://github.com/ray-project/ray/blob/master/doc/source/ray-overview/examples/mcp-ray-serve/05%20(Optional)%20Build_docker_image_for_mcp_server.ipynb): Builds and pushes a lightweight Podman‑based Docker image for a Weather MCP server with uv in an Anyscale workspace.

## Why Ray Serve for MCP
- **Autoscaling:** Dynamically adjusts replica count to match traffic peaks and maintain responsiveness
- **Load balancing:**  Intelligently distributes incoming requests across all replicas for steady throughput
- **Observability:** Exposes real‑time metrics on request rates, resource usage & system health
- **Fault tolerance:** Detects failures, restarts components, and reroutes traffic to healthy replicas for continuous availability
- **Composition:**  Chains deployments—pre‑process, infer, post‑process, and custom logic—into a single seamless pipeline


## Anyscale service benefits
- **Production ready:**  Enterprise‑grade infrastructure management and automated deployments for real‑world MCP traffic
- **[High availability](https://docs.anyscale.com/platform/services/faq#does-services-support-multiple-availability-zones-for-high-availability):**  Availability‑Zone‑aware scheduling and zero‑downtime rolling updates to maximize uptime
- **[Logging](https://docs.anyscale.com/monitoring/accessing-logs) and [tracing](https://docs.anyscale.com/monitoring/tracing):**  Comprehensive logs, distributed tracing, and real‑time dashboards for end‑to‑end observability
- **[Head node fault tolerance](https://docs.anyscale.com/platform/services/head-node-ft/):**  Managed head‑node redundancy to eliminate single points of failure in your Ray cluster coordination layer


## Prerequisites

- Ray Serve, which is included in the base Docker image
- Podman, to deploy MCP tools with existing Docker images for notebooks 3 through 5 
- A Brave API key set in your environment (`BRAVE_API_KEY`) for notebooks 3 and 4
- MCP Python library

## Development

You can run this example on your own Ray cluster or on [Anyscale workspaces](https://docs.anyscale.com/platform/workspaces/), which enables development without worrying about infrastructure—like working on a laptop. Workspaces come with:
- **Development tools**: Spin up a remote session from your local IDE (Cursor, VS Code, etc.) and start coding, using the tools you're familiar with combined with the power of Anyscale's compute.
- **Dependencies**: Continue to install dependencies using familiar tools like pip. Anyscale propagates all dependencies to your cluster.
- **Compute**: Leverage any reserved instance capacity, spot instance from any compute provider of your choice by deploying Anyscale into your account. Alternatively, you can use the Anyscale cloud for a full serverless experience.
- **Debugging**: Leverage a [distributed debugger](https://docs.anyscale.com/platform/workspaces/workspaces-debugging/#distributed-debugger) to get the same VS Code-like debugging experience.

Learn more about Anyscale Workspaces in the [official documentation](https://docs.anyscale.com/platform/workspaces/).

**Note**: Run the entire tutorial for free on [Anyscale](https://console.anyscale.com/)—all dependencies come pre-installed, and compute autoscales automatically. To run it elsewhere, install the dependencies from the [`Dockerfiles`](https://github.com/ray-project/ray/blob/master/doc/source/ray-overview/examples/mcp-ray-serve/build-mcp-docker-image/) provided and provision the appropriate resources..

## Production

Seamlessly integrate with your existing CI/CD pipelines by leveraging the Anyscale [CLI](https://docs.anyscale.com/reference/quickstart-cli) or [SDK](https://docs.anyscale.com/reference/quickstart-sdk) to deploy [highly available services](https://docs.anyscale.com/platform/services) and run [reliable batch jobs](https://docs.anyscale.com/platform/jobs). Developing in an environment nearly identical to production—a multi-node cluster—drastically accelerates the dev-to-prod transition. This tutorial also introduces proprietary RayTurbo features that optimize workloads for performance, fault tolerance, scale, and observability.

## No infrastructure headaches

Abstract away infrastructure from your ML/AI developers so they can focus on their core ML development. You can additionally better manage compute resources and costs with [enterprise governance and observability](https://www.anyscale.com/blog/enterprise-governance-observability) and [admin capabilities](https://docs.anyscale.com/administration/overview) so you can set [resource quotas](https://docs.anyscale.com/reference/resource-quotas/), set [priorities for different workloads](https://docs.anyscale.com/administration/cloud-deployment/global-resource-scheduler) and gain [observability of your utilization across your entire compute fleet](https://docs.anyscale.com/administration/resource-management/telescope-dashboard).
If you're running on a Kubernetes cloud (EKS, GKE, etc.), you can still access the proprietary RayTurbo optimizations demonstrated in this tutorial by deploying the [Anyscale Kubernetes operator](https://docs.anyscale.com/administration/cloud-deployment/kubernetes/).

```{toctree}
:hidden:

01 Deploy_custom_mcp_in_streamable_http_with_ray_serve.ipynb
02 Build_mcp_gateway_with_existing_ray_serve_apps.ipynb
03 Deploy_single_mcp_stdio_docker_image_with_ray_serve.ipynb
04 Deploy_multiple_mcp_stdio_docker_images_with_ray_serve.ipynb
05 (Optional) Build_docker_image_for_mcp_server.ipynb
```

