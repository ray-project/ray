# Deploy custom Model Context Protocol (MCP) servers

<div align="left">
<a target="_blank" href="https://console.anyscale.com/"><img src="https://img.shields.io/badge/🚀 Run_on-Anyscale-9hf"></a>&nbsp;
<a href="https://github.com/ray-project/ray" role="button"><img src="https://img.shields.io/static/v1?label=&amp;message=View%20On%20GitHub&amp;color=586069&amp;logo=github&amp;labelColor=2f363d"></a>&nbsp;
</div>

This tutorial demonstrates how to build and deploy **custom Model Context Protocol (MCP)** servers using Ray Serve in both **HTTP streaming** and **stdio** modes. MCP enables scalable, dynamic, and multi-tenant model serving by decoupling model routing from application logic.

- [**`01-Deploy_custom_mcp_in_streamable_http_with_ray_serve.ipynb`**](https://github.com/ray-project/ray/blob/master/doc/source/ray-overview/examples/mcp-ray-serve/01%20Deploy_custom_mcp_in_streamable_http_with_ray_serve.ipynb): Deploy a single MCP server in **HTTP streaming mode**, using Ray Serve and FastAPI.
- [**`02-Build_mcp_gateway_with_existing_ray_serve_apps.ipynb`**](https://github.com/ray-project/ray/blob/master/doc/source/ray-overview/examples/mcp-ray-serve/02%20Build_mcp_gateway_with_existing_ray_serve_apps.ipynb): Route traffic to existing Ray Serve applications through an MCP gateway.
- [**`03-Deploy_single_mcp_stdio_docker_image_with_ray_serve.ipynb`**](https://github.com/ray-project/ray/blob/master/doc/source/ray-overview/examples/mcp-ray-serve/03%20Deploy_single_mcp_stdio_docker_image_with_ray_serve.ipynb): Deploy an MCP Server with standard input/output stream as a scalable HTTP service managed by Ray Serve.
- [**`04-Deploy_multiple_mcp_stdio_docker_images_with_ray_serve.ipynb`**](https://github.com/ray-project/ray/blob/master/doc/source/ray-overview/examples/mcp-ray-serve/04%20Deploy_multiple_mcp_stdio_docker_images_with_ray_serve.ipynb): Run multiple MCP servers using a shared deployment.
- [**`05-(Optional)_Build_docker_image_for_mcp_server.ipynb`**](https://github.com/ray-project/ray/blob/master/doc/source/ray-overview/examples/mcp-ray-serve/05%20(Optional)%20Build_docker_image_for_mcp_server.ipynb): Guide to building and customizing a Docker image for a standalone MCP server.


## Prerequisites

- Ray Serve, which is included in the base Docker image
- Podman, to deploy MCP tools with existing Docker images for notebooks 3 through 5 
- A Brave API key set in your environment (`BRAVE_API_KEY`)
- an MCP Python library

### Setting the API key

Before running notebooks 3 and 4, you must set your Brave API key:
```bash
export BRAVE_API_KEY=your-api-key
```

## Development

You can run this example on your own Ray cluster or on [Anyscale workspaces](https://docs.anyscale.com/platform/workspaces/), which enables development without worrying about infrastructure—like working on a laptop. Workspaces come with:
- **Development tools**: Spin up a remote session from your local IDE (Cursor, VS Code, etc.) and start coding, using the tools you're familiar with combined with the power of Anyscale's compute.
- **Dependencies**: Continue to install dependencies using familiar tools like pip. Anyscale propagates all dependencies to your cluster.

```bash
pip install -q "ray[serve]" "fastapi" "httpx" "uvicorn" "aiohttp" "tqdm"
```

* **Compute**: Leverage any reserved instance capacity, spot instance from any compute provider of your choice by deploying Anyscale into your account. Alternatively, you can use the Anyscale cloud for a full serverless experience.

  * Under the hood, a cluster spins up and is efficiently managed by Anyscale.
* **Debugging**: Leverage a [distributed debugger](https://docs.anyscale.com/platform/workspaces/workspaces-debugging/#distributed-debugger) to get the same VS Code-like debugging experience.

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
