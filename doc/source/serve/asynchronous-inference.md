(serve-asynchronous-inference)=

:::{warning}
This API is in alpha and may change before becoming stable.
:::

# Asynchronous Inference

This guide shows how to run long-running inference asynchronously in Ray Serve using background task processing. With asynchronous tasks, your HTTP APIs stay responsive while the system performs work in the background.

## Why asynchronous inference?

Ray Serve customers need a way to handle long-running API requests asynchronously. Some inference workloads (such as video processing or large document indexing) take longer than typical HTTP timeouts, so when a user submits one of these requests the system should enqueue the work in a background queue for later processing and immediately return a quick response. This decouples request lifetime from compute time while the task executes asynchronously, while still leveraging Serve's scalability.

## Use cases

Common use cases include video inference (such as transcoding, detection, and transcription over long videos) and document indexing pipelines that ingest, parse, and vectorize large files or batches. More broadly, any long-running AI/ML workload where immediate results aren't required benefits from running asynchronously.

## Key concepts

- **@task_consumer**: A Serve deployment that consumes and executes tasks from a queue. Requires a `TaskProcessorConfig` parameter to configure the task processor; by default it uses the Celery task processor, but you can provide your own implementation.
- **@task_handler**: A decorator applied to a method inside a `@task_consumer` class. Each handler declares the task it handles via `name=...`; if `name` is omitted, the method's function name is used as the task name. All tasks with that name in the consumer's configured queue (set via the `TaskProcessorConfig` above) are routed to this method for execution.


## Components and APIs

The following sections describe the core APIs for asynchronous inference, with minimal examples to get you started.


### `TaskProcessorConfig`
Configures the task processor, including queue name, adapter (default is Celery), adapter config, retry limits, and dead-letter queues. The following example shows how to configure the task processor:

```python
from ray.serve.schema import TaskProcessorConfig, CeleryAdapterConfig

processor_config = TaskProcessorConfig(
    queue_name="my_queue",
    # Optional: Override default adapter string (default is Celery)
    # adapter="ray.serve.task_processor.CeleryTaskProcessorAdapter",
    adapter_config=CeleryAdapterConfig(
        broker_url="redis://localhost:6379/0",     # Or "filesystem://" for local testing
        backend_url="redis://localhost:6379/1",    # Result backend (optional for fire-and-forget)
    ),
    max_retries=5,
    failed_task_queue_name="failed_tasks",              # Application errors after retries
)
```

:::{note}
The filesystem broker is intended for local testing only and has limited functionality. For example, it doesn't support `cancel_tasks`. For production deployments, use a production-ready broker such as Redis or RabbitMQ. See the [Celery broker documentation](https://docs.celeryq.dev/en/stable/getting-started/backends-and-brokers/) for the full list of supported brokers.
:::

### `@task_consumer`
Decorator that turns a Serve deployment into a task consumer using the provided `TaskProcessorConfig`. The following code creates a task consumer:

```python
from ray import serve
from ray.serve.task_consumer import task_consumer

@serve.deployment
@task_consumer(task_processor_config=processor_config)
class SimpleConsumer:
    pass
```

### `@task_handler`
Decorator that registers a method on the consumer as a named task handler. The following example shows how to define a task handler:

```python
from ray.serve.task_consumer import task_handler, task_consumer

@serve.deployment
@task_consumer(task_processor_config=processor_config)
class SimpleConsumer:
    @task_handler(name="process_request")
    def process_request(self, data):
        return f"processed: {data}"
```

:::{note}
Ray Serve currently supports only synchronous handlers. Declaring an `async def` handler raises `NotImplementedError`.
:::


### `instantiate_adapter_from_config`
Factory function that returns a task processor adapter instance for the given `TaskProcessorConfig`. You can use the returned object to enqueue tasks, fetch status, retrieve metrics, and more. The following example demonstrates creating an adapter and enqueuing tasks:

```python
from ray.serve.task_consumer import instantiate_adapter_from_config

adapter = instantiate_adapter_from_config(task_processor_config=processor_config)
# Enqueue synchronously (returns TaskResult)
result = adapter.enqueue_task_sync(task_name="process_request", args=["hello"])
# Later, fetch status synchronously
status = adapter.get_task_status_sync(result.id)
```

:::{note}
All Ray actor options specified in the `@serve.deployment` decorator (such as `num_gpus`, `num_cpus`, `resources`, etc.) are applied to the task consumer replicas. This allows you to allocate specific hardware resources for your task processing workloads.
:::


## End-to-end example: Document indexing

This example shows how to configure the processor, build a consumer with a handler, enqueue tasks from an ingress deployment, and check task status.

```python
import io
import logging
import requests
from fastapi import FastAPI
from pydantic import BaseModel, HttpUrl
from PyPDF2 import PdfReader
from ray import serve
from ray.serve.schema import CeleryAdapterConfig, TaskProcessorConfig
from ray.serve.task_consumer import (
    instantiate_adapter_from_config,
    task_consumer,
    task_handler,
)

logger = logging.getLogger("ray.serve")
fastapi_app = FastAPI(title="Async PDF Processing API")

TASK_PROCESSOR_CONFIG = TaskProcessorConfig(
    queue_name="pdf_processing_queue",
    adapter_config=CeleryAdapterConfig(
        broker_url="redis://127.0.0.1:6379/0",
        backend_url="redis://127.0.0.1:6379/0",
    ),
    max_retries=3,
    failed_task_queue_name="failed_pdfs",
)

class ProcessPDFRequest(BaseModel):
    pdf_url: HttpUrl
    max_summary_paragraphs: int = 3


@serve.deployment(num_replicas=2, max_ongoing_requests=5)
@task_consumer(task_processor_config=TASK_PROCESSOR_CONFIG)
class PDFProcessor:
    """Background worker that processes PDF documents asynchronously."""

    @task_handler(name="process_pdf")
    def process_pdf(self, pdf_url: str, max_summary_paragraphs: int = 3):
        """Download PDF, extract text, and generate summary."""
        try:
            response = requests.get(pdf_url, timeout=30)
            response.raise_for_status()

            pdf_reader = PdfReader(io.BytesIO(response.content))
            if not pdf_reader.pages:
                raise ValueError("PDF contains no pages")

            full_text = "\n".join(
                page.extract_text() for page in pdf_reader.pages if page.extract_text()
            )
            if not full_text.strip():
                raise ValueError("PDF contains no extractable text")

            paragraphs = [p.strip() for p in full_text.split("\n\n") if p.strip()]
            summary = "\n\n".join(paragraphs[:max_summary_paragraphs])

            return {
                "status": "success",
                "pdf_url": pdf_url,
                "page_count": len(pdf_reader.pages),
                "word_count": len(full_text.split()),
                "summary": summary,
            }
        except requests.exceptions.RequestException as e:
            raise ValueError(f"Failed to download PDF: {str(e)}")
        except Exception as e:
            raise ValueError(f"Failed to process PDF: {str(e)}")


@serve.deployment()
@serve.ingress(fastapi_app)
class AsyncPDFAPI:
    """HTTP API for submitting and checking PDF processing tasks."""

    def __init__(self, task_processor_config: TaskProcessorConfig, handler):
        self.adapter = instantiate_adapter_from_config(task_processor_config)

    @fastapi_app.post("/process")
    async def process_pdf(self, request: ProcessPDFRequest):
        """Submit a PDF processing task and return task_id immediately."""
        task_result = self.adapter.enqueue_task_sync(
            task_name="process_pdf",
            kwargs={
                "pdf_url": str(request.pdf_url),
                "max_summary_paragraphs": request.max_summary_paragraphs,
            },
        )
        return {
            "task_id": task_result.id,
            "status": task_result.status,
            "message": "PDF processing task submitted successfully",
        }

    @fastapi_app.get("/status/{task_id}")
    async def get_status(self, task_id: str):
        """Get task status and results."""
        status = self.adapter.get_task_status_sync(task_id)
        return {
            "task_id": task_id,
            "status": status.status,
            "result": status.result if status.status == "SUCCESS" else None,
            "error": str(status.result) if status.status == "FAILURE" else None,
        }

app = AsyncPDFAPI.bind(TASK_PROCESSOR_CONFIG, PDFProcessor.bind())
```

In this example:
- `DocumentIndexingConsumer` reads tasks from `document_indexing_queue` queue and processes them.
- `API` enqueues tasks through `enqueue_task_sync` and fetches status through `get_task_status_sync`.
- Passing `consumer` into `API.__init__` ensures both deployments are part of the Serve application graph.

## Concurrency and reliability

 Manage concurrency by setting `max_ongoing_requests` on the consumer deployment; this caps how many tasks each replica can process simultaneously. For at-least-once delivery, adapters should acknowledge a task only after the handler completes successfully. Failed tasks are retried up to `max_retries`; once exhausted, they are routed to the failed-task DLQ when configured. The default Celery adapter acknowledges on success, providing at-least-once processing.

## Dead letter queues (DLQs)

Dead letter queues handle two types of problematic tasks:
- **Unprocessable tasks**: The system routes tasks with no matching handler to `unprocessable_task_queue_name` if set.
- **Failed tasks**: The system routes tasks that raise application exceptions after exhausting retries, have mismatched arguments, and other errors to `failed_task_queue_name` if set.

## Rollouts and compatibility

During deployment upgrades, both old and new consumer replicas may run concurrently and pull from the same queue. If task schemas or names change, either version may see incompatible tasks.

Recommendations:
- **Version task names and payloads** to allow coexistence across versions.
- **Don't remove handlers** until you drain old tasks.
- **Monitor DLQs** for deserialization or handler resolution failures and re-enqueue or transform as needed.

## Limitations

- Ray Serve supports only synchronous `@task_handler` methods.
- External (non-Serve) workers are out of scope; all consumers run as Serve deployments.
- Delivery guarantees ultimately depend on the configured broker. Results are optional when you don't configure a result backend.

:::{note}
The APIs in this guide reflect the alpha interfaces in `ray.serve.schema` and `ray.serve.task_consumer`.
:::

