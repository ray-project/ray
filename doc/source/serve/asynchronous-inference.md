---
myst:
  html_meta:
    description: "Run long-running inference asynchronously using the @task_consumer and @task_handler APIs with Celery-backed queues, keeping HTTP responses immediate."
---

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
    def process_pdf(self, request: ProcessPDFRequest):
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
    def get_status(self, task_id: str):
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

Set `max_ongoing_requests` on the consumer deployment to cap how many tasks each replica processes simultaneously. The default Celery adapter uses late acknowledgement and requeues tasks on worker loss. Delivery behavior depends on the broker and its configuration. Write handlers that tolerate duplicate execution.

By default, the Celery adapter uses `max_retries` for retries of application exceptions. After those retries are exhausted, it routes the failed task to the configured DLQ. Broker redelivery after a process dies can occur without advancing this application retry count. If a job needs a limit across worker losses or re-enqueueing, track admitted attempts or a deadline in application-owned durable state.

(serve-async-inference-checkpoints)=
### Resume work from checkpoints

To resume inference between pages, video segments, or batches, store checkpoints in your application. The task consumer API doesn't provide durable checkpoints or resumable progress. `get_task_status_sync(task_id)` reads the adapter's task status and result. It doesn't restore partially completed inference.

The following example separates task delivery, execution attempts, and committed job state:

```{literalinclude} doc_code/async_inference_checkpoint_contract.py
:language: python
:start-after: __records_begin__
:end-before: __records_end__
```

Use these fields to identify work and recover committed state:

| Field | Contract |
| --- | --- |
| `job_id` | Stable application job ID. Pass it in the task payload and reuse it across submission retries and re-enqueueing. |
| `fingerprint` | Immutable input revision, model and pipeline versions, and relevant parameters. A mutable URL alone doesn't identify an input revision. |
| `task_id` | Adapter task ID for status and diagnostics. Retries may retain it, while re-enqueueing may create a different ID for the same job. |
| `attempt_id`, `fence` | Fresh execution ID and a monotonically increasing token that identifies the job's owner. |
| `checkpoint_ref`, `next_unit` | Immutable checkpoint reference and the next unit to process. Progress counts committed work. |
| `result_ref` | Immutable result reference. Its presence marks the job complete. |

Implement three transitions in the handler:

1. Claim the job or return its result. Reject a mismatched fingerprint before checking for completion. If the job is complete, return `result_ref` without recomputing it. Otherwise, atomically acquire an application-owned lease and fencing token for a fresh `attempt_id`. Duplicate deliveries must not take over a live lease. After expiry, another attempt can take over even if the old worker still runs.
1. Resume from the committed checkpoint. Restore all completed units `[0, next_unit)` from the checkpoint or its manifest, then process the next unit. Write each checkpoint artifact durably before atomically committing its reference and progress. Reject backwards progress. Make retries of the same commit idempotent.
1. Publish the result before returning. Write the final artifact durably, then atomically commit `result_ref` and the terminal state. Only then return from the handler so the adapter can acknowledge the task. If publication times out, read back the record before retrying. The task API doesn't provide a transaction spanning this record and queue acknowledgement.

For every checkpoint and result commit, atomically check the attempt's ownership token with the write. An old attempt must not overwrite state after takeover. With object storage, publish immutable artifact references through a transactional record and clean up unreferenced artifacts later. Poll committed application state by `job_id` instead of combining progress events from different attempts.

Retain the job record, fencing generation, and referenced artifacts for as long as attempts can write or tasks can be redelivered. Deleting and recreating state or reusing a `job_id` during that period resets fencing and result deduplication. Checkpointing doesn't make external effects exactly once. Use idempotency keys or a transaction for those effects, and expect computation since the last checkpoint to repeat.

Verify these three failure cases:

| Failure window | Expected recovery |
| --- | --- |
| A worker dies after committing a checkpoint | The replacement loads that checkpoint and resumes at the next uncommitted unit with a new token. |
| A worker dies after committing the result but before acknowledgement | The redelivered task returns the same result, even if its `task_id` differs. |
| An old attempt keeps running after lease takeover | Its token cannot publish checkpoints or results. Only the new owner can commit, and completed state stays immutable. |

Download the {download}`checkpoint example <doc_code/async_inference_checkpoint_contract.py>` and run its tests with the Python standard library. From the Ray repository root, run:

```bash
python doc/source/serve/doc_code/async_inference_checkpoint_contract.py -v
```

:::{note}
The example models atomic storage transitions with in-memory records. Each claim assumes lease admission has succeeded. Tests interleave attempts to check recovery, identity, and commit rules. They don't implement leases, durable storage, inference, or a broker. Test those components separately with your storage and adapter.
:::

(serve-async-inference-autoscaling)=
## Autoscaling

For workloads with variable traffic you can enable autoscaling so that replicas scale up when messages pile up in the queue and scale back down (optionally to zero) when the queue drains.

Ray Serve provides a built-in `AsyncInferenceAutoscalingPolicy` — a [class-based autoscaling policy](serve-custom-autoscaling-policies) that polls your message broker for queue length and scales replicas to match demand from both pending queue messages and in-flight requests.

### Basic example

::::{tab-set}

:::{tab-item} Python (imperative)

```{literalinclude} doc_code/async_inference_autoscaling.py
:language: python
:start-after: __basic_example_begin__
:end-before: __basic_example_end__
```

:::

:::{tab-item} YAML (declarative)

```{literalinclude} doc_code/async_inference_autoscaling.yaml
:language: yaml
:start-after: __basic_example_begin__
:end-before: __basic_example_end__
```

:::

::::

:::{note}
The `broker_url` and `queue_name` in `policy_kwargs` must match the values in your `TaskProcessorConfig`. The policy reads queue length from the same broker that your task consumer reads tasks from.
:::


### Policy parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `broker_url` | `str` | *(required)* | URL of the message broker (e.g. `redis://localhost:6379/0` or `amqp://guest:guest@localhost:5672//`). |
| `queue_name` | `str` | *(required)* | Name of the queue to monitor. Must match `TaskProcessorConfig.queue_name`. |
| `rabbitmq_management_url` | `str` | `None` | RabbitMQ HTTP management API URL (e.g. `http://guest:guest@localhost:15672/api/`). Required only for RabbitMQ brokers. |
| `poll_interval_s` | `float` | `10.0` | How often (seconds) to poll the broker for queue length. Lower values increase responsiveness but add broker load. |

All standard `AutoscalingConfig` parameters (`upscale_delay_s`, `downscale_delay_s`, `upscaling_factor`, `downscaling_factor`, etc.) apply on top of this policy. See [Advanced Ray Serve Autoscaling](serve-advanced-autoscaling) for details.

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
