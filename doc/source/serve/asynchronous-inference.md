(serve-asynchronous-inference)=

# Asynchronous inference in Ray Serve

This guide shows how to run long-running inference asynchronously in Ray Serve using background task processing. With async tasks, your HTTP APIs stay responsive while the system performs work in the background.

## Why asynchronous inference?

Some inference workloads (such as video processing or large document indexing) take longer than typical HTTP timeouts. Asynchronous task processing lets you enqueue work to a background queue and immediately return, decoupling request lifetime from compute time while still leveraging Serve's scalability.

## Use cases

- **Video inference**: Transcoding, detection, transcription over long videos.
- **Document indexing**: Ingestion, parsing, vectorization of large files or batches.
- **Any long-running ML task** where immediate results aren't required.

## Key concepts

- **Task**: A user-defined function invocation with arguments to execute asynchronously.
- **Task Consumer Deployment**: A Serve deployment that consumes and executes tasks from a queue.
- **Task Handler**: A method inside the consumer marked to handle a named task.
- **Task Processor Adapter**: Pluggable adapter that interfaces with a task processor or broker (such as Celery).
- **Task Result**: A model representing task metadata, status, and optional return value.

## Components and APIs

The following sections describe the core APIs for asynchronous inference, with minimal examples to get you started.

### `CeleryAdapterConfig`
Configuration for the Celery adapter, including broker and backend URLs and worker settings.

The following example shows a Celery adapter configuration:
```python
from ray.serve.schema import CeleryAdapterConfig

celery_config = CeleryAdapterConfig(
    broker_url="redis://localhost:6379/0",     # Or "filesystem://" for local testing
    backend_url="redis://localhost:6379/1",    # Result backend (optional for fire-and-forget)
    worker_concurrency=10,
)
```

### `TaskProcessorConfig`
Configures the task processor, including queue name, adapter (default is Celery), adapter config, retry limits, and dead-letter queues.

The following example shows how to configure the task processor:
```python
from ray.serve.schema import TaskProcessorConfig

processor_config = TaskProcessorConfig(
    queue_name="my_queue",
    # Optional: Override default adapter string (default is Celery)
    # adapter="ray.serve.task_processor.CeleryTaskProcessorAdapter",
    adapter_config=celery_config,
    max_retries=5,
    failed_task_queue_name="failed_tasks",              # Application errors after retries
    unprocessable_task_queue_name="unprocessable_tasks" # Missing handler or deserialization errors
)
```

### `TaskResult`
Represents a task's ID, status, timestamp, and optional result.

The following example demonstrates creating and accessing a task result:
```python
from ray.serve.schema import TaskResult

result = TaskResult(
    id="task-123",
    status="SUCCESS",
    result={"message": "ok"},
)
print(result.id, result.status)
```

### `@task_consumer`
Decorator that turns a Serve deployment into a task consumer using the provided `TaskProcessorConfig`.

The following code creates a task consumer:
```python
from ray import serve
from ray.serve.task_consumer import task_consumer

@serve.deployment
@task_consumer(task_processor_config=processor_config)
class SimpleConsumer:
    pass
```

### `@task_handler`
Decorator that registers a method on the consumer as a named task handler.

:::{note}
Ray Serve currently supports only synchronous handlers. Declaring an `async def` handler raises `NotImplementedError`.
:::

The following example shows how to define a task handler:
```python
from ray.serve.task_consumer import task_handler

class SimpleConsumer:
    @task_handler(name="process_request")
    def process_request(self, data):
        return f"processed: {data}"
```

### `instantiate_adapter_from_config`
Factory function that returns a task processor adapter instance for the given `TaskProcessorConfig`. You can use the returned object to enqueue tasks, fetch status, retrieve metrics, and more.

The following example demonstrates creating an adapter and enqueuing tasks:
```python
from ray.serve.task_consumer import instantiate_adapter_from_config

adapter = instantiate_adapter_from_config(task_processor_config=processor_config)
# Enqueue synchronously (returns TaskResult with ID)
result = adapter.enqueue_task_sync("process_request", args=["hello"])
# Later, fetch status synchronously
status = adapter.get_task_status_sync(result.id)
```

### Submit tasks from outside Serve (producers)
You can enqueue tasks from external producers (non-Serve code) if they share the same `TaskProcessorConfig`.

The following example shows how to enqueue tasks from external code:
```python
from ray.serve.task_consumer import instantiate_adapter_from_config
from ray.serve.schema import TaskProcessorConfig, CeleryAdapterConfig

celery_config = CeleryAdapterConfig(
    broker_url="redis://localhost:6379/0",
    backend_url="redis://localhost:6379/1",
)
processor_config = TaskProcessorConfig(queue_name="my_queue", adapter_config=celery_config)

adapter = instantiate_adapter_from_config(task_processor_config=processor_config)
result = adapter.enqueue_task_sync("process_request", args=["payload"])
print("Enqueued:", result.id)
```

## End-to-end example: Document indexing

This example shows how to configure the processor, build a consumer with a handler, enqueue tasks from an ingress deployment, and check task status.

```python
import ray
from ray import serve
from fastapi import FastAPI, Request

from ray.serve.schema import CeleryAdapterConfig, TaskProcessorConfig, TaskResult
from ray.serve.task_consumer import (
    task_consumer,
    task_handler,
    instantiate_adapter_from_config,
)

# 1) Configure the Celery adapter
celery_config = CeleryAdapterConfig(
    broker_url="redis://localhost:6379/0",   # Broker URL
    backend_url="redis://localhost:6379/1",  # Optional result backend
    worker_concurrency=10,
)

# 2) Configure the task processor
processor_config = TaskProcessorConfig(
    queue_name="document_indexing_queue",
    adapter_config=celery_config,
    max_retries=3,
    failed_task_queue_name="doc_failed",
    unprocessable_task_queue_name="doc_unprocessable",
)

# 3) Define the consumer deployment for background processing
@serve.deployment(num_replicas=2)
@task_consumer(task_processor_config=processor_config)
class DocumentIndexingConsumer:
    def __init__(self):
        self.indexer = DocumentIndexingEngine()  # Your implementation

    @task_handler(name="index_document")
    def index_document(self, document_id: str, document_url: str) -> dict:
        content = self.indexer.download(document_url)
        metadata = self.indexer.process(content)
        return {"document_id": document_id, "status": "indexed", "metadata": metadata}

# 4) Define an ingress deployment to submit tasks and fetch status
app = FastAPI()

@serve.deployment
@serve.ingress(app)
class API:
    def __init__(self, consumer_handle, task_processor_config: TaskProcessorConfig):
        # Keep a reference to the consumer to include it in the application graph
        self.consumer = consumer_handle
        self.adapter = instantiate_adapter_from_config(
            task_processor_config=task_processor_config
        )

    @app.post("/submit")
    async def submit(self, request: Request):
        data = await request.json()
        # Enqueue synchronously; returns TaskResult containing ID
        task: TaskResult = self.adapter.enqueue_task_sync(
            "index_document", kwargs=data
        )
        return {"task_id": task.id}

    @app.get("/status/{task_id}")
    async def status(self, task_id: str):
        # Synchronously fetch current status or result
        return self.adapter.get_task_status_sync(task_id)

# 5) Build and run the application
consumer = DocumentIndexingConsumer.bind()
app_graph = API.bind(consumer, processor_config)

serve.run(app_graph)
```

In this example:
- `DocumentIndexingConsumer` reads tasks from `document_indexing_queue` and processes them.
- `API` enqueues tasks through `enqueue_task_sync` and fetches status through `get_task_status_sync`.
- Passing `consumer` into `API.__init__` ensures both deployments are part of the Serve application graph.

## Concurrency and reliability

To manage concurrency, configure consumer-side concurrency through the adapter config (such as `worker_concurrency` in `CeleryAdapterConfig`). To ensure at-least-once processing, adapters should acknowledge tasks only after successful execution. The system retries failed tasks up to `max_retries` times; if they continue to fail, the system routes them to the failed-task DLQ when configured. The default Celery adapter acknowledges on success to provide at-least-once processing.

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

