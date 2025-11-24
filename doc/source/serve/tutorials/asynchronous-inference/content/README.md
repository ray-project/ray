# Asynchronous Inference with Ray Serve

**⏱️ Time to complete:** 30 minutes

This template demonstrates how to build scalable asynchronous inference services using Ray Serve. Learn how to handle long-running PDF processing tasks without blocking HTTP responses, using Celery task queues and Redis as a message broker.

## Overview

Traditional synchronous APIs block until processing completes, causing timeouts for long-running tasks. Ray Serve's asynchronous inference pattern decouples request lifetime from compute time by:

1. Accepting HTTP requests and immediately returning a task ID
2. Enqueuing work to background processors (Celery workers)
3. Allowing clients to poll for status and retrieve results

This example implements a **PDF processing service** that extracts text and generates summaries from PDF documents.

## Architecture Overview

```
┌─────────────┐
│   Client    │
└──────┬──────┘
       │ HTTP POST /process
       ▼
┌─────────────────────┐
│   AsyncPDFAPI       │ ← Ingress Deployment
│ (HTTP Endpoints)    │
└──────┬──────────────┘
       │ enqueue_task()
       ▼
┌─────────────────────┐
│   Redis Queue       │ ← Message Broker
│ (Celery Backend)    │
└──────┬──────────────┘
       │ consume tasks
       ▼
┌─────────────────────┐
│   PDFProcessor      │ ← Task Consumer Deployment
│ @task_consumer      │   (Scaled to N replicas)
│ - process_pdf       │
└─────────────────────┘
```

## Prerequisites

- Python 3.9+
- Ray 2.50.0+
- Redis (for message broker and result backend)

## Step 1: Setup Redis

Redis serves as both the message broker (task queue) and result backend.

**Install and start Redis (Google Colab compatible):**


```python
# Install and start Redis server
!sudo apt-get update -qq
!sudo apt-get install -y redis-server
!redis-server --port 6399 --save "" --appendonly no --daemonize yes

# Verify Redis is running
!redis-cli -p 6399 ping
```

**Alternative methods:**

- **macOS:** `brew install redis && brew services start redis`
- **Docker:** `docker run -d -p 6379:6379 redis:latest`
- **Other platforms:** [Official Redis Installation Guide](https://redis.io/docs/getting-started/installation/)

## Step 2: Install Dependencies


```python
!pip install -q ray[serve-async-inference]>=2.50.0 requests>=2.31.0 PyPDF2>=3.0.0 celery[redis]
```

## Step 3: Start the Ray Serve Application

Let's see and run the code for the service. We will go through each component independently.

### 3.1 Ingress Deployment to Handle HTTP Requests

The `AsyncPDFAPI` deployment handles HTTP requests and enqueues tasks.


```python
import logging
from fastapi import FastAPI
from pydantic import BaseModel, HttpUrl
from ray import serve
from ray.serve.handle import DeploymentHandle
from ray.serve.schema import TaskProcessorConfig
from ray.serve.task_consumer import instantiate_adapter_from_config

fastapi_app = FastAPI(title="Async PDF Processing API")
logger = logging.getLogger("ray.serve")

@serve.deployment(ray_actor_options={"num_cpus": 0.1})
@serve.ingress(fastapi_app)
class AsyncPDFAPI:
    """
    HTTP API for submitting and checking PDF processing tasks.

    Endpoints:
    - POST /process: Submit a PDF processing task
    - GET /status/{task_id}: Check task status and get results
    """

    class ProcessPDFRequest(BaseModel):
        """Request schema for PDF processing."""
        pdf_url: HttpUrl
        max_summary_paragraphs: int = 3

    def __init__(self, task_processor_config: TaskProcessorConfig, handler: DeploymentHandle):
        """Initialize the API with task adapter."""
        self.adapter = instantiate_adapter_from_config(task_processor_config)
        logger.info("AsyncPDFAPI initialized")

    @fastapi_app.post("/process")
    async def process_pdf(self, request: ProcessPDFRequest):
        """
        Submit a PDF processing task.

        Returns task_id immediately without waiting for processing to complete.
        Client should poll /status/{task_id} to check progress.
        """
        task_result = self.adapter.enqueue_task_sync(
            task_name="process_pdf",
            kwargs={
                "pdf_url": str(request.pdf_url),
                "max_summary_paragraphs": request.max_summary_paragraphs,
            },
        )

        logger.info(f"Enqueued task: {task_result}")

        return {
            "task_id": task_result.id,
            "status": task_result.status,
            "message": "PDF processing task submitted successfully",
        }

    @fastapi_app.get("/status/{task_id}")
    async def get_status(self, task_id: str):
        """
        Get task status and results.

        Status values:
        - PENDING: Task queued, waiting for worker
        - STARTED: Worker is processing the task
        - SUCCESS: Task completed successfully (result available)
        - FAILURE: Task failed (error message available)
        """
        status = self.adapter.get_task_status_sync(task_id)

        return {
            "task_id": task_id,
            "status": status.status,
            "result": status.result if status.status == "SUCCESS" else None,
            "error": str(status.result) if status.status == "FAILURE" else None,
        }

```

**Key points:**

- `/process` endpoint enqueues tasks and returns immediately with a task ID
- `/status/{task_id}` endpoint polls Redis to check task status
- No blocking - responses are instant regardless of processing time

### 3.2 Create the Task Consumer Deployment

Now, below is the deployment consumer code, which will read from the task queue and implement the tasks.

**What's a Task Consumer?**

The `@task_consumer` decorator transforms a Ray Serve deployment into a worker that consumes tasks from a queue.

**What's a Task Handler?**

The `@task_handler` decorator registers a method to process a specific task type. Each handler corresponds to a task name that producers use when enqueuing work.

For more details, see the [Asynchronous Inference Guide](https://docs.ray.io/en/master/serve/asynchronous-inference.html).


```python
import io
import time
from typing import Dict, Any
import requests
from PyPDF2 import PdfReader
from ray import serve
from ray.serve.schema import CeleryAdapterConfig, TaskProcessorConfig
from ray.serve.task_consumer import (
    task_consumer,
    task_handler,
)

TASK_PROCESSOR_CONFIG = TaskProcessorConfig(
    queue_name="pdf_processing_queue",
    adapter_config=CeleryAdapterConfig(
        broker_url="redis://127.0.0.1:6399/0",
        backend_url="redis://127.0.0.1:6399/0",
    ),
    max_retries=3,
    failed_task_queue_name="failed_pdfs",
    unprocessable_task_queue_name="invalid_pdfs",
)

@serve.deployment(num_replicas=2, max_ongoing_requests=5, ray_actor_options={"num_cpus": 0.1})
@task_consumer(task_processor_config=TASK_PROCESSOR_CONFIG)
class PDFProcessor:
    """
    Background worker that processes PDF documents asynchronously.

    Configuration:
    - num_replicas=2: Run 2 worker instances
    - max_ongoing_requests=5: Each worker handles up to 5 concurrent tasks
    - max_retries=3: Retry failed tasks up to 3 times
    """

    @task_handler(name="process_pdf")
    def process_pdf(
        self, pdf_url: str, max_summary_paragraphs: int = 3
    ) -> Dict[str, Any]:
        """
        Download PDF, extract text, and generate summary.
        """
        start_time = time.time()
        logger.info(f"Processing PDF: {pdf_url}")

        try:
            # Download PDF from URL
            response = requests.get(pdf_url, timeout=30)
            response.raise_for_status()

            # Parse PDF content
            pdf_file = io.BytesIO(response.content)
            try:
                pdf_reader = PdfReader(pdf_file)
            except Exception as e:
                raise ValueError(f"Invalid PDF file: {str(e)}")

            if len(pdf_reader.pages) == 0:
                raise ValueError("PDF contains no pages")

            # Extract text from all pages
            full_text = ""
            for page in pdf_reader.pages:
                text = page.extract_text()
                if text:
                    full_text += text + "\n"

            if not full_text.strip():
                raise ValueError("PDF contains no extractable text")

            # Generate summary (first N paragraphs)
            paragraphs = [p.strip() for p in full_text.split("\n\n") if p.strip()]
            summary = "\n\n".join(paragraphs[:max_summary_paragraphs])

            # Calculate metadata
            result = {
                "status": "success",
                "pdf_url": pdf_url,
                "page_count": len(pdf_reader.pages),
                "word_count": len(full_text.split()),
                "full_text": full_text,
                "summary": summary,
                "processing_time_seconds": round(time.time() - start_time, 2),
            }

            logger.info(f"Processed PDF: {result['page_count']} pages, {result['word_count']} words")
            return result

        except requests.exceptions.RequestException as e:
            error_msg = f"Failed to download PDF: {str(e)}"
            logger.error(error_msg)
            raise ValueError(error_msg)
        except Exception as e:
            error_msg = f"Failed to process PDF: {str(e)}"
            logger.error(error_msg)
            raise ValueError(error_msg)

```

**Key points:**

- `num_replicas=2` scales to 2 worker instances for parallel processing
- The consumer automatically polls the queue and processes tasks
- Failed tasks retry up to `max_retries` times before moving to the dead letter queue

### 3.3 Bind Deployments into an Application

Now, we will combine the deployments and run the application.


```python
consumer = PDFProcessor.bind()

app = AsyncPDFAPI.bind(task_processor_config=TASK_PROCESSOR_CONFIG, handler=consumer)

serve.run(
    target=app,
    blocking=False
)
```

## Step 4: Test the service

Let's execute the client code, which calls the Ray Serve application to process PDFs and then polls for results using task IDs. First, define the base URL to query and import the required modules.


```python
import time
from typing import Dict, Any

import requests

# Base URL of your Ray Serve/FastAPI app
BASE_URL = "http://localhost:8000".rstrip("/")
```

Submit two tasks. The application returns a `task_id` for each request that you can use to poll for results.


```python
def process_pdf(pdf_url: str, max_summary_paragraphs: int = 3) -> str:
    """
    Submit a PDF processing task and return the task_id.
    """
    response = requests.post(
        f"{BASE_URL}/process",
        json={
            "pdf_url": pdf_url,
            "max_summary_paragraphs": max_summary_paragraphs,
        },
    )
    response.raise_for_status()
    data = response.json()
    return data["task_id"]


pdf_urls = [
    "https://www.w3.org/WAI/ER/tests/xhtml/testfiles/resources/pdf/dummy.pdf",
    "https://arxiv.org/pdf/1706.03762.pdf",
]

task_ids = []

for i, url in enumerate(pdf_urls, 1):
        task_id = process_pdf(url)
        task_ids.append((task_id, url))
        print(f"   ✓ Task {i} submitted: {task_id}  ({url})")
```

Next, poll the Ray Serve application using the `task_id` obtained in the previous step to retrieve the result.


```python
def get_task_status(task_id: str) -> Dict[str, Any]:
    response = requests.get(f"{BASE_URL}/status/{task_id}")
    response.raise_for_status()
    return response.json()

for i, (task_id, url) in enumerate(task_ids, 1):
        print(f"\nTask {i} ({url.split('/')[-1]}):")
        result = get_task_status(task_id)
        res = result.get("result")
        if res:
            print(f"   ✓ Complete: {res.get('page_count')} pages, {res.get('word_count')} words")
            print(f"   ✓ Processing time: {res.get('processing_time_seconds')}s")
        else:
            print("   ✗ No result payload found in response.")
```

## Deploy to Anyscale

To deploy this application to production on Anyscale:

1. Update Redis configuration in your server code with your production Redis instance
2. Deploy using the Anyscale CLI:

   ```bash
   anyscale service deploy -f service.yaml
   ```

3. Get your service URL:

   ```bash
   anyscale service status
   ```

## Learn More

- [Ray Serve Documentation](https://docs.ray.io/en/latest/serve/index.html)
- [Asynchronous Inference Guide](https://docs.ray.io/en/master/serve/asynchronous-inference.html)
- [Celery Documentation](https://docs.celeryq.dev/)
- [Anyscale Platform](https://docs.anyscale.com/)
