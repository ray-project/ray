# Asynchronous Inference with Ray Serve

**⏱️ Time to complete**: 30 minutes

This template demonstrates how to build scalable asynchronous inference services using Ray Serve. Learn how to handle long-running PDF processing tasks without blocking HTTP responses, using Celery task queues and Redis as a message broker.

## Overview

Traditional synchronous APIs block until processing completes, causing timeouts for long-running tasks. Ray Serve's asynchronous inference pattern decouples request lifetime from compute time by:

1. Accepting HTTP requests and immediately returning a task ID
2. Enqueuing work to background processors (Celery workers)
3. Allowing clients to poll for status and retrieve results

This example implements a **PDF processing service** that extracts text and generates summaries from PDF documents.

## Prerequisites

- Python 3.9+
- Ray 2.50.0+
- Redis (for message broker and result backend)

## Step 1: Setup Redis

Redis serves as both the message broker (task queue) and result backend.

**Install and start Redis (Google Colab compatible)**


```python
# Install and start Redis server
!sudo apt-get update -qq
!sudo apt-get install -y redis-server
!redis-server --port 6399 --save "" --appendonly no --daemonize yes

# Verify Redis is running
!redis-cli -p 6399 ping
```

**Alternative methods:**

- macOS: `brew install redis && brew services start redis`
- Docker: `docker run -d -p 6379:6379 redis:latest`
- [Official Redis Installation Guide](https://redis.io/docs/getting-started/installation/)

## Step 2: Install Dependencies


```python
!pip install -q ray[serve-async-inference]>=2.50.0 requests>=2.31.0 PyPDF2>=3.0.0 celery[redis]
```


## Step 3: Start the Ray Serve Application

First, let's run the complete example to see it in action. We have the server code written in `server.py`, where we have mentioned our app with producer and consumer deployments.


```python
from ray import serve
from server import app

serve.run(
    target=app,
    blocking=False
)
```

### Understanding the Server Code

Now let's break down the key components of `server.py` that make asynchronous inference work:

#### 1. Configure the Task Adapter

First, we configure the Celery adapter to connect to Redis:

```python
from ray.serve.asynchronous_inference.celery import CeleryAdapterConfig
from ray.serve.asynchronous_inference import TaskProcessorConfig

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
```

This tells Ray Serve where to send tasks (broker) and store results (backend).

#### 2. Create the Task Consumer Deployment

The `PDFProcessor` deployment processes PDF tasks from the queue:

```python
@serve.deployment(num_replicas=2, max_ongoing_requests=5)
@task_consumer(
    TaskProcessorConfig(
        queue_name="pdf_processing_queue",
        adapter_config=adapter_config,
        max_retries=3,
    )
)
class PDFProcessor:
    @task_handler(name="process_pdf")
    def process_pdf(self, pdf_url: str, max_summary_paragraphs: int = 3):
        # Download and process PDF
        response = requests.get(pdf_url)
        pdf_reader = PdfReader(BytesIO(response.content))
        
        # Extract text from all pages
        full_text = ""
        for page in pdf_reader.pages:
            full_text += page.extract_text()
        
        # Generate summary (first N paragraphs)
        paragraphs = [p.strip() for p in full_text.split("\n\n") if p.strip()]
        summary = "\n\n".join(paragraphs[:max_summary_paragraphs])
        
        return {
            "status": "success",
            "page_count": len(pdf_reader.pages),
            "word_count": len(full_text.split()),
            "summary": summary,
            "processing_time_seconds": time.time() - start_time,
        }
```

Key points:
- `num_replicas=2` scales to 2 worker instances for parallel processing
- `@task_consumer` decorator makes this deployment consume tasks from the queue
- `@task_handler` decorator registers the `process_pdf` method as a task handler

#### 3. Create the API Ingress Deployment

The `AsyncPDFAPI` deployment handles HTTP requests and enqueues tasks:

```python
@serve.deployment
class AsyncPDFAPI:
    def __init__(self, pdf_processor_handle):
        self.pdf_processor_handle = pdf_processor_handle
        self.adapter = CeleryAdapter(adapter_config)
    
    @serve.route("/process", methods=["POST"])
    async def process_pdf(self, request: Request):
        data = await request.json()
        
        # Enqueue task and immediately return task ID
        task_id = await self.adapter.enqueue_task(
            task_name="process_pdf",
            kwargs={
                "pdf_url": data["pdf_url"],
                "max_summary_paragraphs": data.get("max_summary_paragraphs", 3),
            },
        )
        
        return {"task_id": task_id.dict(), "status": "PENDING"}
    
    @serve.route("/status/{task_id}", methods=["GET"])
    async def get_status(self, task_id: str):
        # Check task status in Redis
        status = await self.adapter.get_task_status(TaskID(id=task_id))
        
        response = {
            "task_id": task_id,
            "status": status.state.value,
        }
        
        if status.state == TaskState.SUCCESS:
            response["result"] = status.result
        elif status.state == TaskState.FAILURE:
            response["error"] = str(status.result)
        
        return response
```

Key points:
- `/process` endpoint enqueues tasks and returns immediately with a task ID
- `/status/{task_id}` endpoint polls Redis to check task status
- No blocking - responses are instant regardless of processing time

#### 4. Bind Deployments into an Application

Finally, we wire everything together:

```python
pdf_processor = PDFProcessor.bind()

app = AsyncPDFAPI.bind(pdf_processor)
```

The `pdf_processor` handle is passed to the API so it knows which deployment's queue to enqueue tasks to.

## Step 4: Test the Service

Now we will execute the `client.py`, which basically calls our above-created ray serve application to process the PDF, and then poll those task ids for the result.


```python
# Run the client to test the async PDF processing service
!python client.py
```

### Understanding the Client Code

Now let's break down how the async workflow works. We'll use the client methods interactively:

```python
import requests
import time

BASE_URL = "http://localhost:8000"
```



#### 1. Submit a PDF Processing Task

Submit returns immediately with a task ID, without waiting for processing:

```python
pdf_url = "https://www.w3.org/WAI/ER/tests/xhtml/testfiles/resources/pdf/dummy.pdf"

response = requests.post(
    f"{BASE_URL}/process",
    json={
        "pdf_url": pdf_url,
        "max_summary_paragraphs": 2
    }
)

task_data = response.json()
task_id = task_data["task_id"]["id"]

print(f"✓ Task submitted!")
print(f"  Task ID: {task_id}")
print(f"  Status: {task_data['status']}")
```

#### 2. Poll for Task Status

Check the task status to see if it's complete:

```python
# Check status (may need to run this cell multiple times)
response = requests.get(f"{BASE_URL}/status/{task_id}")
status_data = response.json()

print(f"Task status: {status_data['status']}")

if status_data['status'] == 'SUCCESS':
    result = status_data['result']
    print(f"\n✓ Complete!")
    print(f"  Pages: {result['page_count']}")
    print(f"  Words: {result['word_count']}")
    print(f"  Time: {result['processing_time_seconds']}s")
elif status_data['status'] == 'FAILURE':
    print(f"\n✗ Failed: {status_data.get('error')}")
else:
    print(f"  Still processing... (Status: {status_data['status']})")
```

#### 3. Wait for Completion

Or use a polling loop to automatically wait:

```python
# Submit a new task and wait for completion
pdf_url = "https://arxiv.org/pdf/1706.03762.pdf"

response = requests.post(
    f"{BASE_URL}/process",
    json={"pdf_url": pdf_url, "max_summary_paragraphs": 3}
)

task_id = response.json()["task_id"]["id"]
print(f"Task submitted: {task_id}\n")

# Poll until complete
max_attempts = 40
for attempt in range(max_attempts):
    response = requests.get(f"{BASE_URL}/status/{task_id}")
    status_data = response.json()
    
    if status_data['status'] == 'SUCCESS':
        result = status_data['result']
        print(f"\n✓ Complete!")
        print(f"  Pages: {result['page_count']}")
        print(f"  Words: {result['word_count']}")
        print(f"  Time: {result['processing_time_seconds']}s")
        print(f"\n  Summary preview:")
        print(f"  {result['summary'][:200]}...")
        break
    elif status_data['status'] == 'FAILURE':
        print(f"✗ Failed: {status_data.get('error')}")
        break
    elif attempt % 5 == 0:
        print(f"  Still processing... ({status_data['status']})")
    
    time.sleep(3)
```

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

## Key Concepts

### Task Consumer

The `@task_consumer` decorator transforms a Ray Serve deployment into a Celery worker that processes tasks from a queue:

```python
@serve.deployment(num_replicas=2, max_ongoing_requests=5)
@task_consumer(
    TaskProcessorConfig(
        queue_name="pdf_processing_queue",
        adapter_config=CeleryAdapterConfig(...),
        max_retries=3,
    )
)
class PDFProcessor:
    ...
```

### Task Handler

The `@task_handler` decorator marks a method that processes a specific task type:

```python
@task_handler(name="process_pdf")
def process_pdf(self, pdf_url: str, max_summary_paragraphs: int = 3):
    # Download PDF, extract text, generate summary
    return {"status": "success", ...}
```

### Task Adapter

The adapter provides methods to interact with the task queue:

```python
# Enqueue a task
task_id = adapter.enqueue_task_sync(
    task_name="process_pdf",
    kwargs={"pdf_url": url}
)

# Check status
status = adapter.get_task_status_sync(task_id)
```

## Deploy to Anyscale

1. Update Redis configuration in `server.py` with your production Redis instance
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
