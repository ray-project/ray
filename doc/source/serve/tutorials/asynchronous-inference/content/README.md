# Asynchronous Inference with Ray Serve

**⏱️ Time to complete**: 30 minutes

This template demonstrates how to build scalable asynchronous inference services using Ray Serve. Learn how to handle long-running PDF processing tasks without blocking HTTP responses, using Celery task queues and Redis as a message broker.

## Overview

Traditional synchronous APIs block until processing completes, causing timeouts for long-running tasks. Ray Serve's asynchronous inference pattern decouples request lifetime from compute time by:

1. Accepting HTTP requests and immediately returning a task ID
2. Enqueuing work to background processors (Celery workers)
3. Allowing clients to poll for status and retrieve results

This example implements a **PDF processing service** that extracts text and generates summaries from PDF documents.

## Use Cases

- PDF document processing (text extraction, summarization)
- Video transcoding and analysis
- Large-scale data transformations
- Batch inference on expensive models
- Any task where response time > 30 seconds

## Prerequisites

- Python 3.9+
- Ray 2.50.0+
- Redis (for message broker and result backend)

## Quick Start

### 1. Setup Redis

Redis serves as both the message broker (task queue) and result backend.

**Docker (Recommended for local testing)**
```bash
docker run -d -p 6379:6379 redis:latest
```

**Alternative: Install locally**
- macOS: `brew install redis && brew services start redis`
- Ubuntu: `sudo apt-get install redis-server && sudo systemctl start redis`
- [Official Redis Installation Guide](https://redis.io/docs/getting-started/installation/)

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Run the Service Locally

```bash
# Start Ray Serve with the async inference application
serve run server:app

# The service will be available at http://localhost:8000
```

### 4. Test the Service

Run the example client in a separate terminal:

```bash
python client.py
```

**Expected output:**
```
======================================================================
Asynchronous PDF Processing Example
======================================================================

======================================================================
Step 1: Submitting PDF processing tasks
======================================================================
   ✓ Task 1 submitted: 5f8e9a1b-2c3d-4e5f-6a7b-8c9d0e1f2a3b
   ✓ Task 2 submitted: 1a2b3c4d-5e6f-7a8b-9c0d-1e2f3a4b5c6d

======================================================================
Step 2: Waiting for tasks to complete
======================================================================

Task 1 (dummy.pdf):
  Task status: STARTED, waiting...
   ✓ Complete: 1 pages, 89 words
   ✓ Processing time: 2.1s

Task 2 (1706.03762.pdf):
  Task status: STARTED, waiting...
   ✓ Complete: 15 pages, 9,453 words
   ✓ Processing time: 5.3s

======================================================================
Example complete!
======================================================================
```

#### Understanding the Client Code

The example client (`client.py`) demonstrates the async workflow:

**1. Submit tasks** - Returns immediately with a task ID:
```python
client = AsyncPDFClient()
task_id = client.process_pdf("https://example.com/doc.pdf")
# Returns: "5f8e9a1b-2c3d-4e5f-6a7b-8c9d0e1f2a3b"
```

**2. Poll for status** - Check task progress:
```python
status = client.get_task_status(task_id)
# Returns: {"status": "PENDING", "result": None}
# Later: {"status": "SUCCESS", "result": {...}}
```

**3. Wait for completion** - Automatically polls until done:
```python
result = client.wait_for_task(task_id, timeout=60.0)
# Returns full results when task completes
```

### 5. Alternative: Test with cURL

You can also test the API directly without the client:

**Submit a task:**
```bash
curl -X POST http://localhost:8000/process \
  -H "Content-Type: application/json" \
  -d '{
    "pdf_url": "https://www.w3.org/WAI/ER/tests/xhtml/testfiles/resources/pdf/dummy.pdf",
    "max_summary_paragraphs": 3
  }'

# Response: {"task_id": {"id": "abc123..."}, "status": "queued"}
```

**Check status:**
```bash
curl http://localhost:8000/status/abc123...

# Response: {"status": "SUCCESS", "result": {...}}
```

## Deploy to Anyscale

### 1. Update Redis Configuration

Edit `server.py` to use your production Redis instance:

```python
adapter_config=CeleryAdapterConfig(
    broker_url=os.getenv("REDIS_BROKER_URL", "redis://localhost:6379/0"),
    backend_url=os.getenv("REDIS_RESULT_BACKEND", "redis://localhost:6379/0"),
)
```

### 2. Deploy Using Anyscale CLI

```bash
# Deploy the service
anyscale service deploy -f service.yaml

# Get the service URL
anyscale service status

# Test the deployed service
curl -X POST https://your-service-url.anyscale.com/process \
  -H "Content-Type: application/json" \
  -d '{"pdf_url": "https://www.w3.org/WAI/ER/tests/xhtml/testfiles/resources/pdf/dummy.pdf"}'
```

## Architecture

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

### Key Components

**1. Task Consumer (`@task_consumer` decorator)**
- Transforms a Ray Serve deployment into a Celery worker
- Processes tasks from Redis queue in the background
- Scales independently with `num_replicas`
- Limits concurrent tasks with `max_ongoing_requests`

**2. Task Handler (`@task_handler` decorator)**
- Method that processes a specific task type (`process_pdf`)
- Downloads PDF from URL, extracts text, generates summary
- Returns results stored in Redis backend
- Automatic retry on failure (up to `max_retries`)

**3. Ingress API (`AsyncPDFAPI`)**
- HTTP endpoints for task submission
- Uses `instantiate_adapter_from_config()` to create task adapter
- Enqueues tasks with `enqueue_task_sync()`
- Checks status with `get_task_status_sync()`

**4. Dead Letter Queues**
- `failed_task_queue_name`: Tasks that exceeded max retries
- `unprocessable_task_queue_name`: Tasks with no matching handler

## How It Works

### PDF Processing Flow

1. **Client submits request** with PDF URL
2. **API validates** and enqueues task to Redis
3. **Returns task_id** immediately (HTTP 200)
4. **Background worker** picks up task from queue
5. **Downloads** PDF from URL
6. **Extracts** text using PyPDF2
7. **Generates** summary (first N paragraphs)
8. **Stores** results in Redis
9. **Client polls** `/status/{task_id}` to get results

### Task States

- **PENDING**: Task queued, waiting for worker
- **STARTED**: Worker picked up task, processing
- **SUCCESS**: Task completed successfully
- **FAILURE**: Task failed (error in handler or max retries exceeded)

## Configuration

### Task Processor Config

```python
TaskProcessorConfig(
    queue_name="pdf_processing_queue",        # Main task queue
    adapter_config=CeleryAdapterConfig(...),  # Broker/backend config
    max_retries=3,                            # Retry failed tasks
    failed_task_queue_name="failed_pdfs",     # DLQ for failed tasks
    unprocessable_task_queue_name="invalid_pdfs",  # DLQ for bad tasks
)
```

### Scaling Configuration

Control throughput by adjusting these parameters in `service.yaml`:

```yaml
deployments:
  - name: PDFProcessor
    num_replicas: 2              # Number of worker instances
    max_ongoing_requests: 5      # Concurrent tasks per replica
    # Total throughput: num_replicas × max_ongoing_requests = 10 concurrent tasks
```


## Customization

### Modify Summary Generation

Change the summary algorithm in `server.py`:

```python
# Current: First N paragraphs
paragraphs = [p.strip() for p in full_text.split("\n\n") if p.strip()]
summary = "\n\n".join(paragraphs[:max_summary_paragraphs])

# Alternative: First N sentences
sentences = full_text.split(". ")
summary = ". ".join(sentences[:max_summary_paragraphs]) + "."

# Alternative: Use an AI model
from transformers import pipeline
summarizer = pipeline("summarization")
summary = summarizer(full_text, max_length=200)[0]["summary_text"]
```

### Add More Task Handlers

Add additional processing capabilities:

```python
@task_handler(name="extract_images")
def extract_images(self, pdf_url: str) -> Dict[str, Any]:
    """Extract images from a PDF."""
    # Download PDF
    response = requests.get(pdf_url, timeout=30)
    pdf_file = io.BytesIO(response.content)
    pdf_reader = PdfReader(pdf_file)

    # Extract images (implementation depends on use case)
    images = []
    for page in pdf_reader.pages:
        if "/XObject" in page["/Resources"]:
            xObject = page["/Resources"]["/XObject"].get_object()
            for obj in xObject:
                if xObject[obj]["/Subtype"] == "/Image":
                    images.append(obj)

    return {
        "status": "success",
        "image_count": len(images),
        "images": images,
    }
```

Then add corresponding API endpoint:

```python
@fastapi_app.post("/extract-images")
async def extract_images(self, request: ProcessPDFRequest):
    task_id = self.adapter.enqueue_task_sync(
        task_name="extract_images",
        kwargs={"pdf_url": str(request.pdf_url)},
    )
    return {"task_id": task_id, "status": "queued"}
```

## Error Handling

### Automatic Retries

Tasks automatically retry on failure up to `max_retries` times:

```python
TaskProcessorConfig(
    max_retries=3,  # Retry up to 3 times
)
```


## Learn More

- [Ray Serve Documentation](https://docs.ray.io/en/latest/serve/index.html)
- [Asynchronous Inference Guide](https://docs.ray.io/en/master/serve/asynchronous-inference.html)
- [Celery Documentation](https://docs.celeryq.dev/)
- [Redis Documentation](https://redis.io/docs/)
- [PyPDF2 Documentation](https://pypdf2.readthedocs.io/)
- [Anyscale Platform](https://docs.anyscale.com/)

## Support

For issues or questions:
- [Ray Serve GitHub Issues](https://github.com/ray-project/ray/issues)
- [Ray Discourse Community](https://discuss.ray.io/)
- [Anyscale Support](https://docs.anyscale.com/support)
