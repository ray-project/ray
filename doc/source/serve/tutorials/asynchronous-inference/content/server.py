"""
Ray Serve Asynchronous Inference - PDF Processing Example

This example shows how to build async inference services that handle long-running
tasks without blocking HTTP responses. Tasks are queued to Redis and processed by
background workers.
"""

import io
import logging
import time
from typing import Dict, Any

import requests
from fastapi import FastAPI
from pydantic import BaseModel, HttpUrl
from PyPDF2 import PdfReader

from ray import serve
from ray.serve.handle import DeploymentHandle
from ray.serve.schema import CeleryAdapterConfig, TaskProcessorConfig
from ray.serve.task_consumer import (
    instantiate_adapter_from_config,
    task_consumer,
    task_handler,
)

logger = logging.getLogger("ray.serve")

TASK_PROCESSOR_CONFIG = TaskProcessorConfig(
    queue_name="pdf_processing_queue",
    adapter_config=CeleryAdapterConfig(
        broker_url="redis://asynchronous-template-testing-redis.onvfjm.ng.0001.usw2.cache.amazonaws.com:6379/0",
        backend_url="redis://asynchronous-template-testing-redis.onvfjm.ng.0001.usw2.cache.amazonaws.com:6379/0",
    ),
    max_retries=3,
    failed_task_queue_name="failed_pdfs",
    unprocessable_task_queue_name="invalid_pdfs",
)

# ============================================================================
# Request Model
# ============================================================================

class ProcessPDFRequest(BaseModel):
    """Request schema for PDF processing."""
    pdf_url: HttpUrl
    max_summary_paragraphs: int = 3


# ============================================================================
# Task Consumer - Background Worker
# ============================================================================

@serve.deployment(num_replicas=2, max_ongoing_requests=5)
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

        Args:
            pdf_url: URL to the PDF file
            max_summary_paragraphs: Number of paragraphs for summary (default: 3)

        Returns:
            Dictionary with extracted text, summary, and metadata
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


# ============================================================================
# HTTP API - Ingress Deployment
# ============================================================================

fastapi_app = FastAPI(title="Async PDF Processing API")


@serve.deployment()
@serve.ingress(fastapi_app)
class AsyncPDFAPI:
    """
    HTTP API for submitting and checking PDF processing tasks.

    Endpoints:
    - POST /process: Submit a PDF processing task
    - GET /status/{task_id}: Check task status and get results
    """

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


# ============================================================================
# Application Setup
# ============================================================================

def build_app():
    """Build and configure the Ray Serve application."""
    # Deploy background worker
    consumer = PDFProcessor.bind()

    # Deploy HTTP API
    api = AsyncPDFAPI.bind(TASK_PROCESSOR_CONFIG, consumer)

    return api


# Entry point for Ray Serve
app = build_app()
