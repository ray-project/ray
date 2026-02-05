"""
Example client for testing asynchronous PDF processing.

Demonstrates:
1. Submitting PDF processing tasks
2. Polling for task status
3. Retrieving results when complete
"""

import time
import argparse
from typing import Dict, Any, Optional

import requests


class AsyncPDFClient:
    """Client for interacting with the async PDF processing API."""

    def __init__(self, base_url: str, auth_header: Optional[str] = None):
        """
        Initialize the client.

        :param base_url: Base URL of the async PDF processing API
        :param auth_header: Optional Authorization header value, e.g. "Bearer TOKEN"
        """
        self.base_url = base_url.rstrip("/")
        self.headers: Dict[str, str] = {}
        if auth_header:
            # Use the provided value as the Authorization header
            self.headers["Authorization"] = auth_header

    def process_pdf(self, pdf_url: str, max_summary_paragraphs: int = 3) -> str:
        """
        Submit a PDF processing task.
        """
        response = requests.post(
            f"{self.base_url}/process",
            json={
                "pdf_url": pdf_url,
                "max_summary_paragraphs": max_summary_paragraphs,
            },
            headers=self.headers,
        )
        response.raise_for_status()
        return response.json()["task_id"]

    def get_task_status(self, task_id: str) -> Dict[str, Any]:
        """
        Get the current status of a task.
        """
        response = requests.get(
            f"{self.base_url}/status/{task_id}",
            headers=self.headers,
        )
        response.raise_for_status()
        return response.json()

    def wait_for_task(
        self,
        task_id: str,
        poll_interval: float = 2.0,
        timeout: float = 120.0,
    ) -> Dict[str, Any]:
        """
        Wait for a task to complete by polling its status.
        """
        start_time = time.time()

        while True:
            # Check if we've exceeded the timeout
            if time.time() - start_time > timeout:
                raise TimeoutError(f"Task {task_id} timed out after {timeout}s")

            # Get current task status
            status = self.get_task_status(task_id)
            state = status["status"]

            if state == "SUCCESS":
                return status
            elif state == "FAILURE":
                raise RuntimeError(f"Task failed: {status.get('error')}")
            elif state in ["PENDING", "STARTED"]:
                print(f"  Task status: {state}, waiting...")
                time.sleep(poll_interval)
            else:
                print(f"  Unknown status: {state}, waiting...")
                time.sleep(poll_interval)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Asynchronous PDF Processing Client"
    )
    parser.add_argument(
        "base_url",
        help="Base URL of the async PDF processing API, e.g. http://localhost:8000",
    )
    parser.add_argument(
        "-H",
        "--header",
        dest="auth_header",
        help="Authorization header value, e.g. 'Bearer YOUR_TOKEN'",
    )
    return parser.parse_args()


def main():
    """Run example PDF processing tasks."""
    args = parse_args()

    client = AsyncPDFClient(base_url=args.base_url, auth_header=args.auth_header)

    print("=" * 70)
    print("Asynchronous PDF Processing Example")
    print("=" * 70)

    # Example: Process multiple PDFs in parallel
    print("\n" + "=" * 70)
    print("Step 1: Submitting PDF processing tasks")
    print("=" * 70)

    pdf_urls = [
        "https://www.w3.org/WAI/ER/tests/xhtml/testfiles/resources/pdf/dummy.pdf",
        "https://arxiv.org/pdf/1706.03762.pdf",
    ]

    # Submit all tasks
    task_ids = []
    for i, url in enumerate(pdf_urls, 1):
        try:
            task_id = client.process_pdf(url)
            task_ids.append((task_id, url))
            print(f"   ✓ Task {i} submitted: {task_id}")
        except Exception as e:
            print(f"   ✗ Task {i} failed to submit: {e}")

    # Wait for all tasks to complete
    print("\n" + "=" * 70)
    print("Step 2: Waiting for tasks to complete")
    print("=" * 70)

    for i, (task_id, url) in enumerate(task_ids, 1):
        print(f"\nTask {i} ({url.split('/')[-1]}):")
        try:
            result = client.wait_for_task(task_id, timeout=60.0)
            if result["result"]:
                res = result["result"]
                print(
                    f"   ✓ Complete: {res['page_count']} pages, {res['word_count']} words"
                )
                print(f"   ✓ Processing time: {res['processing_time_seconds']}s")
        except Exception as e:
            print(f"   ✗ Error: {e}")

    print("\n" + "=" * 70)
    print("Example complete!")
    print("=" * 70)


if __name__ == "__main__":
    main()
