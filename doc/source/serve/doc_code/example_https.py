#!/usr/bin/env python3
"""
Example demonstrating HTTPS support in Ray Serve.

Before running this example, you need SSL certificates.
For testing, you can generate self-signed certificates:

    # Generate a private key
    openssl genrsa -out server.key 2048

    # Generate a certificate signing request
    openssl req -new -key server.key -out server.csr

    # Generate a self-signed certificate (valid for 365 days)
    openssl x509 -req -days 365 -in server.csr -signkey server.key -out server.crt

Then run this example:
    python example_https.py

And test with:
    # For self-signed certificates, use -k to skip certificate verification
    curl -k https://localhost:8443/
"""

import ray
from ray import serve
from ray.serve import Application
from ray.serve.config import HTTPOptions


@serve.deployment
class HelloWorld:
    def __call__(self):
        return "Hello from HTTPS Ray Serve!"


# Example 1: Basic HTTPS configuration
def create_https_app() -> Application:
    return HelloWorld.bind()


if __name__ == "__main__":
    # Initialize Ray
    ray.init()

    # Configure HTTPS options
    https_options = HTTPOptions(
        host="0.0.0.0",
        port=8443,  # Common HTTPS port
        ssl_keyfile="server.key",  # Path to your SSL key file
        ssl_certfile="server.crt",  # Path to your SSL certificate file
        # Optional: ssl_keyfile_password="your_password" if key is encrypted
        # Optional: ssl_ca_certs="ca.crt" for client certificate verification
    )

    # Start Ray Serve with HTTPS
    serve.start(http_options=https_options)

    # Deploy the application
    app = create_https_app()
    serve.run(app, name="https-example", route_prefix="/")

    print("HTTPS server is running!")
    print("Test with: curl -k https://localhost:8443/")
    print("(Use -k flag for self-signed certificates)")

    # Keep the server running for testing
    import time
    import os

    # If running in test/CI environment, don't run forever
    if os.environ.get("CI") or os.environ.get("BUILDKITE"):
        print("Running in CI environment, testing deployment briefly...")
        time.sleep(2)
        # Test that the server is working
        try:
            import requests

            response = requests.get("https://localhost:8443/", verify=False)
            print(f"Test response: {response.status_code}")
        except Exception as e:
            print(f"Test failed: {e}")
        serve.shutdown()
        ray.shutdown()
    else:
        # Interactive mode - run until interrupted
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\nShutting down...")
            serve.shutdown()
            ray.shutdown()
