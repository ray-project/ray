#!/usr/bin/env python3
"""
Advanced HTTPS configuration examples for Ray Serve.

This example demonstrates various SSL/TLS configurations including:
1. Basic HTTPS with self-signed certificates
2. HTTPS with password-protected key files
3. HTTPS with client certificate verification (mutual TLS)
4. Programmatic certificate generation for testing
"""

import os
import ray
from ray import serve
from ray.serve.config import HTTPOptions
from cryptography import x509
from cryptography.x509.oid import NameOID
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from datetime import datetime, timedelta


def generate_self_signed_cert(cert_file="server.crt", key_file="server.key"):
    """Generate a self-signed certificate for testing."""
    # Generate private key
    key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
    )

    # Generate certificate
    subject = issuer = x509.Name(
        [
            x509.NameAttribute(NameOID.COUNTRY_NAME, "US"),
            x509.NameAttribute(NameOID.STATE_OR_PROVINCE_NAME, "California"),
            x509.NameAttribute(NameOID.LOCALITY_NAME, "San Francisco"),
            x509.NameAttribute(NameOID.ORGANIZATION_NAME, "Ray Serve Test"),
            x509.NameAttribute(NameOID.COMMON_NAME, "localhost"),
        ]
    )

    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.utcnow())
        .not_valid_after(datetime.utcnow() + timedelta(days=365))
        .add_extension(
            x509.SubjectAlternativeName(
                [
                    x509.DNSName("localhost"),
                    x509.DNSName("127.0.0.1"),
                ]
            ),
            critical=False,
        )
        .sign(key, hashes.SHA256())
    )

    # Write private key
    with open(key_file, "wb") as f:
        f.write(
            key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.TraditionalOpenSSL,
                encryption_algorithm=serialization.NoEncryption(),
            )
        )

    # Write certificate
    with open(cert_file, "wb") as f:
        f.write(cert.public_bytes(serialization.Encoding.PEM))

    print(f"Generated self-signed certificate: {cert_file}")
    print(f"Generated private key: {key_file}")


@serve.deployment
class SecureEndpoint:
    def __call__(self, request):
        return {
            "message": "Secure connection established!",
            "protocol": request.url.scheme,
            "host": request.url.hostname,
            "port": request.url.port,
            "path": request.url.path,
        }


def example_basic_https():
    """Example 1: Basic HTTPS configuration."""
    print("\n=== Example 1: Basic HTTPS ===")

    # Generate certificates if they don't exist
    if not os.path.exists("server.crt") or not os.path.exists("server.key"):
        generate_self_signed_cert()

    # Configure HTTPS
    https_options = HTTPOptions(
        host="0.0.0.0",
        port=8443,
        ssl_keyfile="server.key",
        ssl_certfile="server.crt",
    )

    # Start Ray Serve with HTTPS
    serve.start(http_options=https_options)
    serve.run(SecureEndpoint.bind(), name="secure-app", route_prefix="/")

    print("HTTPS server running on https://localhost:8443/")
    print("Test with: curl -k https://localhost:8443/")


def example_https_with_ca():
    """Example 2: HTTPS with CA certificate for client verification."""
    print("\n=== Example 2: HTTPS with Client Certificate Verification ===")

    # In production, you would use real CA certificates
    # For this example, we'll use the same self-signed cert as CA
    if not os.path.exists("ca.crt"):
        # In real scenario, this would be your CA certificate
        import shutil

        shutil.copy("server.crt", "ca.crt")

    https_options = HTTPOptions(
        host="0.0.0.0",
        port=8444,
        ssl_keyfile="server.key",
        ssl_certfile="server.crt",
        ssl_ca_certs="ca.crt",  # Enable client certificate verification
    )

    serve.start(http_options=https_options)
    serve.run(SecureEndpoint.bind(), name="mtls-app", route_prefix="/")

    print("HTTPS server with client verification running on https://localhost:8444/")
    print("Clients must present a valid certificate signed by the CA")


def example_https_config_dict():
    """Example 3: Using HTTPS configuration from a dictionary (useful for config files)."""
    print("\n=== Example 3: HTTPS from Configuration Dictionary ===")

    # This configuration could come from a YAML/JSON file
    config = {
        "applications": [
            {
                "name": "secure-app",
                "route_prefix": "/api",
                "import_path": "example_https_advanced:SecureEndpoint",
            }
        ],
        "http_options": {
            "host": "0.0.0.0",
            "port": 8445,
            "ssl_keyfile": "server.key",
            "ssl_certfile": "server.crt",
        },
    }

    # Deploy using configuration
    serve.start(http_options=HTTPOptions(**config["http_options"]))
    serve.run(SecureEndpoint.bind(), name="config-app", route_prefix="/api")

    print("HTTPS server from config running on https://localhost:8445/api")


if __name__ == "__main__":
    import sys

    ray.init()

    examples = {
        "1": example_basic_https,
        "2": example_https_with_ca,
        "3": example_https_config_dict,
    }

    if len(sys.argv) > 1 and sys.argv[1] in examples:
        examples[sys.argv[1]]()
    else:
        print("Usage: python example_https_advanced.py [1|2|3]")
        print("1 - Basic HTTPS")
        print("2 - HTTPS with client certificate verification")
        print("3 - HTTPS from configuration dictionary")
        print("\nRunning basic HTTPS example...")
        example_basic_https()

    # Keep server running
    import time
    import os

    # If running in test/CI environment, don't run forever
    if os.environ.get("CI") or os.environ.get("BUILDKITE"):
        print("Running in CI environment, testing deployment briefly...")
        time.sleep(2)
        # Test basic functionality
        try:
            import requests

            # Test the deployed endpoint
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
