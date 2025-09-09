import asyncio
import json
import os
import ssl
import tempfile

import pytest
import requests
import websockets

import ray
from ray import serve
from ray._private.tls_utils import generate_self_signed_tls_certs
from ray.serve.config import HTTPOptions


@pytest.fixture(scope="session")
def ssl_cert_and_key():
    """Generate SSL certificates using Ray's built-in utilities for testing."""
    # Generate certificate and key using Ray's utility
    cert_contents, key_contents = generate_self_signed_tls_certs()

    # Create temp directory that persists for the session
    temp_dir = tempfile.mkdtemp(prefix="ray_serve_https_test_")

    # Write server certificate and key
    cert_path = os.path.join(temp_dir, "server.crt")
    key_path = os.path.join(temp_dir, "server.key")

    with open(cert_path, "w") as f:
        f.write(cert_contents)
    with open(key_path, "w") as f:
        f.write(key_contents)

    yield {
        "key_path": key_path,
        "cert_path": cert_path,
        "temp_dir": temp_dir,
    }

    # Cleanup
    import shutil

    try:
        shutil.rmtree(temp_dir)
    except Exception:
        pass  # Ignore cleanup errors


@pytest.fixture
def https_serve_instance(ssl_cert_and_key):
    """Start Ray Serve with HTTPS enabled."""
    # Ensure Ray is shutdown before starting
    try:
        ray.shutdown()
    except Exception:
        pass

    # Disable runtime env upload (dashboard should work now that it's built)
    ray.init(runtime_env={"working_dir": None})
    serve.start(
        http_options=HTTPOptions(
            ssl_keyfile=ssl_cert_and_key["key_path"],
            ssl_certfile=ssl_cert_and_key["cert_path"],
        )
    )
    yield serve
    serve.shutdown()
    ray.shutdown()


class TestHTTPSProxy:
    def test_https_basic_deployment(self, https_serve_instance):
        """Test basic HTTPS deployment functionality."""

        @serve.deployment
        def hello():
            return "Hello HTTPS!"

        serve.run(hello.bind())

        # Test HTTPS request with certificate verification disabled for self-signed cert
        response = requests.get(
            "https://localhost:8000/hello",
            verify=False,  # Skip cert verification for self-signed
        )
        assert response.status_code == 200
        assert response.text == "Hello HTTPS!"

    def test_https_vs_http_requests(self, https_serve_instance):
        """Test that HTTP requests fail when HTTPS is enabled."""

        @serve.deployment
        def echo():
            return "echo"

        serve.run(echo.bind())

        # HTTPS request should succeed
        https_response = requests.get("https://localhost:8000/echo", verify=False)
        assert https_response.status_code == 200

        # HTTP request should fail with connection error
        with pytest.raises(requests.exceptions.ConnectionError):
            requests.get("http://localhost:8000/echo", timeout=5)

    def test_https_with_fastapi_deployment(self, https_serve_instance):
        """Test HTTPS with FastAPI-based deployment."""
        from fastapi import FastAPI

        app = FastAPI()

        @app.get("/items/{item_id}")
        async def read_item(item_id: int):
            return {"item_id": item_id, "secure": True}

        @serve.deployment
        @serve.ingress(app)
        class FastAPIDeployment:
            pass

        serve.run(FastAPIDeployment.bind())

        response = requests.get("https://localhost:8000/items/42", verify=False)
        assert response.status_code == 200
        assert response.json() == {"item_id": 42, "secure": True}

    def test_https_concurrent_requests(self, https_serve_instance):
        """Test HTTPS with concurrent requests."""
        import concurrent.futures

        @serve.deployment
        def concurrent_handler():
            import time

            time.sleep(0.1)  # Small delay to test concurrency
            return "concurrent"

        serve.run(concurrent_handler.bind())

        def make_request():
            return requests.get(
                "https://localhost:8000/concurrent_handler", verify=False
            )

        # Send 10 concurrent requests
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(make_request) for _ in range(10)]
            responses = [f.result() for f in futures]

        # All requests should succeed
        for response in responses:
            assert response.status_code == 200
            assert response.text == "concurrent"

    def test_https_large_payload(self, https_serve_instance):
        """Test HTTPS with large payloads."""

        @serve.deployment
        class LargePayloadHandler:
            def __call__(self, request):
                # Return a large response (1MB)
                large_data = "x" * (1024 * 1024)  # 1MB string
                return {"data": large_data, "size": len(large_data)}

        serve.run(LargePayloadHandler.bind())

        response = requests.get(
            "https://localhost:8000/LargePayloadHandler", verify=False
        )
        assert response.status_code == 200
        data = response.json()
        assert data["size"] == 1024 * 1024
        assert len(data["data"]) == 1024 * 1024

    def test_https_websocket_with_fastapi(self, https_serve_instance):
        """Test WebSocket functionality with FastAPI over HTTPS."""
        from fastapi import FastAPI, WebSocket, WebSocketDisconnect

        app = FastAPI()

        @app.websocket("/ws")
        async def websocket_endpoint(websocket: WebSocket):
            await websocket.accept()
            try:
                while True:
                    # Receive message from client
                    data = await websocket.receive_text()
                    message = json.loads(data)

                    # Echo back with modification
                    response = {
                        "echo": message.get("message", ""),
                        "secure": True,
                        "protocol": "wss",
                    }
                    await websocket.send_text(json.dumps(response))
            except WebSocketDisconnect:
                pass

        @serve.deployment
        @serve.ingress(app)
        class WebSocketDeployment:
            pass

        serve.run(WebSocketDeployment.bind())

        # Test WebSocket connection over HTTPS (wss://)
        async def test_websocket():
            # Create SSL context that doesn't verify certificates (for self-signed certs)
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE

            uri = "wss://localhost:8000/ws"
            async with websockets.connect(uri, ssl=ssl_context) as websocket:
                # Send test message
                test_message = {"message": "Hello WebSocket over HTTPS!"}
                await websocket.send(json.dumps(test_message))

                # Receive response
                response = await websocket.recv()
                data = json.loads(response)

                # Verify response
                assert data["echo"] == "Hello WebSocket over HTTPS!"
                assert data["secure"] is True
                assert data["protocol"] == "wss"

                # Send another message to test bidirectional communication
                test_message2 = {"message": "Second message"}
                await websocket.send(json.dumps(test_message2))

                response2 = await websocket.recv()
                data2 = json.loads(response2)
                assert data2["echo"] == "Second message"

        # Run the async test
        asyncio.run(test_websocket())

    def test_https_websocket_multiple_connections(self, https_serve_instance):
        """Test multiple WebSocket connections over HTTPS."""
        from fastapi import FastAPI, WebSocket, WebSocketDisconnect

        app = FastAPI()

        # Store active connections
        connections = []

        @app.websocket("/ws/broadcast")
        async def websocket_broadcast(websocket: WebSocket):
            await websocket.accept()
            connections.append(websocket)
            try:
                while True:
                    data = await websocket.receive_text()
                    message = json.loads(data)

                    # Broadcast to all connections
                    broadcast_message = {
                        "type": "broadcast",
                        "message": message.get("message", ""),
                        "connections": len(connections),
                        "secure": True,
                    }

                    # Send to all connected clients
                    disconnected = []
                    for conn in connections:
                        try:
                            await conn.send_text(json.dumps(broadcast_message))
                        except Exception:
                            disconnected.append(conn)

                    # Remove disconnected clients
                    for conn in disconnected:
                        connections.remove(conn)

            except WebSocketDisconnect:
                if websocket in connections:
                    connections.remove(websocket)

        @serve.deployment
        @serve.ingress(app)
        class WebSocketBroadcastDeployment:
            pass

        serve.run(WebSocketBroadcastDeployment.bind())

        async def test_multiple_websockets():
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE

            uri = "wss://localhost:8000/ws/broadcast"

            # Connect multiple clients
            websocket1 = await websockets.connect(uri, ssl=ssl_context)
            websocket2 = await websockets.connect(uri, ssl=ssl_context)

            try:
                # Send message from client 1
                test_message = {"message": "Hello from client 1"}
                await websocket1.send(json.dumps(test_message))

                # Both clients should receive the broadcast
                response1 = await websocket1.recv()
                response2 = await websocket2.recv()

                data1 = json.loads(response1)
                data2 = json.loads(response2)

                # Verify both received the same broadcast
                assert data1["type"] == "broadcast"
                assert data1["message"] == "Hello from client 1"
                assert data1["connections"] == 2
                assert data1["secure"] is True

                assert data2["type"] == "broadcast"
                assert data2["message"] == "Hello from client 1"
                assert data2["connections"] == 2
                assert data2["secure"] is True

            finally:
                await websocket1.close()
                await websocket2.close()

        # Run the async test
        asyncio.run(test_multiple_websockets())


class TestSSLConfiguration:
    def test_ssl_config_validation_success(self, ssl_cert_and_key):
        """Test successful SSL configuration validation."""
        key_path = ssl_cert_and_key["key_path"]
        cert_path = ssl_cert_and_key["cert_path"]

        # Should not raise exception
        options = HTTPOptions(ssl_keyfile=key_path, ssl_certfile=cert_path)
        assert options.ssl_keyfile == key_path
        assert options.ssl_certfile == cert_path

    def test_ssl_config_validation_missing_key(self):
        """Test SSL configuration validation with missing key file."""
        with tempfile.TemporaryDirectory() as temp_dir:
            cert_path = os.path.join(temp_dir, "test.crt")
            with open(cert_path, "w") as f:
                f.write("dummy cert")

            with pytest.raises(ValueError) as exc_info:
                HTTPOptions(ssl_keyfile=None, ssl_certfile=cert_path)

            assert "Both ssl_keyfile and ssl_certfile must be provided together" in str(
                exc_info.value
            )

    def test_ssl_config_validation_missing_cert(self):
        """Test SSL configuration validation with missing cert file."""
        with tempfile.TemporaryDirectory() as temp_dir:
            key_path = os.path.join(temp_dir, "test.key")
            with open(key_path, "w") as f:
                f.write("dummy key")

            with pytest.raises(ValueError) as exc_info:
                HTTPOptions(ssl_keyfile=key_path, ssl_certfile=None)

            assert "Both ssl_keyfile and ssl_certfile must be provided together" in str(
                exc_info.value
            )

    def test_ssl_config_with_password(self, ssl_cert_and_key):
        """Test SSL configuration with key file password."""
        key_path = ssl_cert_and_key["key_path"]
        cert_path = ssl_cert_and_key["cert_path"]

        options = HTTPOptions(
            ssl_keyfile=key_path, ssl_certfile=cert_path, ssl_keyfile_password="secret"
        )
        assert options.ssl_keyfile_password == "secret"

    def test_ssl_config_with_ca_certs(self, ssl_cert_and_key):
        """Test SSL configuration with CA certificates."""
        key_path = ssl_cert_and_key["key_path"]
        cert_path = ssl_cert_and_key["cert_path"]
        # Use cert as CA for testing purposes
        ca_path = cert_path

        options = HTTPOptions(
            ssl_keyfile=key_path, ssl_certfile=cert_path, ssl_ca_certs=ca_path
        )
        assert options.ssl_ca_certs == ca_path


class TestHTTPSErrorHandling:
    def test_ssl_file_paths_validation(self):
        """Test that SSL file paths are properly configured in HTTPOptions."""
        with tempfile.TemporaryDirectory() as temp_dir:
            key_path = os.path.join(temp_dir, "test.key")
            cert_path = os.path.join(temp_dir, "test.crt")

            # Create dummy files (content doesn't matter for this test)
            with open(key_path, "w") as f:
                f.write("dummy key")
            with open(cert_path, "w") as f:
                f.write("dummy cert")

            # Test that HTTPOptions accepts valid file paths
            options = HTTPOptions(ssl_keyfile=key_path, ssl_certfile=cert_path)
            assert options.ssl_keyfile == key_path
            assert options.ssl_certfile == cert_path

    def test_https_requires_both_cert_and_key_files(self):
        """Test that HTTPS configuration requires both certificate and key files."""
        # This test validates our SSL validation logic works correctly

        # Should work with both files
        options = HTTPOptions(ssl_keyfile="key.pem", ssl_certfile="cert.pem")
        assert options.ssl_keyfile == "key.pem"
        assert options.ssl_certfile == "cert.pem"

        # Should work with neither file
        options = HTTPOptions()
        assert options.ssl_keyfile is None
        assert options.ssl_certfile is None


class TestHTTPSIntegration:
    def test_https_with_custom_port(self, ssl_cert_and_key):
        """Test HTTPS on custom port."""
        # Ensure Ray is shutdown before starting
        try:
            ray.shutdown()
        except Exception:
            pass

        # Disable dashboard to prevent SSL conflicts and disable runtime env upload
        ray.init(include_dashboard=False, runtime_env={"working_dir": None})

        try:
            serve.start(
                http_options=HTTPOptions(
                    host="127.0.0.1",
                    port=8443,
                    ssl_keyfile=ssl_cert_and_key["key_path"],
                    ssl_certfile=ssl_cert_and_key["cert_path"],
                )
            )

            @serve.deployment
            def custom_port_handler():
                return "custom port"

            serve.run(custom_port_handler.bind())

            response = requests.get(
                "https://127.0.0.1:8443/custom_port_handler", verify=False
            )
            assert response.status_code == 200
            assert response.text == "custom port"
        finally:
            try:
                serve.shutdown()
            except Exception:
                pass
            ray.shutdown()

    def test_https_deployment_update(self, https_serve_instance):
        """Test deployment updates work correctly with HTTPS."""

        @serve.deployment
        def updatable():
            return "version 1"

        serve.run(updatable.bind())

        # Test initial version
        response = requests.get("https://localhost:8000/updatable", verify=False)
        assert response.text == "version 1"

        # Update deployment
        @serve.deployment
        def updatable():
            return "version 2"

        serve.run(updatable.bind())

        # Test updated version
        response = requests.get("https://localhost:8000/updatable", verify=False)
        assert response.text == "version 2"


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
