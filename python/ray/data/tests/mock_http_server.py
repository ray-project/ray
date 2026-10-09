# extracted from fsspec
#    https://github.com/fsspec/filesystem_spec/blob/a8cfd9c52a20c930c67ff296b60dbcda89d64db9/fsspec/tests/conftest.py
import contextlib
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

port = 9898
data = b"\n".join([b"some test data"] * 1000)
data_file = "http://localhost:%i/index/data_file" % port
index = b'<a href="%s">Link</a>' % data_file.encode()


class HTTPTestHandler(BaseHTTPRequestHandler):
    files = {
        "/index/data_file": data,
        "/index": index,
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def _respond(self, code=200, headers=None, data=b""):
        headers = headers or {}
        headers.update({"User-Agent": "test"})
        self.send_response(code)
        for k, v in headers.items():
            self.send_header(k, str(v))
        self.end_headers()
        if data:
            self.wfile.write(data)

    def do_GET(self):
        file_path = self.path.rstrip("/")
        file_data = self.files.get(file_path)
        if file_data is None:
            return self._respond(404)
        if "Range" in self.headers:
            ran = self.headers["Range"]
            b, ran = ran.split("=")
            start, end = ran.split("-")
            if start:
                file_data = file_data[int(start) : (int(end) + 1) if end else None]
            else:
                # suffix only
                file_data = file_data[-int(end) :]
        if "give_length" in self.headers:
            response_headers = {"Content-Length": len(file_data)}
            self._respond(200, response_headers, file_data)
        elif "give_range" in self.headers:
            self._respond(
                200,
                {"Content-Range": "0-%i/%i" % (len(file_data) - 1, len(file_data))},
                file_data,
            )
        else:
            self._respond(200, data=file_data)


@contextlib.contextmanager
def serve():
    server_address = ("", port)
    httpd = HTTPServer(server_address, HTTPTestHandler)
    th = threading.Thread(target=httpd.serve_forever)
    th.daemon = True
    th.start()
    try:
        yield "http://localhost:%i" % port
    finally:
        httpd.socket.close()
        httpd.shutdown()
        th.join()


@pytest.fixture(scope="module")
def http_server():
    with serve() as s:
        yield s


@pytest.fixture(scope="module")
def http_file():
    return data_file
