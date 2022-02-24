# extracted from aioboto3
#    https://github.com/terrycain/aioboto3/blob/16a1a1085191ebe6d40ee45d9588b2173738af0c/tests/mock_server.py
import pytest
import requests
import shutil
import signal
import subprocess as sp
import time

_proxy_bypass = {
    "http": None,
    "https": None,
}


def start_service(service_name, host, port):
    moto_svr_path = shutil.which("moto_server")
    if not moto_svr_path:
        pytest.skip("moto not installed")
    args = [moto_svr_path, service_name, "-H", host, "-p", str(port)]
    # For debugging
    # args = '{0} {1} -H {2} -p {3} 2>&1 | \
    # tee -a /tmp/moto.log'.format(moto_svr_path, service_name, host, port)
    process = sp.Popen(
        args, stdin=sp.PIPE, stdout=sp.PIPE, stderr=sp.PIPE
    )  # shell=True
    url = "http://{host}:{port}".format(host=host, port=port)

    for i in range(0, 30):
        output = process.poll()
        if output is not None:
            print("moto_server exited status {0}".format(output))
            stdout, stderr = process.communicate()
            print("moto_server stdout: {0}".format(stdout))
            print("moto_server stderr: {0}".format(stderr))
            pytest.fail("Can not start service: {}".format(service_name))

        try:
            # we need to bypass the proxies due to monkeypatches
            requests.get(url, timeout=5, proxies=_proxy_bypass)
            break
        except requests.exceptions.ConnectionError:
            time.sleep(0.5)
    else:
        stop_process(process)  # pytest.fail doesn't call stop_process
        pytest.fail("Can not start service: {}".format(service_name))

    return process


def stop_process(process):
    try:
        process.send_signal(signal.SIGTERM)
        process.communicate(timeout=20)
    except sp.TimeoutExpired:
        process.kill()
        outs, errors = process.communicate(timeout=20)
        exit_code = process.returncode
        msg = "Child process finished {} not in clean way: {} {}".format(
            exit_code, outs, errors
        )
        raise RuntimeError(msg)


# TODO(Clark): We should be able to use "session" scope here, but we've found
# that the s3_fs fixture ends up hanging with S3 ops timing out (or the server
# being unreachable). This appears to only be an issue when using the tmp_dir
# fixture as the S3 dir path. We should fix this since "session" scope should
# reduce a lot of the per-test overhead (2x faster execution for IO methods in
# test_dataset.py).
@pytest.fixture(scope="function")
def s3_server():
    host = "localhost"
    port = 5002
    url = "http://{host}:{port}".format(host=host, port=port)
    process = start_service("s3", host, port)
    yield url
    stop_process(process)
