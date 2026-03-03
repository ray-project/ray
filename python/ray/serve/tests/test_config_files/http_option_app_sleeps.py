import time

from ray import serve


@serve.deployment
def sleep(request):
    sleep_s = float(request.query_params.get("sleep_s", 0))
    print(f"sleep_s: {sleep_s}")
    time.sleep(sleep_s)
    return "Task Succeeded!"


sleep_node = sleep.bind()
