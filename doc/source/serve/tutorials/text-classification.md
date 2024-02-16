(serve-text-classification-tutorial)=

# Serve a Text Classification Model
This example uses a DistilBERT model to build an IMDB review classification application with Ray Serve.

To run this example, install the following:

```bash
pip install "ray[serve]" requests torch transformers
```

This example uses the [distilbert-base-uncased](https://huggingface.co/docs/transformers/tasks/sequence_classification) model and [FastAPI](https://fastapi.tiangolo.com/). Save the following code to a file named distilbert_app.py:

Use the following Serve code:
```{literalinclude} ../doc_code/distilbert.py
:language: python
:start-after: __example_code_start__
:end-before: __example_code_end__
```

Use `serve run distilbert_app:entrypoint` to start the Serve application.

:::{note}
The autoscaling config sets `min_replicas` to 0, which means the deployment starts with no `ObjectDetection` replicas. These replicas spawn only when a request arrives. When no requests arrive after a certain period of time, Serve downscales `ObjectDetection` back to 0 replica to save GPU resources.
:::

You should see the following messages in the logs:
```text
(ServeController pid=362, ip=10.0.44.233) INFO 2023-03-08 16:44:57,579 controller 362 http_state.py:129 - Starting HTTP proxy with name 'SERVE_CONTROLLER_ACTOR:SERVE_PROXY_ACTOR-7396d5a9efdb59ee01b7befba448433f6c6fc734cfa5421d415da1b3' on node '7396d5a9efdb59ee01b7befba448433f6c6fc734cfa5421d415da1b3' listening on '127.0.0.1:8000'
(ServeController pid=362, ip=10.0.44.233) INFO 2023-03-08 16:44:57,588 controller 362 http_state.py:129 - Starting HTTP proxy with name 'SERVE_CONTROLLER_ACTOR:SERVE_PROXY_ACTOR-a30ea53938547e0bf88ce8672e578f0067be26a7e26d23465c46300b' on node 'a30ea53938547e0bf88ce8672e578f0067be26a7e26d23465c46300b' listening on '127.0.0.1:8000'
(ProxyActor pid=439, ip=10.0.44.233) INFO:     Started server process [439]
(ProxyActor pid=5779) INFO:     Started server process [5779]
(ServeController pid=362, ip=10.0.44.233) INFO 2023-03-08 16:44:59,362 controller 362 deployment_state.py:1333 - Adding 1 replica to deployment 'APIIngress'.
2023-03-08 16:45:01,316 SUCC <string>:93 -- Deployed Serve app successfully.
```

Use the following code to send requests:
```python
import requests

prompt = "This was a masterpiece. Not completely faithful to the books, but enthralling from beginning to end. Might be my favorite of the three."
input = "%20".join(prompt.split(" "))
resp = requests.get(f"http://127.0.0.1:8000/classify?sentence={prompt}")
print(resp.status_code, resp.json())
```
The output of the client code is the response status code, the label, which is positive in this example, and the label's score.
```text
200 [{'label': 'LABEL_1', 'score': 0.9994940757751465}]
```
