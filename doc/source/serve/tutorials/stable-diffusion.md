(serve-stable-diffusion-tutorial)=

# Serve a Stable Diffusion Model
This example runs a Stable Diffusion application with Ray Serve.

To run this example, install the following:

```bash
pip install "ray[serve]" requests torch diffusers==0.12.1 transformers
```

This example uses the [stabilityai/stable-diffusion-2](https://huggingface.co/stabilityai/stable-diffusion-2) model and [FastAPI](https://fastapi.tiangolo.com/) to build the example. Save the following code to a file named stable_diffusion.py. 

The Serve code is as follows:
```{literalinclude} ../doc_code/stable_diffusion.py
:language: python
:start-after: __example_code_start__
:end-before: __example_code_end__
```

Use `serve run stable_diffusion:entrypoint` to start the Serve application.

:::{note}
The autoscaling config sets `min_replicas` to 0, which means the deployment starts with no `ObjectDetection` replicas. These replicas spawn only when a request arrives. When no requests arrive after a certain period of time, Serve downscales `ObjectDetection` back to 0 replica to save GPU resources.
:::

You should see these messages in the output:
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

prompt = "a cute cat is dancing on the grass."
input = "%20".join(prompt.split(" "))
resp = requests.get(f"http://127.0.0.1:8000/imagine?prompt={input}")
with open("output.png", 'wb') as f:
    f.write(resp.content)
```
The app saves the `output.png` file locally. The following is an example of an output image.
![image](https://raw.githubusercontent.com/ray-project/images/master/docs/serve/stable_diffusion_output.png)
