(serve-object-detection-tutorial)=

# Serving an Object Detection Model
This example runs an object detection application with Ray Serve.

To run this example, install the following:

```bash
pip install "ray[serve]" requests torch
```

This example uses the [ultralytics/yolov5](https://github.com/ultralytics/yolov5) model and [FastAPI](https://fastapi.tiangolo.com/). Save the following code to a file named object_detection.py.

Use the following Serve code:
```{literalinclude} ../doc_code/object_detection.py
:language: python
:start-after: __example_code_start__
:end-before: __example_code_end__
```

Use `serve run object_detection:entrypoint` to start the serve application.

:::{note}
The autoscaling config sets `min_replicas` to 0, which means the deployment starts with no `ObjectDetection` replicas. These replicas spawn only when a request arrives. After a period where no requests arrive, Serve downscales `ObjectDetection` back to 0 replicas to save GPU resources.

:::

You should see the following logs:
```text
(ServeReplica:ObjectDection pid=4747)   warnings.warn(
(ServeReplica:ObjectDection pid=4747) Downloading: "https://github.com/ultralytics/yolov5/zipball/master" to /home/ray/.cache/torch/hub/master.zip
(ServeReplica:ObjectDection pid=4747) YOLOv5 🚀 2023-3-8 Python-3.9.16 torch-1.13.0+cu116 CUDA:0 (Tesla T4, 15110MiB)
(ServeReplica:ObjectDection pid=4747) 
(ServeReplica:ObjectDection pid=4747) Fusing layers... 
(ServeReplica:ObjectDection pid=4747) YOLOv5s summary: 213 layers, 7225885 parameters, 0 gradients
(ServeReplica:ObjectDection pid=4747) Adding AutoShape... 
2023-03-08 21:10:21,685 SUCC <string>:93 -- Deployed Serve app successfully.
```

Use the following code to send requests:
```python
import requests

image_url = "https://ultralytics.com/images/zidane.jpg"
resp = requests.get(f"http://127.0.0.1:8000/detect?image_url={image_url}")

with open("output.jpeg", 'wb') as f:
    f.write(resp.content)
```
The output.png file is saved locally. Check it out!
![image](https://raw.githubusercontent.com/ray-project/images/master/docs/serve/object_detection_output.jpeg)
