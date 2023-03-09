(serve-object-detection-tutorial)=

# Serving Object Detection Model
In this guide, we will show you how to build an object detection application with Ray Serve.

For this example, we use [ultralytics/yolov5](https://github.com/ultralytics/yolov5) model and FastAPI to build the example. 

Serve Code:
```{literalinclude} ../doc_code/object_detection.py
```

Save above code to a file e.g. object_detection.py. You can use `serve run object_detection:entrypoint` to start the serve application.

:::{note}
The autoscaling config is using min_replica = 0, replica will be spawn when the request arrives. When there is no request, Serve Application will downsacle replicas to 0 for saving GPU resource.
:::

You should see output in the end.
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

You can use following code to send request.
```python
import requests

image_url = "https://ultralytics.com/images/zidane.jpg"
resp = requests.get(f"http://127.0.0.1:8000/detect?image_url={image_url}")

with open("output.jpeg", 'wb') as f:
    f.write(resp.content)
```
At the end, `output.png` file should be saved locally. Checkout!
![image](https://raw.githubusercontent.com/ray-project/images/master/docs/serve/object_detection_output.jpeg)

