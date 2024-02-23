(aws-neuron-core-inference-tutorial)=

# Serve an Inference Model on AWS NeuronCores Using FastAPI (Experimental)
This example compiles a BERT-based model and deploys the traced model on an AWS Inferentia (Inf2) or Tranium (Trn1)
instance using Ray Serve and FastAPI.


:::{note}
  Before starting this example: 
  * Set up [PyTorch Neuron](https://awsdocs-neuron.readthedocs-hosted.com/en/latest/general/setup/torch-neuronx.html#setup-torch-neuronx)
  * Install AWS NeuronCore drivers and tools, and torch-neuronx based on the instance-type

:::

```bash
python -m pip install "ray[serve]" requests transformers
```

This example uses the [j-hartmann/emotion-english-distilroberta-base](https://huggingface.co/j-hartmann/emotion-english-distilroberta-base) model and [FastAPI](https://fastapi.tiangolo.com/).

Use the following code to compile the model:
```{literalinclude} ../doc_code/aws_neuron_core_inference_serve.py
:language: python
:start-after: __compile_neuron_code_start__
:end-before: __compile_neuron_code_end__
```


For compiling the model, you should see the following log messages:
```text
Downloading (…)lve/main/config.json: 100%|█████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 1.00k/1.00k [00:00<00:00, 242kB/s]
Downloading pytorch_model.bin: 100%|█████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 329M/329M [00:01<00:00, 217MB/s]
Downloading (…)okenizer_config.json: 100%|█████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 294/294 [00:00<00:00, 305kB/s]
Downloading (…)olve/main/vocab.json: 100%|██████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 798k/798k [00:00<00:00, 22.0MB/s]
Downloading (…)olve/main/merges.txt: 100%|██████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 456k/456k [00:00<00:00, 57.0MB/s]
Downloading (…)/main/tokenizer.json: 100%|████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 1.36M/1.36M [00:00<00:00, 6.16MB/s]
Downloading (…)cial_tokens_map.json: 100%|█████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 239/239 [00:00<00:00, 448kB/s]
huggingface/tokenizers: The current process just got forked, after parallelism has already been used. Disabling parallelism to avoid deadlocks...
To disable this warning, you can either:
        - Avoid using `tokenizers` before the fork if possible
        - Explicitly set the environment variable TOKENIZERS_PARALLELISM=(true | false)
Saved Neuron-compiled model ./sentiment_neuron.pt
```

The traced model should be ready for deployment. Save the following code to a file named aws_neuron_core_inference_serve.py.

Use `serve run aws_neuron_core_inference_serve:entrypoint` to start the Serve application.
```{literalinclude} ../doc_code/aws_neuron_core_inference_serve.py
:language: python
:start-after: __neuron_serve_code_start__
:end-before: __neuron_serve_code_end__
```


You should see the following log messages when a deployment is successful:
```text
(ServeController pid=43105) INFO 2023-08-23 20:29:32,694 controller 43105 deployment_state.py:1372 - Deploying new version of deployment default_BertBaseModel.
(ServeController pid=43105) INFO 2023-08-23 20:29:32,695 controller 43105 deployment_state.py:1372 - Deploying new version of deployment default_APIIngress.
(ProxyActor pid=43147) INFO 2023-08-23 20:29:32,620 http_proxy 10.0.1.234 http_proxy.py:1328 - Proxy actor 8be14f6b6b10c0190cd0c39101000000 starting on node 46a7f740898fef723c3360ef598c1309701b07d11fb9dc45e236620a.
(ProxyActor pid=43147) INFO:     Started server process [43147]
(ServeController pid=43105) INFO 2023-08-23 20:29:32,799 controller 43105 deployment_state.py:1654 - Adding 1 replica to deployment default_BertBaseModel.
(ServeController pid=43105) INFO 2023-08-23 20:29:32,801 controller 43105 deployment_state.py:1654 - Adding 1 replica to deployment default_APIIngress.
2023-08-23 20:29:44,690 SUCC scripts.py:462 -- Deployed Serve app successfully.
```

Use the following code to send requests:
```python
import requests

response = requests.get(f"http://127.0.0.1:8000/infer?sentence=Ray is super cool")
print(response.status_code, response.json())
```
The response includes a status code and the classifier output:

```text
200 joy
```
