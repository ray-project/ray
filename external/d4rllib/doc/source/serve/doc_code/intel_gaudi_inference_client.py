# __main_code_start__
import requests

# Prompt for the model
prompt = "Once upon a time,"

# Add generation config here
config = {}

# Non-streaming response
sample_input = {"text": prompt, "config": config, "stream": False}
outputs = requests.post("http://127.0.0.1:8000/", json=sample_input, stream=False)
print(outputs.text, flush=True)

# Streaming response
sample_input["stream"] = True
outputs = requests.post("http://127.0.0.1:8000/", json=sample_input, stream=True)
outputs.raise_for_status()
for output in outputs.iter_content(chunk_size=None, decode_unicode=True):
    print(output, end="", flush=True)
print()

# __main_code_end__
