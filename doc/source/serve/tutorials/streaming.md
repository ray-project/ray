(serve-streaming-tutorial)=

# Streaming Tutorial

This guide walks you through deploying a chatbot that streams output back to the
user. We show

* How to stream outputs from your Serve deployments
* How to batch requests when streaming output
* How to add WebSockets to your Serve deployments

This tutorial should help you with following use cases:

* You want to serve a large language model that should stream results back token-by-token.
* You want to serve a chatbot that must accept a stream of requests from the user.

# Create the Deployment

This tutorial serves the [DialoGPT](https://huggingface.co/microsoft/DialoGPT-small) model. Install the HuggingFace library to access it:

```
pip install transformers
```

Open a new Python file called `tutorial_stream.py`. First, add the imports and the Serve logger.

```{literalinclude} ../doc_code/streaming_tutorial.py
:language: python
:start-after: __setup_start__
:end-before: __setup_end__
```

Create a FastAPI deployment, and initialize the model and the tokenizer in the
constructor:

```{literalinclude} ../doc_code/streaming_tutorial.py
:language: python
:start-after: __textbot_constructor_start__
:end-before: __textbot_constructor_end__
```
