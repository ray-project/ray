(serve-pytorch-tutorial)=

# PyTorch Tutorial

In this guide, we will load and serve a PyTorch Resnet Model.
In particular, we show:

- How to load the model from PyTorch's pre-trained modelzoo.
- How to parse the JSON request, transform the payload and evaluated in the model.

Please see the [Key Concepts](key-concepts) to learn more general information about Ray Serve.

This tutorial requires Pytorch and Torchvision installed in your system. Ray Serve
is framework agnostic and works with any version of PyTorch.

```bash
pip install torch torchvision
```

Let's import Ray Serve and some other helpers.

```{literalinclude} ../../../../python/ray/serve/examples/doc/tutorial_pytorch.py
:end-before: __doc_import_end__
:start-after: __doc_import_begin__
```

Services are just defined as normal classes with `__init__` and `__call__` methods.
The `__call__` method will be invoked per request.

```{literalinclude} ../../../../python/ray/serve/examples/doc/tutorial_pytorch.py
:end-before: __doc_define_servable_end__
:start-after: __doc_define_servable_begin__
```

Now that we've defined our services, let's deploy the model to Ray Serve. We will
define a Serve deployment that will be exposed over an HTTP route.

```{literalinclude} ../../../../python/ray/serve/examples/doc/tutorial_pytorch.py
:end-before: __doc_deploy_end__
:start-after: __doc_deploy_begin__
```

Let's query it!

```{literalinclude} ../../../../python/ray/serve/examples/doc/tutorial_pytorch.py
:end-before: __doc_query_end__
:start-after: __doc_query_begin__
```
