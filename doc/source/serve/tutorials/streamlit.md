(streamlit-serve-tutorial)=

# Building a Streamlit app with Ray Serve

In this example, we will show you how to wrap a machine learning model served
by Ray Serve in a [Streamlit application](https://streamlit.io/).

Specifically, we're going to download a GPT-2 model from the `transformer` library,
define a Ray Serve deployment with it, and then define and launch a Streamlit app.
Let's take a look.


## Deploying a model with Ray Serve

To start off, we import Ray Serve and Streamlit, as well as the
`transformers` and `requests` libraries:

```{eval-rst}
.. literalinclude:: ../doc_code/streamlit_app.py
    :language: python
    :start-after: __streamlit_imports_begin__
    :end-before: __streamlit_imports_end__
```

Next, we define a Ray Serve deployment with a GPT-2 model, by using the
`@serve.deployment` decorator on a `model` function that takes a `request` argument.
In this function we define a GPT-2 model with a call to `pipeline` and return
the result of querying the model.
Before defining the deployment, we start Ray Serve using `serve.start()`, and
then proceed to deploy the model with `model.deploy()`.

```{eval-rst}
.. literalinclude:: ../doc_code/streamlit_app.py
    :language: python
    :start-after: __streamlit_model_begin__
    :end-before: __streamlit_model_end__
```

Note that we're using Streamlit's `session_state` to make sure the deployment
only gets run once.
If we didn't use such a mechanism, Streamlit would simply run the whole script again,
which is not what we want.

To test this deployment we use a simple `example` query to get a `response` from
the model running on `localhost:8000/model`.
The first time you use this endpoint, the model will be downloaded first,
which can take a while to complete.
Subsequent calls will be faster.

```{eval-rst}
.. literalinclude:: ../doc_code/streamlit_app.py
    :language: python
    :start-after: __streamlit_test_begin__
    :end-before: __streamlit_test_end__
```

## Defining and launching a Streamlit app

To define a streamlit app, let's first create a convenient wrapper that takes
a `query` argument and returns the result of querying the GPT model.

```{eval-rst}
.. literalinclude:: ../doc_code/streamlit_app.py
    :language: python
    :start-after: __streamlit_gpt_begin__
    :end-before: __streamlit_gpt_end__
```

Apart from this `gpt2` function, the only other thing that we need is a way
for users to specify the model input, and a way to display the result.
Since our model takes text as input and output, this turns out to be pretty simple:

```{eval-rst}
.. literalinclude:: ../doc_code/streamlit_app.py
    :language: python
    :start-after: __streamlit_app_begin__
    :end-before: __streamlit_app_end__
```

To serve this model with Streamlit, we use just a few simple text components,
namely `st.title`, `st.header`, and
`st.text` for output and `st.text_input` for getting the model input.
We also use a button to trigger model inference for a new input prompt.
There's much more you can do with Streamlit, but this is just a simple example.

```{margin}
The [Streamlit API documentation](https://docs.streamlit.io/library/api-reference)
covers all viable Streamlit components in detail.
```

Finally, if you put everything we just did together in a single file
called `streamlit_app.py`, you can run your Streamlit app with Ray Serve as follows:

```python pycharm={"name": "#%% bash\n"}
streamlit run streamlit_app.py
```

<!-- #region pycharm={"name": "#%% md\n"} -->
This should launch an interface that you can interact with that looks like this:

```{image} https://raw.githubusercontent.com/ray-project/images/master/docs/serve/streamlit_serve_gpt.png
```

To summarize, if you know the basics of Streamlit,
it's straightforward to deploy a model with Ray Serve with it.
