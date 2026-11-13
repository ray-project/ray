---
orphan: true
myst:
  html_meta:
    description: "Host a Streamlit app as a Ray Serve deployment with Streamlit's ASGI-compatible App API."
---

# Serve a Streamlit app with Ray Serve

This tutorial shows how to host a [Streamlit](https://streamlit.io/) app with Ray Serve. Streamlit provides an ASGI-compatible `st.App` entry point, and Ray Serve's {func}`@serve.ingress <ray.serve.ingress>` decorator accepts ASGI applications.

This example requires Streamlit 1.53 or later.

## Build the Streamlit app

Create a file named `streamlit_app.py`:

```python
import streamlit as st

st.set_page_config(page_title="Ray Serve and Streamlit")
st.title("Ray Serve and Streamlit")

name = st.text_input("What's your name?", value="Ray")
st.write(f"Hello, {name}!")
```

## Wrap the app in a Serve deployment

Create a second file named `serve_app.py` in the same directory. Build the Streamlit app inside the replica so Ray Serve doesn't serialize the ASGI application from the driver process.

```python
from pathlib import Path

from ray import serve


def build_streamlit_app():
    import streamlit as st

    app_path = Path(__file__).with_name("streamlit_app.py")
    return st.App(app_path)


@serve.deployment(num_replicas=1)
@serve.ingress(build_streamlit_app)
class StreamlitIngress:
    pass


app = StreamlitIngress.bind()
```

The builder function runs when Serve initializes the replica. The `Path` expression resolves the Streamlit script next to `serve_app.py` in the deployment's working directory.

## Deploy the app

Install Ray Serve and Streamlit:

```console
pip install "ray[serve]" "streamlit>=1.53"
```

Run the application from the directory containing both Python files:

```console
serve run serve_app:app
```

When you connect to a remote cluster, upload both files with the `working_dir` option:

```console
serve run --address=ray://<head-node-ip-address>:10001 --working-dir=. serve_app:app
```

Open [http://localhost:8000](http://localhost:8000) in a browser to view the Streamlit app.

:::{note}
Keep one replica per Streamlit app unless your deployment provides session affinity. Streamlit stores session state on the server, so requests from one browser session must reach the same replica.
:::

For more information about HTTP applications in Ray Serve, see the {ref}`Set Up FastAPI and HTTP <serve-set-up-fastapi-http>` guide.
