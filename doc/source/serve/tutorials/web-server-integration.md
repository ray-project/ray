(serve-web-server-integration-tutorial)=

# Integration with Existing Web Servers

In this guide, you will learn how to use Ray Serve to scale up your existing web application.  The key feature of Ray Serve that makes this possible is the Python-native {ref}`servehandle-api`, which allows you keep using your same Python web server while offloading your heavy computation to Ray Serve.

We give two examples, one using a [FastAPI](https://fastapi.tiangolo.com/) web server and another using an [AIOHTTP](https://docs.aiohttp.org/en/stable/) web server, but the same approach will work with any Python web server.

## Scaling Up a FastAPI Application

Ray Serve has a native integration with FastAPI - please see {ref}`serve-fastapi-http`.

## Scaling Up an AIOHTTP Application

In this section, we'll integrate Ray Serve with an [AIOHTTP](https://docs.aiohttp.org/en/stable/) web server run using [Gunicorn](https://gunicorn.org/).  You'll need to install AIOHTTP and gunicorn with the command `pip install aiohttp gunicorn`.

First, here is the script that deploys Ray Serve:

```{literalinclude} ../../../../python/ray/serve/examples/doc/aiohttp/aiohttp_deploy_serve.py
```

Next is the script that defines the AIOHTTP server:

```{literalinclude} ../../../../python/ray/serve/examples/doc/aiohttp/aiohttp_app.py
```

Here's how to run this example:

1. Run `ray start --head` to start a local Ray cluster in the background.
2. In the directory where the example files are saved, run `python aiohttp_deploy_serve.py` to deploy our Ray Serve deployment.
3. Run `gunicorn aiohttp_app:app --worker-class aiohttp.GunicornWebWorker` to start the AIOHTTP app using gunicorn.
4. To test out the server, run `curl localhost:8000/dummy-model`.  This should output `Model received data: dummy input`.
5. For cleanup, you can press Ctrl-C to stop the Gunicorn server, and run `ray stop` to stop the background Ray cluster.
