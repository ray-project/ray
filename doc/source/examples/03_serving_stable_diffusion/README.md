# Serving a Stable Diffusion Model

## Install Cluster-wide Dependencies

We'll use `pip install --user` to install the necessary requirements.
On an [Anyscale Workspace](https://docs.anyscale.com/user-guide/develop-and-debug/workspaces),
this will install packages to a shared filesystem that will be available to all nodes in the cluster.

```
pip install --user -r requirements.txt
```

## Deploy the Ray Serve application locally

The Ray Serve application with the model serving logic can be found in `app.py`, where we define:
- The `/imagine` API endpoint that we will query to generate the image.
- The stable diffusion model loaded inside a Ray Serve Deployment.
  This deployment will be replicated as needed when Ray automatically scales up our model serving to more nodes.

Let's deploy the Ray Serve application locally (at `http://localhost:8000`)!
Open a terminal in Jupyter/VSCode and run the following command:

```
serve run app:entrypoint
```

This command will continue running to host your local Ray Serve application.
This will be the place to view all the autoscaling logs, as well as any logs emitted by
the model inference once requests start coming through.

## Make a Request

We also include a very basic `client.py` script that will submit prompts as HTTP requests to our local endpoint at `http://localhost:8000/imagine`.
Start the client script in another terminal and enter a prompt to generate your first image!

```
python client.py
```

```
Enter a prompt: twin peaks sf in basquiat painting style

Generated image in 67.54 seconds to: 90374611.png
```

![Example output](https://user-images.githubusercontent.com/3887863/221063452-3c5e5f6b-fc8c-410f-ad5c-202441cceb51.png)


Once the stable diffusion model finishes generating your image, it will be included in the HTTP response body.
The script writes this to an image in your Workspace directory for you to view.

You've successfully served a stable diffusion model!
You can modify this template and quickly iterate your model deployment directly on your cluster via the Anyscale Workspace,
testing with the local endpoint.
Once you're ready for your application to be used in production,
you can go through the following steps to deploy the model as an **Anyscale Service**!

## Deploy the application as an Anyscale Service

TODO

