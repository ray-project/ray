# Serving a Stable Diffusion Model with Ray Serve

This template loads a pretrained stable diffusion model from HuggingFace and serves it to a local endpoint as a [Ray Serve](https://docs.ray.io/en/latest/serve/index.html) deployment.

By the end, we'll have an application that generates images using stable diffusion for a given prompt!

The application will look something like this:

```
Enter a prompt (or 'q' to quit):   twin peaks sf in basquiat painting style

Generating image(s)...

Generated 4 image(s) in 11.75 seconds to the directory: 58b298d9
```

![Example output](https://github-production-user-asset-6210df.s3.amazonaws.com/3887863/239090189-dc1f1b7b-2fa0-4886-ae12-ca5d35b8ebc9.png)

When the workspace is up and running, start coding by clicking on the Jupyter or VSCode icon above. Open the `serving_stable_diffusion.ipynb` file and follow the instructions there.
