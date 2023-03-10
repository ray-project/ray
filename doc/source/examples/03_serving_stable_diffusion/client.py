import aiohttp
import asyncio
import os
import halo
import requests
import time
import uuid

import ray

ray.init()
NUM_NODES = len(ray.nodes())
ENDPOINT = "http://localhost:8000/imagine"
spinner = halo.Halo(text="Generating image...", spinner="dots")


async def generate_image(session, prompt):
    req = {"prompt": prompt, "img_size": 776}
    async with session.get(ENDPOINT, params=req) as resp:
        image_data = await resp.read()
    return image_data


async def main():
    try:
        requests.get(ENDPOINT, timeout=0.1)
    except Exception as e:
        raise RuntimeWarning(
            "Did you setup the Ray Serve model replicas with "
            "`serve run app:entrypoint` in another terminal yet?"
        ) from e

    while True:
        example_prompt = "dog reading a newspaper in 4k hd ultra realistic"
        prompt = input(f"Enter a prompt (ex: '{example_prompt}'): ")

        start = time.time()
        spinner.start()

        async with aiohttp.ClientSession() as session:
            tasks = []
            for i in range(NUM_NODES):
                tasks.append(generate_image(session, prompt))
            images = await asyncio.gather(*tasks)

        dirname = f"{uuid.uuid4().hex[:8]}"
        os.makedirs(dirname)
        for i, image in enumerate(images):
            with open(os.path.join(dirname, f"{i}.png"), "wb") as f:
                f.write(image)

        spinner.stop()
        elapsed = time.time() - start

        print(
            f"\nGenerated {len(images)} image(s) in {elapsed:.2f} seconds to: "
            f"{dirname}\n"
        )


asyncio.run(main())
