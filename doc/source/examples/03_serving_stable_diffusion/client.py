import aiohttp
import asyncio
import os
import halo
import ray
import time
import uuid


ray.init()
NUM_NODES = len(ray.nodes())

spinner = halo.Halo(text="Generating image...", spinner="dots")


async def generate_image(session, prompt):
    req = {"prompt": prompt, "img_size": 776}
    async with session.get("http://localhost:8000/imagine", params=req) as resp:
        image_data = await resp.read()
    return image_data


async def main():
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
            f"\nGenerated {len(images)} image(s) in {elapsed:.2f} seconds to: {dirname}\n"
        )


asyncio.run(main())
