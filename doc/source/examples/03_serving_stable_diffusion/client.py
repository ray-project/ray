import halo
import requests
import time
import uuid

spinner = halo.Halo(text="Generating image...", spinner="dots")

while True:
    example_prompt = "dog reading a newspaper in 4k hd ultra realistic"
    prompt = input(f"Enter a prompt (ex: '{example_prompt}'): ")

    start = time.time()
    spinner.start()

    # Make the request to our Serve application
    req = {"prompt": prompt, "img_size": 776}
    resp = requests.get("http://localhost:8000/imagine", params=req)

    # Write the repsonse as an image!
    filename = f"{uuid.uuid4().hex[:8]}.png"
    with open(filename, "wb") as out_file:
        out_file.write(resp.content)

    spinner.stop()
    elapsed = time.time() - start

    print(f"\nGenerated image in {elapsed:.2f} seconds to: {filename}\n")
