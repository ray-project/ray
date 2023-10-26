import requests

endpoint = "http://localhost:8000/imagine"


def generate_image(prompt, image_size):
    req = {"prompt": prompt, "img_size": image_size}
    resp = requests.get(endpoint, params=req)
    return resp.content


image = generate_image("twin peaks sf in basquiat painting style", 640)
filename = "image.png"
with open(filename, "wb") as f:
    f.write(image)
