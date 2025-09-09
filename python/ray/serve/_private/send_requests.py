# send_requests.py (Modified for debugging)

import io
import logging
import pprint
import time

import numpy as np
import requests
from PIL import Image

import ray

# --- Keep your existing setup code ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

url = "http://127.0.0.1:8000/test"

dummy_image = np.zeros((2048, 2048, 3), dtype=np.uint8)
dummy_depth = np.zeros((2048, 2048), dtype=float)


def serialize_data(data, format="jpeg"):
    bio = io.BytesIO()
    image = Image.fromarray(data)
    image.save(bio, format=format)
    return bio.getvalue()


binary_dummy_image = serialize_data(dummy_image, "jpeg")
binary_dummy_depth = serialize_data(dummy_depth, "tiff")
data = {"image_data": binary_dummy_image, "depth_data": binary_dummy_depth}

# --- NEW DEBUGGING CODE ---
# Connect to the running Ray and Serve instance
ray.init(address="auto", namespace="serve", ignore_reinit_error=True)

# The prefix for all proxy actor names
PROXY_ACTOR_NAME_PREFIX = "SERVE_PROXY_ACTOR-"

# List all named actors in the cluster
all_actors = ray.util.list_named_actors()
proxy_actor_name = None

# Find the first actor whose name matches the proxy prefix
for actor in all_actors:
    print(f"Checking actor: {actor}")
    if actor and actor.startswith(PROXY_ACTOR_NAME_PREFIX):
        proxy_actor_name = actor
        break  # Stop after finding the first one

if proxy_actor_name:
    print(f"Found proxy actor name: {proxy_actor_name}")
    # Now you can get the handle
    # proxy_handle = ray.get_actor(proxy_actor_name)
else:
    print("Could not find a running proxy actor.")

proxy = ray.get_actor(proxy_actor_name)

# --- RUN THE REQUESTS ---
for i in range(10):
    try:
        logger.info(f"Request {i + 1}/10 starting...")
        response = requests.post(url, files=data)
        logger.info(
            f"✅ Request {i + 1} SUCCESS - Response: {len(response.content) / 1024 / 1024:.1f}MB"
        )
    except Exception as e:
        logger.error(f"💥 Request {i + 1} FAILED: {e}")

logger.info("\n--- All requests finished. Final check. ---")
time.sleep(2)  # Give a moment for any final GC to run.

# --- NEW DEBUGGING INSPECTION ---
logger.info("Inspecting proxy actor state for memory leaks...")
try:
    # Use the handle to call our new debugging method.
    report = ray.get(proxy._get_internal_state_for_debugging.remote())
    print("\n--- MEMORY LEAK REPORT ---")
    pprint.pprint(report)
    print("--------------------------\n")

    if report["lingering_alive"] > 0:
        logger.warning(
            "🔴 LEAK DETECTED: Lingering objects found in memory. "
            "Check 'lingering_referrers' in the report above."
        )
    else:
        logger.info("✅ NO LEAK DETECTED: All tracked objects were garbage collected.")

except Exception as e:
    logger.error(f"💥 Failed to get debug report from proxy: {e}")
