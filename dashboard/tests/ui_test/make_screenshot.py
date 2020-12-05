import asyncio
from pyppeteer import launch
import ray
import time
from ray.cluster_utils import Cluster
import requests
import sys


async def main():
    cluster = Cluster()
    cluster.add_node(dashboard_port=9999)
    cluster.add_node()

    max_try = 10
    while True:
        try:
            requests.get("http://localhost:9999")
            break
        except Exception as e:
            print(e)
            time.sleep(0.5)
            max_try -= 1
            if max_try == 0:
                sys.exit(1)

    browser = await launch(
        # executablePath='/usr/bin/google-chrome-stable',
        args=["--no-sandbox", '--disable-setuid-sandbox'])
    page = await browser.newPage()
    await page.setViewport({
        "width": 1200,
        "height": 800,
    })
    await page.goto('http://localhost:9999', {
        "waitUntil": 'networkidle0',
        "timeout": 5000,
    })
    await asyncio.sleep(5)
    await page.screenshot({'path': 'dashboard_render.png', "fullPage": "true"})
    await browser.close()

    cluster.shutdown()


asyncio.get_event_loop().run_until_complete(main())