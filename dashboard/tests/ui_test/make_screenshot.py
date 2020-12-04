import asyncio
from pyppeteer import launch
import ray
import time
from ray.cluster_utils import Cluster


async def main():
    cluster = Cluster()
    cluster.add_node(dashboard_port=9999)
    cluster.add_node()

    browser = await launch()
    page = await browser.newPage()
    await page.setViewport({
        "width": 1200,
        "height": 800,
    })
    await page.goto('http://localhost:9999', {
        "waitUntil": 'networkidle0',
        "timeout": 2000,
    })
    await page.screenshot({'path': 'dashboard_render.png', "fullPage": "true"})
    await browser.close()

    cluster.shutdown()


asyncio.get_event_loop().run_until_complete(main())