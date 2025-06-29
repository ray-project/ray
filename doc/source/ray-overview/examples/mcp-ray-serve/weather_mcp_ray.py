from typing import Any
import httpx
from fastapi import FastAPI
from mcp.server.fastmcp import FastMCP
from ray import serve
from contextlib import asynccontextmanager

# Constants.
NWS_API_BASE = "https://api.weather.gov"
USER_AGENT = "weather-app/1.0"

# Helper Functions.
async def make_nws_request(url: str) -> dict[str, Any] | None:
    headers = {"User-Agent": USER_AGENT, "Accept": "application/geo+json"}
    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            resp = await client.get(url, headers=headers)
            resp.raise_for_status()
            return resp.json()
        except Exception:
            return None


def format_alert(feature: dict) -> str:
    props = feature["properties"]
    return (
        f"Event: {props.get('event', 'Unknown')}\n"
        f"Area: {props.get('areaDesc', 'Unknown')}\n"
        f"Severity: {props.get('severity', 'Unknown')}\n"
        f"Description: {props.get('description', 'No description available')}\n"
        f"Instructions: {props.get('instruction', 'No specific instructions provided')}"
    )

# Instantiate FastMCP and register tools via decorators.
mcp = FastMCP("weather", stateless_http=True)

@mcp.tool()
async def get_alerts(state: str) -> str:
    """Fetch active alerts for a given state code (e.g., 'CA')."""
    url = f"{NWS_API_BASE}/alerts/active/area/{state}"
    data = await make_nws_request(url)
    if not data or "features" not in data:
        return "Unable to fetch alerts or no alerts found."
    features = data["features"]
    if not features:
        return "No active alerts for this state."
    return "\n---\n".join(format_alert(f) for f in features)

@mcp.tool()
async def get_forecast(latitude: float, longitude: float) -> str:
    """Fetch a 5-period weather forecast for given lat/lon."""
    points_url = f"{NWS_API_BASE}/points/{latitude},{longitude}"
    points_data = await make_nws_request(points_url)
    if not points_data or "properties" not in points_data:
        return "Unable to fetch forecast data for this location."

    forecast_url = points_data["properties"].get("forecast")
    if not forecast_url:
        return "No forecast URL found for this location."

    forecast_data = await make_nws_request(forecast_url)
    if not forecast_data or "properties" not in forecast_data:
        return "Unable to fetch detailed forecast."

    periods = forecast_data["properties"].get("periods", [])
    if not periods:
        return "No forecast periods available."

    parts: list[str] = []
    for p in periods[:5]:
        parts.append(
            f"{p['name']}:\nTemperature: {p['temperature']}°{p['temperatureUnit']}\n" +
            f"Wind: {p['windSpeed']} {p['windDirection']}\n" +
            f"Forecast: {p['detailedForecast']}"
        )
    return "\n---\n".join(parts)

## FastAPI app and Ray Serve setup.
@asynccontextmanager
async def lifespan(app: FastAPI):
    # 1) Mount the MCP app.
    app.mount("/", mcp.streamable_http_app())

    # 2) Enter the session_manager's context.
    async with mcp.session_manager.run():
        yield

fastapi_app = FastAPI(lifespan=lifespan)

@serve.deployment(
    autoscaling_config={
        "min_replicas": 2, 
        "max_replicas": 20, 
        "target_ongoing_requests": 5
        },
    ray_actor_options={"num_cpus": 0.2}
)
@serve.ingress(fastapi_app)
class WeatherMCP:
    def __init__(self):
        pass
       

# Ray Serve entry point.
app = WeatherMCP.bind()

## Run in terminal.
# serve run weather_mcp_ray:app
