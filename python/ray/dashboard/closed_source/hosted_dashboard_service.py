import argparse

from aiohttp import web

grafana_api_key = "eyJrIjoiNDRjelZmMW9JY0N3aUFFQ3N0bnljdVhDblZaSWFYR08iLCJuIjoiZGV2ZWxvcG1lbnR0ZXN0IiwiaWQiOjF9"
grafana_url = "http://localhost:3000/"

async def health(request):
    return web.json_response({"Status": "Healty"})


async def return_ingest_server_url(request):
    # TODO(sang): Prepare the proper authorization process.
    result = {"ingestor_url": "localhost:50051", "access_token": "1234"}
    return web.json_response(result)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--host",
        required=False,
        type=str,
        default="127.0.0.1",
        help="The host to use for the GRPC server.")
    parser.add_argument(
        "--port",
        required=False,
        default=8080,
        type=str,
        help="The port to use for a server.")
    args = parser.parse_args()
    app = web.Application()
    app.add_routes([
        web.get("/auth", return_ingest_server_url),
        web.get("/health", health)
    ])
    web.run_app(app, host=args.host, port=args.port, shutdown_timeout=5.0)
