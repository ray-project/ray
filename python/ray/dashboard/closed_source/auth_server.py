from aiohttp import web

async def handle(request):
    print(request.match_info)
    text = {'good': 'bad'}
    return web.json_response(text)

async def return_ingest_server_url(request):
    result = {
        'ingestor_url': 'localhost:50051',
        'access_token': '1234'
    }
    return web.json_response(result)

app = web.Application()
app.add_routes([web.get('/', handle),
                web.get('/auth', return_ingest_server_url)])

if __name__ == '__main__':
    host, port = '127.0.0.1', 8080
    web.run_app(app, host=host, port=port, shutdown_timeout=5.0)