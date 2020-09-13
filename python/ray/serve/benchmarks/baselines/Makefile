noop:
	@echo "please specify which baseline to run"

uvicorn:
	uvicorn uvicorn_app:app --no-access-log --workers 1

fastapi:
	uvicorn fastapi_app:app --no-access-log --workers 1

bench:
	wrk -c 100 -t 10 -d 10s http://127.0.0.1:8000

