noop:
	@echo "please specify which baseline to run"

uvicorn:
	uvicorn uvicorn_app:app --no-access-log --workers 1

fastapi:
	uvicorn fastapi_app:app --no-access-log --workers 1
