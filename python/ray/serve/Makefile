format:
	isort -y --recursive .
	black .
clean:
	find . | grep __pycache__ | xargs rm -rf
	find . | grep .benchmarks | xargs rm -rf