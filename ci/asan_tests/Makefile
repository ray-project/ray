SHELL=/bin/bash

echo:
	@echo "Available commands: setup, run, recompile"

setup:
	echo "Setting up the environment"
	pip install -r ray-project/requirements.txt
	pip install -U pytest

	echo "Installing cython example"
	cd "${HOME}"/ray/doc/examples/cython; python setup.py install --user

	echo "Settting up the shell"
	cp bazelrc "${HOME}"/.bazelrc # Setup cache
	echo "LD_PRELOAD=/usr/lib/gcc/x86_64-linux-gnu/7/libasan.so" >> "${HOME}"/.bashrc
	echo "ASAN_OPTIONS=detect_leaks=0" >> "${HOME}"/.bashrc

	echo "Compiling ray"
	cd "${HOME}"/ray; git fetch; git pull origin master
	cd "${HOME}"/ray/python; pip install -e . --verbose || true

run:
	./run_asan_tests.sh

recompile:
	cd "${HOME}"/ray; git fetch; git checkout "${git_sha}"
	cd "${HOME}"/ray/python; pip install -e . --verbose || true
