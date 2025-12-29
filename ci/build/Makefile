# Ray Wheel Build
#
# Usage:
#   make wheel PY=3.10        		# Build ray wheel
#   make wheel-cpp PY=3.10    		# Build ray-cpp wheel
#   make wheel-all PY=3.10    		# Build both
#   make wheel-test PY=3.10         # Test the wheel
#   make wheel-clean                # Clean .whl/ directory

.PHONY: wheel wheel-cpp wheel-all test clean help

PY ?= 3.10
CUDA ?= cpu               # only relevant for image targets
PKG ?= ray                # ray|cpp|all (used by wheel targets)

PY_VERS ?= 3.11 3.12
CUDA_VERS ?= cpu cu121

ROOT := $(abspath $(dir $(lastword $(MAKEFILE_LIST)))/../..)
WHEEL_DIR := $(ROOT)/.whl
LOG_DIR := $(ROOT)/.logs

BUILD_WHEEL := $(ROOT)/ci/build/build-wheel-local.sh
TEST_WHEEL  := $(ROOT)/ci/build/test-wheel-local.sh
BUILD_IMAGE := $(ROOT)/ci/build/build-image-local.sh

help:
	@echo "Wheel (PY only):"
	@echo "  make wheel PY=3.12"
	@echo "  make wheel-cpp PY=3.12"
	@echo "  make wheel-all PY=3.12"
	@echo "  make matrix-wheels PY_VERS='3.11 3.12'"
	@echo ""
	@echo "Image (PY x CUDA):"
	@echo "  make image PKG=ray PY=3.12 CUDA=cu121"
	@echo "  make matrix-images PY_VERS='3.11 3.12' CUDA_VERS='cpu cu121'"
	@echo ""
	@echo "Other:"
	@echo "  make wheel-test"
	@echo "  make wheel-clean"
	@echo "  make clean"
	@echo ""
	@echo "Notes:"
	@echo "  - CUDA does NOT affect wheels today; only images."
	@echo "  - Wheels are extracted to $(WHEEL_DIR)/"

print-vars:
	@echo "ROOT=$(ROOT)"
	@echo "PY=$(PY)"
	@echo "CUDA=$(CUDA)"
	@echo "PKG=$(PKG)"
	@echo "PY_VERS=$(PY_VERS)"
	@echo "CUDA_VERS=$(CUDA_VERS)"
	@echo "WHEEL_DIR=$(WHEEL_DIR)"
	@echo "IMAGE_REPO=$(IMAGE_REPO)"
	@echo "IMAGE_TAG=$(IMAGE_TAG)"

wheel:
	$(ROOT)/ci/build/build-wheel-local.sh $(PY) ray

wheel-cpp:
	$(ROOT)/ci/build/build-wheel-local.sh $(PY) cpp

wheel-all:
	$(ROOT)/ci/build/build-wheel-local.sh $(PY) all

wheel-test:
	$(ROOT)/ci/build/test-wheel-local.sh

wheel-clean:
	rm -rf $(ROOT)/.whl/

matrix-wheels:
	@set -e; \
	for py in $(PY_VERS); do \
	  echo "==> wheels: PY=$$py"; \
	  $(MAKE) wheel-all PY=$$py; \
	done

.DEFAULT_GOAL := help
