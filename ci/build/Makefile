# Ray Wheel + Image Build
#
# Scripts:
#   build-wheel-local.sh [PYTHON_VERSION] [TARGET]
#   build-ray-image-local.sh [PYTHON_VERSION] [PLATFORM]

.PHONY: wheel wheel-test wheel-clean matrix-wheels image matrix-images clean help print-vars

ROOT := $(abspath $(dir $(lastword $(MAKEFILE_LIST)))/../..)
WHEEL_DIR := $(ROOT)/.whl
LOG_DIR := $(ROOT)/.logs

BUILD_WHEEL := $(ROOT)/ci/build/build-wheel-local.sh
TEST_WHEEL  := $(ROOT)/ci/build/test-wheel-local.sh
BUILD_IMAGE := $(ROOT)/ci/build/build-ray-image-local.sh

# Defaults (overridable via vars or positionals)
PY ?= 3.10
TARGET ?= ray              # ray|cpp|all
PLATFORM ?= cpu            # cpu|cu<cuda_version>

PY_VERS ?= 3.11 3.12
PLATFORMS ?= cpu cu11.8.0-cudnn8 cu12.1.1-cudnn8

# Extra "goals" after a target are treated as args.
ARGS := $(wordlist 2,$(words $(MAKECMDGOALS)),$(MAKECMDGOALS))
$(eval $(ARGS):;@:)

help:
	@echo "Usage (vars or positional):"
	@echo ""
	@echo "  wheel:"
	@echo "    make wheel                        # defaults: PY=3.10 TARGET=ray"
	@echo "    make wheel 3.12                   # PY=3.12 TARGET=ray"
	@echo "    make wheel 3.12 cpp               # PY=3.12 TARGET=cpp"
	@echo "    make wheel PY=3.12 TARGET=cpp"
	@echo ""
	@echo "  wheel-test:"
	@echo "    make wheel-test                   # defaults: PY=3.10"
	@echo "    make wheel-test 3.12"
	@echo "    make wheel-test PY=3.12"
	@echo ""
	@echo "  image:"
	@echo "    make image                        # defaults: PY=3.10 PLATFORM=cpu"
	@echo "    make image 3.12                   # PY=3.12 PLATFORM=cpu"
	@echo "    make image 3.12 cu12.1.1-cudnn8"
	@echo "    make image PY=3.12 PLATFORM=cu12.1.1-cudnn8"
	@echo ""
	@echo "  matrix:"
	@echo "    make matrix-wheels TARGET=all PY_VERS='3.11 3.12'"
	@echo "    make matrix-images PY_VERS='3.11 3.12' PLATFORMS='cpu cu12.1.1-cudnn8'"
	@echo ""
	@echo "Other:"
	@echo "  make wheel-clean"
	@echo "  make clean"
	@echo ""
	@echo "Notes:"
	@echo "  - PLATFORM does NOT affect wheels today; only images."
	@echo "  - Wheels are extracted to $(WHEEL_DIR)/"

print-vars:
	@echo "ROOT=$(ROOT)"
	@echo "PY=$(PY)"
	@echo "TARGET=$(TARGET)"
	@echo "PLATFORM=$(PLATFORM)"
	@echo "ARGS=$(ARGS)"
	@echo "PY_VERS=$(PY_VERS)"
	@echo "PLATFORMS=$(PLATFORMS)"
	@echo "WHEEL_DIR=$(WHEEL_DIR)"
	@echo "LOG_DIR=$(LOG_DIR)"
	@echo "BUILD_WHEEL=$(BUILD_WHEEL)"
	@echo "BUILD_IMAGE=$(BUILD_IMAGE)"
	@echo "TEST_WHEEL=$(TEST_WHEEL)"

wheel:
	@set -e; \
	py="$(PY)"; target="$(TARGET)"; \
	if [ "$(strip $(ARGS))" != "" ]; then \
	  set -- $(ARGS); \
	  case "$$1" in 3.*) ;; *) echo "Error: positional usage requires python first: make wheel <3.x> [ray|cpp|all]"; exit 1;; esac; \
	  py="$$1"; \
	  if [ $$# -ge 2 ]; then target="$$2"; fi; \
	  if [ $$# -gt 2 ]; then echo "Error: too many args. Usage: make wheel <3.x> [ray|cpp|all]"; exit 1; fi; \
	fi; \
	echo "==> wheel: PY=$$py TARGET=$$target"; \
	$(BUILD_WHEEL) "$$py" "$$target"

wheel-test:
	@set -e; \
	py="$(PY)"; \
	if [ "$(strip $(ARGS))" != "" ]; then \
	  set -- $(ARGS); \
	  case "$$1" in 3.*) ;; *) echo "Error: positional usage requires python first: make wheel-test <3.x>"; exit 1;; esac; \
	  py="$$1"; \
	  if [ $$# -gt 1 ]; then echo "Error: too many args. Usage: make wheel-test <3.x>"; exit 1; fi; \
	fi; \
	echo "==> wheel-test: PY=$$py"; \
	PYTHON_VERSION="$$py" $(TEST_WHEEL)

wheel-clean:
	rm -rf $(WHEEL_DIR)/

matrix-wheels:
	@set -e; \
	for py in $(PY_VERS); do \
	  echo "==> wheels: PY=$$py TARGET=$(TARGET)"; \
	  $(MAKE) wheel $$py $(TARGET); \
	done

image:
	@set -e; \
	py="$(PY)"; platform="$(PLATFORM)"; \
	if [ "$(strip $(ARGS))" != "" ]; then \
	  set -- $(ARGS); \
	  case "$$1" in 3.*) ;; *) echo "Error: positional usage requires python first: make image <3.x> [cpu|cu...]"; exit 1;; esac; \
	  py="$$1"; \
	  if [ $$# -ge 2 ]; then platform="$$2"; fi; \
	  if [ $$# -gt 2 ]; then echo "Error: too many args. Usage: make image <3.x> [cpu|cu...]"; exit 1; fi; \
	fi; \
	echo "==> image: PY=$$py PLATFORM=$$platform"; \
	$(BUILD_IMAGE) "$$py" "$$platform"

matrix-images:
	@set -e; \
	for py in $(PY_VERS); do \
	  for platform in $(PLATFORMS); do \
	    echo "==> image: PY=$$py PLATFORM=$$platform"; \
	    $(MAKE) image $$py $$platform; \
	  done; \
	done

clean: wheel-clean
	@true

.DEFAULT_GOAL := help
