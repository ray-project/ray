# Ray Wheel + Image Build
#
# Usage:
#   make wheel [PY] [TARGET]
#   python build_wheel_local.py [OPTIONS] [PYTHON_VERSION] [TARGET]

.PHONY: wheel wheel-test wheel-clean matrix-wheels clean help print-vars

ROOT := $(abspath $(dir $(lastword $(MAKEFILE_LIST)))/../..)
BUILD_DIR := $(dir $(lastword $(MAKEFILE_LIST)))
WHEEL_DIR := $(ROOT)/.whl

# Defaults (overridable via vars or positionals)
PY ?= 3.10
TARGET ?= ray              # ray|cpp|all
DRY_RUN ?=                 # set to any value for dry-run

PY_VERS ?= 3.11 3.12

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
	@echo "  matrix:"
	@echo "    make matrix-wheels TARGET=all PY_VERS='3.11 3.12'"
	@echo ""
	@echo "Other:"
	@echo "  make wheel-clean"
	@echo "  make clean"
	@echo ""
	@echo "Notes:"
	@echo "  - Wheels are extracted to $(WHEEL_DIR)/"

print-vars:
	@echo "ROOT=$(ROOT)"
	@echo "BUILD_DIR=$(BUILD_DIR)"
	@echo "PY=$(PY)"
	@echo "TARGET=$(TARGET)"
	@echo "ARGS=$(ARGS)"
	@echo "PY_VERS=$(PY_VERS)"
	@echo "WHEEL_DIR=$(WHEEL_DIR)"

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
	python3 $(BUILD_DIR)/build_wheel_local.py $$py $$target $(if $(DRY_RUN),--dry-run)

wheel-clean:
	rm -rf $(WHEEL_DIR)/

matrix-wheels:
	@set -e; \
	for py in $(PY_VERS); do \
	  echo "==> wheels: PY=$$py TARGET=$(TARGET)"; \
	  $(MAKE) wheel $$py $(TARGET); \
	done

clean: wheel-clean
