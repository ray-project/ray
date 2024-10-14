all: build

.PHONY: init
init:
	git submodule update --init --recursive

.PHONY: deps
deps:
	go install github.com/UnnoTed/fileb0x@v1.1.4

.PHONY: build
build:
	fileb0x fileb0x/b0x.yaml