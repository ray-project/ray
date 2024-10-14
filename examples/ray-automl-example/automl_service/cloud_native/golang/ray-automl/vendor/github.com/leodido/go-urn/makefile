SHELL := /bin/bash

build: machine.go

images: docs/urn.png

machine.go: machine.go.rl
	ragel -Z -G2 -e -o $@ $<
	@sed -i '/^\/\/line/d' $@
	@$(MAKE) -s file=$@ snake2camel
	@gofmt -w -s $@

docs/urn.dot: machine.go.rl
	@mkdir -p docs
	ragel -Z -e -Vp $< -o $@

docs/urn.png: docs/urn.dot
	dot $< -Tpng -o $@

.PHONY: bench
bench: *_test.go machine.go
	go test -bench=. -benchmem -benchtime=5s ./...

.PHONY: tests
tests: *_test.go machine.go
	go test -race -timeout 10s -coverprofile=coverage.out -covermode=atomic -v ./...

.PHONY: clean
clean:
	@rm -rf docs
	@rm -f machine.go

.PHONY: snake2camel
snake2camel:
	@awk -i inplace '{ \
	while ( match($$0, /(.*)([a-z]+[0-9]*)_([a-zA-Z0-9])(.*)/, cap) ) \
	$$0 = cap[1] cap[2] toupper(cap[3]) cap[4]; \
	print \
	}' $(file)