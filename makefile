export PATH := $(GOPATH)/bin:$(PATH)

all: build test

gettest:
	@echo "--> go get..."
	go get github.com/stretchr/testify/assert
	go get github.com/pierrre/gotestcover

build:
	@echo "--> Building..."
	go build -v -o bin/mydumper cmd/mydumper/main.go
	go build -v -o bin/myloader cmd/myloader/main.go
	@chmod 755 bin/*

clean:
	@echo "--> Cleaning..."
	@go clean
	@rm -f bin/*

fmt:
	go fmt ./...
	go vet ./...

test:
	@$(MAKE) gettest
	@echo "--> Testing..."
	@$(MAKE) testcommon

testcommon:
	go test -race -v common

# code coverage
COVPKGS =	common
coverage:
	@$(MAKE) gettest
	go build -v -o bin/gotestcover \
	$(GOPATH)/src/github.com/pierrre/gotestcover/*.go;
	bin/gotestcover -coverprofile=coverage.out -v $(COVPKGS)
	go tool cover -html=coverage.out
.PHONY: build clean fmt test coverage
