COMMONENVVAR=GOOS=$(shell uname -s | tr A-Z a-z) GOARCH=$(subst x86_64,amd64,$(patsubst i%86,386,$(shell uname -m)))
BUILDENVVAR=CGO_ENABLED=0

.PHONY: all
all: build

.PHONY: build
build: build-extension

.PHONY: build-extension
build-extension: 
	$(COMMONENVVAR) $(BUILDENVVAR) go build -ldflags '-w' -o bin/pytorch-operator-extension cmd/main.go

.PHONY: fixcodec
	hack/fix-codec-factory.sh
