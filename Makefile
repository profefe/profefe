PKG := github.com/profefe/profefe

GO        ?= go
LDFLAGS   :=
GOFLAGS   :=
TESTFLAGS :=

BUILDTIME := $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
GITSHA := $(shell git rev-parse --short HEAD 2>/dev/null)

ifndef VERSION
	VERSION := git-$(GITSHA)
endif

LDFLAGS += -X $(PKG)/version.Version=$(VERSION)
LDFLAGS += -X $(PKG)/version.Commit=$(GITSHA)
LDFLAGS += -X $(PKG)/version.BuildTime=$(BUILDTIME)

BUILDDIR := BUILD

# Required for globs to work correctly
SHELL := /bin/bash

BUILD.go = $(GO) build $(GOFLAGS)
TEST.go  = $(GO) test $(TESTFLAGS)

DOCKER_BUILD_ARGS += --build-arg VERSION=$(VERSION)
DOCKER_BUILD_ARGS += --build-arg GITSHA=$(GITSHA)
DOCKER_BUILD_ARGS += --build-arg BUILDTIME=$(BUILDTIME)

.PHONY: all
all: build-profefe

build-%:
	$(BUILD.go) -ldflags "$(LDFLAGS)" -o $(BUILDDIR)/$(*) ./cmd/$(*)

.PHONY: deploy
deploy:

.PHONY: test
test:
	$(TEST.go) -ldflags "$(LDFLAGS)" ./...

container:
	docker build $(DOCKER_BUILD_ARGS) -f ./contrib/docker/Dockerfile -t profefe/profefe:$(GITSHA) .
