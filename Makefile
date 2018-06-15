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

BUILDDIR  := BUILD

# Required for globs to work correctly
SHELL := /bin/bash

BUILD.go = $(GO) build $(GOFLAGS)
TEST.go  = $(GO) test $(TESTFLAGS)

all:

build:
	$(BUILD.go) -ldflags "$(LDFLAGS)" -o $(BUILDDIR)/collector $(PKG)/cmd/collector

deploy:

run: build
	$(BUILDDIR)/collector

.PHONY: all build deploy run test
