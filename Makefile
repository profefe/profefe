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

.PHONY: all
all: build-profefe

build-%:
	$(BUILD.go) -ldflags "$(LDFLAGS)" -o $(BUILDDIR)/$(*) ./cmd/$(*)

.PHONY: deploy
deploy:

.PHONY: test
test:
	$(TEST.go) -ldflags "$(LDFLAGS)" ./...

.PHONY: postgres-integration-test
postgres-integration-test:
	-$(GO) clean -testcache
	$(TEST.go) -ldflags "$(LDFLAGS)" -tags=integration ./pkg/storage/postgres/... $(STORAGEFLAGS)
