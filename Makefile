PKG := github.com/profefe/profefe

GO    ?= go

LDFLAGS   :=
GOFLAGS   :=
TESTFLAGS :=

BUILDTIME := $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
GITSHA := $(shell git rev-parse --short HEAD 2>/dev/null)

ifndef VERSION
	VERSION := git-$(GITSHA)
endif

GOFLAGS += -trimpath

LDFLAGS += -X $(PKG)/version.version=$(VERSION)
LDFLAGS += -X $(PKG)/version.commit=$(GITSHA)
LDFLAGS += -X $(PKG)/version.buildTime=$(BUILDTIME)

BINDIR := $(CURDIR)/bin
BUILDDIR := $(CURDIR)/BUILD

DOCKER := docker

DOCKER_IMAGE := profefe/profefe
DOCKER_IMAGE_TAG := $(VERSION)

STATICCHECK := $(BINDIR)/staticcheck

BUILD.go = $(GO) build $(GOFLAGS)
TEST.go  = $(GO) test $(TESTFLAGS)

.DEFAULT_GOAL := all
.SUFFIXES:

.PHONY: all
all: build-profefe

build-%:
	$(BUILD.go) -ldflags "$(LDFLAGS)" -o $(BUILDDIR)/$(*) ./cmd/$(*)

.PHONY: deploy
deploy:

.PHONY: test
test:
	$(TEST.go) -ldflags "$(LDFLAGS)" ./...

.PHONY: staticcheck
staticcheck:
	$(STATICCHECK) ./...

.PHONY: docker-image
docker-image:
	GITSHA=$(GITSHA) VERSION=$(VERSION) \
		./scripts/ci_build_image.sh $(DOCKER_IMAGE) $(DOCKER_IMAGE_TAG)

HAS_staticcheck := $(shell command -v $(STATICCHECK);)

.PHONY: bootstrap
bootstrap:
ifndef HAS_staticcheck
	@echo "Installing staticcheck..."
	GOBIN=$(BINDIR) $(GO) install -trimpath ./vendor/honnef.co/go/tools/cmd/staticcheck
endif
