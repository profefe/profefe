PKG := github.com/profefe/profefe

GO        ?= go
LDFLAGS   :=
GOFLAGS   :=
TESTFLAGS :=

DOCKER ?= docker

BUILDTIME := $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
GITSHA := $(shell git rev-parse --short HEAD 2>/dev/null)

ifndef VERSION
	VERSION := git-$(GITSHA)
endif

LDFLAGS += -X $(PKG)/version.Version=$(VERSION)
LDFLAGS += -X $(PKG)/version.Commit=$(GITSHA)
LDFLAGS += -X $(PKG)/version.BuildTime=$(BUILDTIME)

DOCKER_IMAGE := profefe/profefe
DOCKER_IMAGE_TAG := $(VERSION)

BUILDDIR := BUILD

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

.PHONY: docker-image
docker-image:
	GITSHA=$(GITSHA) VERSION=$(VERSION) \
		./scripts/ci_build_image.sh $(DOCKER_IMAGE) $(DOCKER_IMAGE_TAG)
