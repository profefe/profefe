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

IMAGE_VERSION := $(VERSION)
DOCKER_IMAGE := profefe/profefe

DOCKER_BUILD_ARGS += --build-arg VERSION=$(VERSION)
DOCKER_BUILD_ARGS += --build-arg GITSHA=$(GITSHA)

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
	$(DOCKER) build $(DOCKER_BUILD_ARGS) -t $(DOCKER_IMAGE):$(IMAGE_VERSION) -f ./contrib/docker/Dockerfile .

.PHONY: docker-push-image
docker-push-image: docker-image
	$(DOCKER) tag $(DOCKER_IMAGE):$(IMAGE_VERSION) $(DOCKER_IMAGE):latest
	$(DOCKER) push $(DOCKER_IMAGE):$(IMAGE_VERSION)
	$(DOCKER) push $(DOCKER_IMAGE):latest
