#!/usr/bin/env bash

set -eu

: ${CI=}
: ${DOCKER=docker}
: ${BUILDCTL=buildctl}

: ${PLATFORMS=linux/amd64}

PUSH_IMAGE=
if [ "$1" == "--push" ]; then
    PUSH_IMAGE=push
    shift
fi

IMAGE=$1
shift
IMAGE_TAGS="$*"

DOCKERFILE=contrib/docker/Dockerfile

docker_build() {
    local tag_args=
    for tag in $IMAGE_TAGS; do
        tag_args="$tag_args --tag $IMAGE:$tag"
    done

    set -x

    $DOCKER build \
        --build-arg VERSION=$VERSION \
        --build-arg GITSHA=$GITSHA \
        $tag_args -f $DOCKERFILE .
}

buildctl_build() {
    local image_name=
    for tag in $IMAGE_TAGS; do
        [ -n "$image_name" ] && image_name="${image_name},"
        image_name="${image_name}${IMAGE}:${tag}"
    done

    local push_arg=
    if [ "$PUSH_IMAGE" == "push" ]; then
        push_arg=",push=true"
    fi

    set -x

    $BUILDCTL build \
        --progress=plain \
        --frontend=dockerfile.v0 \
        --local context=. --local dockerfile=. \
        --opt filename=$DOCKERFILE \
        --opt platform=$PLATFORMS \
        --opt build-arg:VERSION=\"$VERSION\" \
        --opt build-arg:GITSHA=\"$GITSHA\" \
        --output type=image,\"name=$image_name\"$push_arg
}

if [ "$CI" == "true" ]; then
    buildctl_build
else
    docker_build
fi
