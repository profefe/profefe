FROM golang:1.13.4-alpine3.10 as builder

env GO111MODULE=off
env SHELL=/bin/sh

ARG VERSION
ARG GITSHA
ARG BUILDTIME

RUN apk add --update --no-cache make
COPY . /go/src/github.com/profefe/profefe
WORKDIR /go/src/github.com/profefe/profefe
RUN make SHELL=$SHELL GITSHA=$GITSHA

FROM alpine

COPY --from=builder /go/src/github.com/profefe/profefe/BUILD/profefe /profefe

ENTRYPOINT ["/profefe"]
