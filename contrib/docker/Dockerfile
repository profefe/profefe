FROM golang:1.13.6-alpine3.10 as builder

RUN apk add --update --no-cache make

env GO111MODULE=off
ARG VERSION
ARG GITSHA

COPY . /go/src/github.com/profefe/profefe
WORKDIR /go/src/github.com/profefe/profefe
RUN make VERSION=$VERSION GITSHA=$GITSHA

FROM alpine

COPY --from=builder /go/src/github.com/profefe/profefe/BUILD/profefe /profefe

ENTRYPOINT ["/profefe"]
