FROM golang:1.14.2-alpine3.11 as builder

RUN apk add --update --no-cache build-base

ARG VERSION
ARG GITSHA

COPY . /go/src/github.com/profefe/profefe
WORKDIR /go/src/github.com/profefe/profefe
RUN make VERSION=$VERSION GITSHA=$GITSHA

FROM alpine

COPY --from=builder /go/src/github.com/profefe/profefe/BUILD/profefe /profefe

ENTRYPOINT ["/profefe"]
