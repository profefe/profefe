FROM golang:1.15.7-alpine as builder

RUN apk add --update --no-cache build-base

ARG VERSION
ARG GITSHA

WORKDIR /go/src/github.com/profefe/profefe
COPY . /go/src/github.com/profefe/profefe
RUN make VERSION=$VERSION GITSHA=$GITSHA

FROM alpine
COPY --from=builder /go/src/github.com/profefe/profefe/BUILD/profefe /profefe
ENTRYPOINT ["/profefe"]
