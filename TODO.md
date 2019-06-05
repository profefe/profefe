# TODO

## General

- more testing
- (?) protobuf instead json for rpc

## Agent

- (?) more logging
- collect and send mutex and other profiles

## Collector

- persistent storage
  tarantool? cassandra? postgres?
- vacuum: remove old profiles base on some retention
- horizontal scaling

## UI

- profiles viewer
  - multi profiles view: heatmap, linesgraph
  - single profile view: top, graph, flamegraph
- see https://github.com/Netflix/flamescope
