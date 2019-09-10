# TODO

## General

- more testing

## Agent

- more logging
- collect and send mutex and other profiles

## Collector

- add metrics
  prometheus? x/vars?
- persistent storage
  tarantool? cassandra? postgres?
- vacuum: remove old profiles base on some retention
- horizontal scaling

## UI

- profiles viewer
  - multi profiles view: heatmap, linesgraph
  - single profile view: top, graph, flamegraph
- see https://github.com/Netflix/flamescope
