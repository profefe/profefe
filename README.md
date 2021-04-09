# profefe

[![Build Status](https://travis-ci.com/profefe/profefe.svg?branch=master)](https://travis-ci.com/profefe/profefe)
[![Go Report Card](https://goreportcard.com/badge/github.com/profefe/profefe)](https://goreportcard.com/report/github.com/profefe/profefe)
[![Docker Pulls](https://img.shields.io/docker/pulls/profefe/profefe.svg)][hub.docker]
[![MIT licensed](https://img.shields.io/badge/license-MIT-blue.svg)](https://raw.githubusercontent.com/profefe/profefe/master/LICENSE)

profefe, a continuous profiling system, collects profiling data from a fleet of running applications and provides API for querying
profiling samples for postmortem performance analysis.

## Why Continuous Profiling?

"[Continuous Profiling and Go](https://medium.com/@tvii/continuous-profiling-and-go-6c0ab4d2504b)" describes
the motivation behind profefe:

> With the increase in momentum around the term “observability” over the last few years, there is a common misconception
> amongst the developers, that observability is exclusively about _metrics_, _logs_ and _tracing_ (a.k.a. “three pillars of observability”)
> [..] With metrics and tracing, we can see the system on a macro-level. Logs only cover the known parts of the system.
> Performance profiling is another signal that uncovers the micro-level of a system; continuous profiling allows
> observing how the components of the application and the infrastructure it runs in, influence the overall system.

## How does it work?

See [Design Docs](DESIGN.md) documentation.

## Quickstart

To build and start *profefe collector*, run:

```shell-session
$ make
$ ./BUILD/profefe -addr=localhost:10100 -storage-type=badger -badger.dir=/tmp/profefe-data

2019-06-06T00:07:58.499+0200    info    profefe/main.go:86    server is running    {"addr": ":10100"}
```

The command above starts *profefe collector* backed by [BadgerDB](https://github.com/dgraph-io/badger) as a storage for profiles. profefe supports other storage types: S3, Google Cloud Storage and [ClickHouse](https://clickhouse.tech/).

Run `./BUILD/profefe -help` to show the list of all available options.

### Example application

profefe ships with a fork of [Google Stackdriver Profiler's example application][5], modified to use *profefe agent*, that sends profiling data to profefe collector.

To start the example application run the following command in a separate terminal window:

```shell-session
$ go run ./examples/hotapp/main.go
```

After a brief period, the application will start sending CPU profiles to profefe collector. 

```shell-session
send profile: http://localhost:10100/api/0/profiles?service=hotapp-service&labels=version=1.0.0&type=cpu
send profile: http://localhost:10100/api/0/profiles?service=hotapp-service&labels=version=1.0.0&type=cpu
send profile: http://localhost:10100/api/0/profiles?service=hotapp-service&labels=version=1.0.0&type=cpu
```

With profiling data persisted, query the profiles from the collector using its HTTP API (_refer to [documentation for collector's HTTP API](#http-api) below_). As an example, request all profiling data associated with the given meta-information (service name and a time frame), as a single *merged* profile:

```shell-session
$ go tool pprof 'http://localhost:10100/api/0/profiles/merge?service=hotapp-service&type=cpu&from=2019-05-30T11:49:00&to=2019-05-30T12:49:00&labels=version=1.0.0'

Fetching profile over HTTP from http://localhost:10100/api/0/profiles...
Saved profile in /Users/varankinv/pprof/pprof.samples.cpu.001.pb.gz
Type: cpu

(pprof) top
Showing nodes accounting for 43080ms, 99.15% of 43450ms total
Dropped 53 nodes (cum <= 217.25ms)
Showing top 10 nodes out of 12
      flat  flat%   sum%        cum   cum%
   42220ms 97.17% 97.17%    42220ms 97.17%  main.load
     860ms  1.98% 99.15%      860ms  1.98%  runtime.nanotime
         0     0% 99.15%    21050ms 48.45%  main.bar
         0     0% 99.15%    21170ms 48.72%  main.baz
         0     0% 99.15%    42250ms 97.24%  main.busyloop
         0     0% 99.15%    21010ms 48.35%  main.foo1
         0     0% 99.15%    21240ms 48.88%  main.foo2
         0     0% 99.15%    42250ms 97.24%  main.main
         0     0% 99.15%    42250ms 97.24%  runtime.main
         0     0% 99.15%     1020ms  2.35%  runtime.mstart
```

profefe includes a tool, that allows importing existing pprof data into the collector. While *profefe collector* is still running, run the following:

```shell-session
$ ./scripts/pprof_import.sh --service service1 --label region=europe-west3 --label host=backend1 --type cpu -- path/to/cpu.prof

uploading service1-cpu-backend1-20190313-0948Z.prof...OK
```

### Using Docker

You can build a docker image with profefe collector, by running the command:

```shell-session
$ make docker-image
```

The documentation about running profefe in docker is in [contrib/docker/README.md](./contrib/docker/README.md).

## HTTP API

### Store pprof-formatted profile

```
POST /api/0/profiles?service=<service>&type=[cpu|heap|...]&labels=<key=value,key=value>
body pprof.pb.gz

< HTTP/1.1 200 OK
< Content-Type: application/json
<
{
  "code": 200,
  "body": {
    "id": <id>,
    "type": <type>,
    ···
  }
}
```

- `service` — service name (string)
- `type` — profile type ("cpu", "heap", "block", "mutex", "goroutine", "threadcreate", or "other")
- `labels` — a set of key-value pairs, e.g. "region=europe-west3,dc=fra,ip=1.2.3.4,version=1.0" (Optional)

**Example**

```shell-session
$ curl -XPOST \
  "http://<profefe>/api/0/profiles?service=api-backend&type=cpu&labels=region=europe-west3,dc=fra" \
  --data-binary "@$HOME/pprof/api-backend-cpu.prof"
```

#### Store runtime execution traces (experimental)

Go's [runtime traces](https://golang.org/pkg/runtime/trace/) are a special case of profiling data, that can be stored
and queried with profefe.

Currently, profefe doesn't support extracting the timestamp of when the trace was created. Client may provide
this information via `created_at` parameter, see below.

```
POST /api/0/profiles?service=<service>&type=trace&created_at=<created_at>&labels=<key=value,key=value>
body trace.out

< HTTP/1.1 200 OK
< Content-Type: application/json
<
{
  "code": 200,
  "body": {
    "id": <id>,
    "type": "trace",
    ···
  }
}
```

- `service` — service name (string)
- `type` — profile type ("trace")
- `created_at` — trace profile creation time, e.g. "2006-01-02T15:04:05" (defaults to server's current time)
- `labels` — a set of key-value pairs, e.g. "region=europe-west3,dc=fra,ip=1.2.3.4,version=1.0" (Optional)


**Example**

```shell-session
$ curl -XPOST \
  "http://<profefe>/api/0/profiles?service=api-backend&type=trace&created_at=2019-05-01T18:45:00&labels=region=europe-west3,dc=fra" \
  --data-binary "@$HOME/pprof/api-backend-trace.out"
```

### Query meta information about stored profiles

```
GET /api/0/profiles?service=<service>&type=<type>&from=<created_from>&to=<created_to>&labels=<key=value,key=value>

< HTTP/1.1 200 OK
< Content-Type: application/json
<
{
  "code": 200,
  "body": [
    {
      "id": <id>,
      "type": <type>
    },
    ···
  ]
}
```

- `service` — service name
- `from`, `to` — a time frame in which profiling data was collected, e.g. "from=2006-01-02T15:04:05"
- `type` — profile type ("cpu", "heap", "block", "mutex", "goroutine", "threadcreate", "trace", "other") (Optional)
- `labels` — a set of key-value pairs, e.g. "region=europe-west3,dc=fra,ip=1.2.3.4,version=1.0" (Optional)

**Example**

```shell-session
$ curl "http://<profefe>/api/0/profiles?service=api-backend&type=cpu&from=2019-05-01T17:00:00&to=2019-05-25T00:00:00"
```

### Query saved profiling data returning it as a single merged profile

```
GET /api/0/profiles/merge?service=<service>&type=<type>&from=<created_from>&to=<created_to>&labels=<key=value,key=value>

< HTTP/1.1 200 OK
< Content-Type: application/octet-stream
< Content-Disposition: attachment; filename="pprof.pb.gz"
<
pprof.pb.gz
```

Request parameters are the same as for querying meta information.

*Note, "type" parameter is required; merging runtime traces is not supported.*

### Return individual profile as pprof-formatted data

```
GET /api/0/profiles/<id>

< HTTP/1.1 200 OK
< Content-Type: application/octet-stream
< Content-Disposition: attachment; filename="pprof.pb.gz"
<
pprof.pb.gz
```

- `id` - id of stored profile, returned with the request for meta information above

#### Merge a set of individual profiles into a single profile

```
GET /api/0/profiles/<id1>+<id2>+...

< HTTP/1.1 200 OK
< Content-Type: application/octet-stream
< Content-Disposition: attachment; filename="pprof.pb.gz"
<
pprof.pb.gz
```

- `id1`, `id2` - ids of stored profiles

*Note, merging is possible only for profiles of the same type; merging runtime traces is not supported.*

### Get services for which profiling data is stored

```
GET /api/0/services

< HTTP/1.1 200 OK
< Content-Type: application/json
<
{
  "code": 200,
  "body": [
    <service1>,
    ···
  ]
}
```

### Get profefe server version

```
GET /api/0/version

< HTTP/1.1 200 OK
< Content-Type: application/json
<
{
  "code": 200,
  "body": {
    "version": <version>,
    "commit": <git revision>,
    "build_time": <build timestamp>"
  }
}
```

## FAQ

### Does continuous profiling affect the performance of the production?

Profiling always comes with some costs. Go collects sampling-based profiling data and for the most applications
the real overhead is small enough (refer to "[Can I profile my production services](https://golang.org/doc/diagnostics.html#profiling)"
from Go's Diagnostics documentation).

To reduce the costs, users can adjust the frequency of collection rounds, e.g. collect 10 seconds of CPU profiles every 5 minutes.

[profefe-agent](https://godoc.org/github.com/profefe/profefe/agent) tries to reduce the overhead further by adding a small
jiggling in-between the profiles collection rounds. This distributes the total profiling overhead, making sure that not all instances
of application's cluster are being profiled at the same time.

### Can I use profefe with non-Go projects?

profefe collects [pprof-formatted](https://github.com/google/pprof/blob/master/README.md) profiling data. The format is used by Go profiler,
but thrid-party profilers for other programming languages support of the format too. For example, [`google/pprof-nodejs`](https://github.com/google/pprof-nodejs) for Node.js,
[`tikv/pprof-rs`](https://github.com/tikv/pprof-rs) for Rust, [`arnaud-lb/php-memory-profiler`](https://github.com/arnaud-lb/php-memory-profiler) for PHP, etc.

Integrating those is the subject of building a transport layer between the profiler and profefe.

## Further reading

While the topic of continuous profiling in the production is quite unrepresented in the public internet, some
research and commercial projects already exist:

- [Stackdriver profiler](https://cloud.google.com/profiler/)
- [Google-Wide Profiling: A Continuous Profiling Infrastructure for Data Centers](https://ai.google/research/pubs/pub36575) (paper)
- [StackImpact](https://stackimpact.com/docs/go-profiling/)
- [conprof](https://github.com/conprof/conprof)
- [Opsian - Continuous Profiling for JVM](https://opsian.com) (provides on-premises plan for enterprise customers)
- [Liveprof - Continuous Profiling for PHP](https://habr.com/ru/company/badoo/blog/436364/) (RUS)
- [FlameScope](https://github.com/Netflix/flamescope)

*profefe is still in its early state. Feedback and contribution are very welcome.*

## License

MIT

[hub.docker]: https://hub.docker.com/r/profefe/profefe
[3]: https://stackimpact.com/
[5]: https://github.com/GoogleCloudPlatform/golang-samples/tree/master/profiler/hotapp
[pprof]: https://github.com/google/pprof/
