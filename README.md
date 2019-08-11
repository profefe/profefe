# profefe - Collect profiling data for long-term analysis

[![Build Status](https://travis-ci.org/profefe/profefe.svg?branch=master)](https://travis-ci.org/profefe/profefe)
[![MIT licensed](https://img.shields.io/badge/license-MIT-blue.svg)](https://raw.githubusercontent.com/profefe/profefe/master/LICENSE)

*profefe* allows to continuously collect profiling data from a running Go service and provides an API for querying
the profiling samples base on metadata associated with the service.

---

**The project is still in the early prototyping stage. Things will change. Opinions and contributions are welcome.**

---

## Why do we need it?

Profiling a single instance of a running Go service [is very easy][1]: one adds `net/http/pprof` to the list of
imports and a "magical" `/debug/pprof/` route is registered to the services' default HTTP server.

Unfortunately, exposing a debug server for external requests might be prohibited by internal security policies.
At the same time, making the server available for a limited group of privileged developers, that can access it only
from a trusted network, can bring unexpected delays in cases when profiling data was needed for a quick investigation
of an incident.

Continues profiling can also help in a situation where an instance showed periodic outstanding behaviour but had
been restarted by an "external force" (i.e. *OOM killer or an On Call Ops in the middle of a weekend*), before
a developer could scrap the profiles.

Services like Google's [Stackdriver Profiler][2] or [StackImpact][3] provides a way for periodically profiling service
instances, but can't be used in a company, whose internal security policy prohibits exporting of any data
to outside of company's own infrastructure.

*profefe* tries solving the described use cases. It periodically scraps profiles from service's instances and stores
it in the collector, that can be deployed on premies.

Profiles from a running instance can be annotated by a set of labels, similar to how [Prometheus][4] allows
annotating metrics with labels.

## How does it work?

*profefe* consists of:

- Collector — a service that receives profiles from the agent, stores them in the persistent storage, and provides an API for querying profiles.
- Agent — an optional library that can be integrated into your project. Its goal is to scrap pprof data from the running instance periodically and send it to the collector.

*Note*: it's tempting to split collector into two separate parts (collector and querier) to inctease scalability. This
is the subject of future research.

## Quickstart

**TODO add quickstart**

Collector requires a storage to keep the profiling data. Currently, the only supported storage is [Badger](https://github.com/dgraph-io/badger).

To build and start the collector, run:

```
> make

> ./BUILD/profefe -log.level debug -badger.dir /tmp/profefe

2019-06-06T00:07:58.499+0200    info    profefe/main.go:86    server is running    {"addr": ":10100"}
```

The project includes a fork of [Stackdriver's example application][5], modified to use profefe's agent and send profiles
to the local collector.
To start the example, in a separate terminal window run:

```
> go run ./examples/hotapp/main.go
```

After a brief period, the application will start sending CPU profiles to the collector:

```
send profile: http://localhost:10100/api/0/profile?instance_id=87cdc549c84507f24944793b1ddbdc34&labels=version%3D1.0.0&service=hotapp-service&type=cpu
send profile: http://localhost:10100/api/0/profile?instance_id=87cdc549c84507f24944793b1ddbdc34&labels=version%3D1.0.0&service=hotapp-service&type=cpu
send profile: http://localhost:10100/api/0/profile?instance_id=87cdc549c84507f24944793b1ddbdc34&labels=version%3D1.0.0&service=hotapp-service&type=cpu
```

## Querying Profiles

Querying the profiling data is an HTTP call to collector's `/api/0/profile`:

```
> go tool pprof 'http://localhost:10100/api/0/profile?service=hotapp-service&type=cpu&from=2019-05-30T11:49:00&to=2019-05-30T12:49:00&labels=version=1.0.0'

Fetching profile over HTTP from http://localhost:10100/api/0/profile...
Saved profile in /Users/varankinv/pprof/pprof.samples.cpu.001.pb.gz
Type: cpu
Entering interactive mode (type "help" for commands, "o" for options)
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

## API

Save pprof data:

```
POST /api/0/profile?service=<service>&instance_id=<iid>&type=[cpu|heap]&labels=<key=value,key=value>
body pprof.pb.gz
```

Query pprof data:

```
GET /api/0/profile?service=<service>&type=[cpu|heap]&from=<created_from>&to=<created_to>&labels=<key=value,key=value>
```

## Feedback

The feedback and contribution are always welcome.

## Further reading

While the topic of continuous profiling in the production is a bit unrepresented in the public internet, some
research or commercial projects are already exist

- [Stackdriver profiler][3]
- [Google-Wide Profiling: A Continuous Profiling Infrastructure for Data Centers](https://ai.google/research/pubs/pub36575) (paper)
- [StackImpact](https://stackimpact.com/docs/go-profiling/)
- [conprof](https://github.com/conprof/conprof)
- [Opsian - Continuous Profiling for JVM](https://opsian.com) (provides on-premises plan for enterprise customers)
- [Liveprof - Continuous Profiling for PHP](https://habr.com/ru/company/badoo/blog/436364/) (RUS)
- [FlameScope](https://github.com/Netflix/flamescope)

## License

MIT

[1]: https://github.com/golang/go/wiki/Performance
[2]: https://cloud.google.com/profiler/
[3]: https://stackimpact.com/
[4]: https://prometheus.io/
[5]: https://github.com/GoogleCloudPlatform/golang-samples/tree/master/profiler/hotapp
[tw1]: https://twitter.com/tvii/status/1090633702252527617
[tw2]: https://twitter.com/tvii/status/1124323719923601408
[pprof]:https://github.com/google/pprof/
