# Performance evidence

These measurements are local capacity-validation points, not production
promises. Broker capacity, replication, retention, payload distribution,
network latency, handler work, and deployment topology must be measured in the
target environment.

## 2026-08-22 local broker run

| Input | Value |
| --- | --- |
| source revision | `3506b146619f442263e3803fafb0403d3ebbea52` plus the uncommitted RabbitStream change set |
| Go | `go1.26.6 darwin/arm64` |
| host | macOS 27.0, Apple M4 Max |
| Docker | client/server 29.7.2 |
| RabbitMQ | 4.3.5, pinned image digest from `integration/compose.yaml` |
| supported Go client | `rabbitmq-stream-go-client` v1.8.3 |
| confirmation policy | one broker-confirmed message per operation, 500-message upstream queue, 10-second confirmation timeout |
| benchmark sample | fixed 50 operations per result |

The wrapper and raw-client comparison includes public message validation and
mapping, client serialization, network transit, broker confirmation, and
delivery-result handling. Connection creation, stream declaration, warm-up,
and shutdown are outside the timer.

| Transport | Payload | Policy wrapper | Raw client | Wrapper allocations |
| --- | ---: | ---: | ---: | ---: |
| plaintext | 128 B | 10.00M messages/hour | 10.77M messages/hour | 4,296 B, 58 allocs/op |
| plaintext | 1 KiB | 11.12M messages/hour | 9.86M messages/hour | 7,096 B, 58 allocs/op |
| plaintext | 64 KiB | 4.56M messages/hour | 5.18M messages/hour | 208,706 B, 58 allocs/op |
| mTLS | 128 B | 13.10M messages/hour | 18.03M messages/hour | 4,320 B, 59 allocs/op |
| mTLS | 1 KiB | 12.91M messages/hour | 16.63M messages/hour | 7,143 B, 59 allocs/op |
| mTLS | 64 KiB | 5.56M messages/hour | 5.50M messages/hour | 209,700 B, 59 allocs/op |

The short local samples establish that the synchronous 128-byte and 1 KiB
workloads crossed the 1M, 5M, and 10M/hour validation points. The 64 KiB
workload crossed 1M/hour but not 10M/hour. The 10-message and 100-message
bounded confirmation windows measured 80.45M/hour and 236.79M/hour
respectively with 1 KiB payloads. These results do not establish sustained
broker capacity.

TLS results were faster in this short localhost sample, so this run does not
establish a stable TLS overhead percentage. The benchmark keeps TLS and
plaintext behavior equivalent and makes a longer statistically repeated run
possible; target-environment evidence remains required before sizing.

The retained-history benchmark consumed and durably stored offsets for a
1,000-message, 1 KiB backlog at 53.30M messages/hour. The application-restart
scenario caught up the final 501 records in 46.87 ms. A broker restart reached
a new confirmed publish in 3.287 s. Those timings are local recovery evidence,
not recovery objectives for a production cluster.

### Idle producer resources

One confirmed producer remained idle for a 30-second steady-state window after
warm-up. The figures are absolute process and broker snapshots, so they include
the Go test harness and the supported client's internal resources rather than
representing a per-producer delta.

| Resource | Observed value |
| --- | ---: |
| process CPU | 0.1322 CPU-seconds, 0.44% of one core over the window |
| live heap after GC | 473,544 bytes |
| goroutines | 13 |
| process file descriptors | 11 |
| broker Streams connections | 9 |

The timed idle loop itself allocated zero bytes and made zero allocations per
operation. These bounded local values are a baseline, not a production budget;
repeat the capture with the deployed endpoint, TLS, telemetry, producer count,
and broker topology before setting alerts or capacity limits.

## Reproduction

Start uniquely named standalone and TLS fixtures from `integration/`, set the
documented endpoint and certificate environment variables, and run:

```sh
GOTOOLCHAIN=go1.26.6 \
GOCACHE="$TASK_OWNED_GOCACHE" \
go test -tags=integration -run '^$' \
  -bench '^Benchmark(EquivalentConfirmedPublish|BoundedConfirmedWindow)$' \
  -benchtime=50x -count=1 -benchmem

GOTOOLCHAIN=go1.26.6 \
GOCACHE="$TASK_OWNED_GOCACHE" \
go test -tags=integration \
  -run '^(TestProducerReconnectsAfterBrokerRestart|TestConsumerCatchesUpBacklogAcrossApplicationRestart)$' \
  -bench '^BenchmarkBacklogCatchUp$' -benchtime=1000x -count=1 -benchmem

GOTOOLCHAIN=go1.26.6 \
GOCACHE="$TASK_OWNED_GOCACHE" \
go test -tags=integration -run '^$' \
  -bench '^BenchmarkIdleProducerResources$' \
  -benchtime=30s -count=1 -benchmem
```

The caller must remove the exact task-owned fixtures and cache after capture.
