# Operations and capacity runbook

This guide covers application use of `rabbitstream`. RabbitMQ topology and
cluster operation remain operator responsibilities.

## Provisioning checklist

- pin the RabbitMQ version and image digest;
- enable the Streams plugin and expose only the intended TLS listener;
- create streams or Super Streams before application startup;
- configure partition count, replication, retention age/bytes, and segment
  size from measured workload requirements;
- issue verified server certificates and client certificates when using mTLS;
- create distinct producer, consumer, inspection, and administrative roles;
- grant virtual-host and stream permissions only for owned names;
- verify DNS and every configured endpoint from the workload network;
- record broker, client, Go, OS, and architecture versions with test evidence.

Applications must not create or mutate production topology through health or
startup paths.

## RabbitMQ permissions

Use RabbitMQ's current Streams authorization documentation for exact regular
expressions. At minimum, evaluate permissions separately for:

- publishing to each stream or backing stream;
- subscribing to each stream and storing a named consumer offset;
- querying stream and Super Stream metadata used for routing and inspection;
- reading the Super Stream exchange/binding topology required by the client;
- health/readiness connectivity.

Do not grant topology creation or deletion to an application that only
publishes, consumes, replays, or inspects. Test a restricted identity against
the real cluster and treat authorization failures as permanent until policy or
credentials change.

## Readiness and liveness

Process liveness must not depend on RabbitMQ. Dependency readiness may use
`rabbitmq.Inspector.Health` with a bounded context. Diagnostic inspection may
query topology and offsets, but must not be on a high-frequency health path.

A temporary broker outage should make the dependency unavailable and let
bounded reconnection or orchestration policy act. Restarting every healthy
process during a broker outage creates a reconnection storm.

## Graceful shutdown

Use this order:

1. stop accepting new publish requests;
2. cancel consumer and replay contexts;
3. wait for `Run` or `RunBatch` to return within the service drain deadline;
4. call every consumer's `Close` with a bounded context;
5. wait for accepted asynchronous publish outcomes;
6. close producers with a bounded context;
7. flush and shut down caller-owned telemetry providers;
8. terminate the process only after owned resources have closed or the outer
   deployment deadline forces termination.

Set Kubernetes `terminationGracePeriodSeconds` longer than handler,
confirmation, and close budgets combined. Do not begin new handler side effects
after the drain deadline.

## Rolling deployment

- keep consumer names stable only when the new version is intended to continue
  the same durable progress;
- use a new consumer name for shadowing, rebuilding, or an independently owned
  projection;
- deploy compatible payload/schema readers before writers;
- preserve one owner per deduplicating producer name and its publishing-ID
  sequence;
- keep Super Stream topology unchanged during an application rollout;
- watch reconnects, ambiguous publishes, handler failures, stored offsets, lag,
  memory, goroutines, connections, and confirmation latency;
- pause or roll back on sustained ambiguity, authorization errors, growing lag,
  or ownership conflicts.

## Credential and certificate rotation

Implement `CredentialProvider` to return a fresh owned snapshot for every
connection attempt. Update the secret source, verify the new credential, then
revoke the old one only after all instances have reconnected. For certificates,
roll trust roots before leaf certificates and retain overlap long enough for
every instance and broker endpoint.

Never put passwords, certificates, private keys, payloads, routing keys, or raw
broker error strings in logs, metrics, traces, fixtures, or incident snippets.

## Capacity planning

The targets below are validation points, not promises:

| Rate | Average events/second |
| --- | ---: |
| 1 million/hour | 278 |
| 5 million/hour | 1,389 |
| 10 million/hour | 2,778 |

Test sustained and burst traffic with the real payload-size distribution,
confirmation policy, TLS, replication, retention, handler, and cluster topology.
Measure broker capacity separately from application handler capacity.

For each target record:

- messages/s and bytes/s accepted and confirmed;
- confirmation p50, p95, p99, and timeout/ambiguous rates;
- one-stream and Super Stream partition throughput;
- keyed routing distribution and hottest partition;
- handler throughput, duration, errors, and offset-store latency;
- backlog catch-up rate while live traffic continues;
- producer and consumer memory, CPU, allocations, goroutines, connections, and
  file descriptors;
- broker disk throughput, free space, memory, network, replica health, and
  retention behavior;
- raw supported-client baseline using equivalent durability and payloads;
- TLS overhead and recovery after broker restart, leader loss, and network
  interruption.

Size `MaxOutstanding`, `MaxBufferedMessages`, batch limits, and concurrency from
measured latency and memory. Do not increase them merely to hide slow
confirmations or handlers. Preserve confirmations, replication, bounds,
security, and ordering during comparisons.

## Alerts

Alert on sustained, actionable conditions rather than single reconnects:

- dependency unavailable beyond the reconnect budget;
- authentication or authorization failures;
- ambiguous or rejected publishes;
- confirmation latency near the configured timeout;
- unconfirmed messages near `MaxOutstanding`;
- handler error rate or duration near `HandlerTimeout`;
- stored offset not advancing while messages arrive;
- lag growing beyond the recovery objective;
- repeated retention gaps;
- partition imbalance or unavailable replicas;
- broker disk/memory alarms and connection growth;
- shutdown duration near the deployment deadline.

Do not label stream name, routing key, message ID, customer identifier, offset,
payload, or arbitrary metadata on metrics.

## Failure recovery

### Ambiguous publish

1. preserve the message ID, producer identity, publishing ID, and safe timing;
2. do not classify the result as a non-send;
3. reconcile against an application-owned idempotency record when available;
4. retry only under the documented duplicate policy;
5. investigate connection loss and confirmation latency.

### Consumer handler failure

1. identify the partition and safe offset metadata without logging payloads;
2. keep the offset unadvanced;
3. correct or classify the failure;
4. resume bounded retry, publish to a retry/dead-letter stream, or restart from
   the stored offset according to policy;
5. verify lag recovery and duplicate handling.

### Retention gap

1. stop replay; do not silently clamp to the first retained record;
2. capture the requested and retained ranges;
3. restore from an application-owned archive or rebuild source if available;
4. otherwise obtain an explicit business decision accepting the missing range;
5. correct retention capacity before retrying.

### Broker or leader failure

1. confirm the affected endpoint, stream, and replica state through operator
   tooling;
2. allow bounded client reconnection; avoid mass application restarts;
3. verify confirmations, consumer progress, duplicate rate, and lag after
   recovery;
4. preserve ambiguous publish records for reconciliation;
5. restore replica health before declaring recovery complete.

## Troubleshooting

| Symptom | Check | Action |
| --- | --- | --- |
| connect timeout | DNS, listener, TLS name, endpoint order, network policy | correct connectivity; keep retries finite |
| authentication/authorization | rotated credential, vhost, stream regex, metadata permissions | fix identity or policy; do not retry indefinitely |
| frequent ambiguous publishes | confirmation latency, connection churn, broker alarms | reduce load or repair cluster; reconcile retries |
| consumer redelivery | handler failure, crash before offset store, store interval | make handler idempotent; tune only with measured tradeoff |
| lag grows | handler throughput, hot partition, offset-store failures | increase safe partition parallelism or fix handler bottleneck |
| replay range failure | retained first/last offsets, requested checkpoint, topology | restore history or revise the explicit range |
| high memory | payload distribution, outstanding confirms, buffers, batch size | lower bounds and remove handler retention |
| shutdown timeout | handler cooperation, confirmation wait, deployment deadline | propagate cancellation and align finite budgets |

## Incident evidence

Record timestamps, safe categories, endpoint identity, stream/partition names
only when classification permits them, retained/stored offsets, version pins,
cluster state, and bounded metrics. Never record credentials or event bodies.
Distinguish local evidence, broker/operator evidence, deployment state, and
production verification in incident updates.
