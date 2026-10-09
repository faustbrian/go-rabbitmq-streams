# Private RabbitMQ Stream client source

`internal/rabbitmqstream/{stream,amqp,message,logs}` contains the production
source closure of RabbitMQ's Go Stream client, not a new protocol implementation.
It is private to this adapter module so downstream Go module builds receive the
patched implementation without relying on dependency vendoring or `replace`.

- Source: https://github.com/rabbitmq/rabbitmq-stream-go-client
- Version: `v1.8.3`
- Immutable Git revision: `e56918bc2858425622a7bd8f569d46daecba289a`
- Go proxy module archive SHA-256:
  `8ead0c2aa818b528885cd223be62fab4b378ef93436ecbecde81640915c16a46`
- License: MIT, preserved in `internal/rabbitmqstream/LICENSE` (terminal newline
  added; license text unchanged).
- Original production-file checksums: `internal/rabbitmqstream/UPSTREAM_SHA256SUMS`.

## Import and update procedure

Obtain the pinned module archive or a detached checkout of the exact revision in
task-owned disposable storage. Verify its immutable identity and archive checksum
before use. Copy only non-test Go files from upstream `pkg/stream`, `pkg/amqp`,
`pkg/message`, and `pkg/logs`, preserving their relative names under
`internal/rabbitmqstream`; preserve upstream `LICENSE` text. Rewrite internal Go
imports beginning `github.com/rabbitmq/rabbitmq-stream-go-client/pkg/` to
`github.com/faustbrian/go-rabbitmq-streams/adapters/rabbitmq/v2/internal/rabbitmqstream/`.
Do not copy upstream module manifests, examples, tests, workflows, or generated
artifacts. The current mechanical import used the exact pinned Go module cache
as a read-only source and applied the resulting additions with `apply_patch`.

Before updating, independently review the upstream delta and dependency closure,
regenerate original-file SHA-256 sums, reapply each local behavioral patch, and
run the focused transport regressions and affected adapter compatibility checks.
Update this identity record and retain the upstream license. Remove disposable
source and caches once their evidence is recorded. Do not replace reviewed local
patches with an unverified upstream update.

## Local patch inventory

- `amqp/encode.go` and `amqp/types.go`: admit array/list/map/composite counts
  and encoded sizes before narrowing or output mutation, propagating encoder
  errors through array callers. Signed-small scalars and signed arrays preserve
  negative values and reject small-width encoding outside its signed range;
  pre-epoch timestamps retain their signed wire representation. Exact-site
  scanner dispositions identify intentional signed wire bits, not unchecked
  count or size conversions.

- `stream/socket.go`: cancellable write admission, exclusive connection-deadline
  ownership, context-bounded actual socket writes, and raw close independent of
  write ownership. An admitted write failure closes the connection because a
  partial protocol frame cannot safely be retried on that stream. Cancellation
  callbacks finish before another writer can inherit deadline ownership.
- `stream/client.go`: synchronize initial connection assignment with socket
  state ownership; native operation contexts cover DNS/TCP/TLS opening, RPC
  response and response-data waits, metadata, publisher and subscriber setup.
  Opening cancellation is stopped and joined before successful resource return.
- `stream/producer.go`: serialize direct buffered publication using the same
  context-bounded socket owner, rather than holding the socket state mutex
  across network I/O.
- `stream/socket_context_test.go`: bounded local pipe regressions for shutdown,
  actual write cancellation, deadlines, admission isolation, and raw read abort.
- `stream/environment.go` and `stream/operation_context.go`: context-aware
  environment/pool admission and retry, fresh terminal socket generation per
  connection attempt, separate environment lifetime, and raw terminal `Abort`.
- `stream/coordinator.go`: retiring a response removes lookup ownership without
  closing channels a reader may already own. All RPC/data waits select caller
  cancellation and terminal socket state; notification streams are unchanged.
- `stream/blocking_queue.go`: cancellable queue admission and stop/join ownership
  before channel destruction. `stream/consumer.go` bounds offset admission and
  native writes, advancing the local checkpoint only after a successful write.
- `stream/operation_context_test.go`: bounded pipe/TCP fixtures cover cancelled
  dial, TLS opening, RPC/data waits, queue admission, failed offsets, fresh
  connection retries, and detachment of successful opening from caller cleanup.
- `stream/decoder_policy.go`, `stream/server_frame.go`, and `stream/buffer_reader.go`:
  finite independent frame/message/chunk/record/protocol-item policies, frame-local
  structural admission before count-driven handlers, bounded response mailbox
  delivery, socket-terminal chunk dispatch, and removal of the unchecked message
  allocation helper. Open properties consume unknown values without losing the
  next key. Negative RPC and empty Open/authentication response shapes are retained.
- `stream/chunk_decoder.go` and `stream/aggregation.go`: checked entry/record
  accounting, independent encoded-message and aggregate decoded-byte budgets,
  frame-local compressed slices, actual output limits for all four codecs, finite
  zstd window/memory configuration, and terminal rejection of malformed chunks.
  Osiris offset/user-data delivery can omit bloom/trailer bytes retained in its
  on-disk header; both stripped delivery and full bounded chunk data are admitted.
- `amqp/decoder_policy.go`, `amqp/buffer.go`, and `amqp/types.go`: structural wire
  admission for declared container sizes, counts, total values, depth and variable
  bytes before the existing semantic codec allocates, subtraction-safe buffer
  bounds, and correct full-width message descriptor consumption. Existing codec
  feature support remains authoritative; this is not a new AMQP implementation.
- `stream/decoder_limits_test.go` and `amqp/decoder_limits_test.go`: tiny frame,
  chunk, codec, AMQP, duplicate-response and terminal-dispatch regressions.
- `stream/lifecycle.go`, `stream/lifecycle_test.go`, and native client/resource
  call sites: stop-before-join task ownership for the adapter-reachable reader,
  heartbeat, publication queue/timeout workers, consumer dispatch and native
  connect/send operations. Confirmation senders release their mutex before
  channel backpressure and are released before their channel closes. Terminal
  close mailboxes never block teardown; RPC reader-owned mailboxes are retired
  rather than destroyed. Environment shutdown signals all socket owners before
  resource cleanup or joining, including task registrations from retired clients.
- `amqp.DecodeMessageWithBudget` and `stream.DecoderLimits.MaxChunkValues`:
  charge actual structurally admitted AMQP values against the independent chunk
  budget before allocating each semantic message, including compact zero-width
  array elements. Encoded bytes and record count remain separate limits.
- Remove unreachable private legacy wrappers/helpers after native context
  migration, including the unused `iEntity` interface and test-purpose
  `sendToSuperStream` helper. Context entrypoints and reachable compatibility
  methods remain; no blanket unused-code analyzer suppression is applied.
- `amqp/types.go` rejects unsupported annotation key shapes before Go map
  insertion, returning an error rather than panicking for binary/list/map/array
  keys. Protocol symbol/ulong keys and existing string/signed-long compatibility
  remain accepted; message, delivery and footer annotations share this boundary.
- Generic `amqp/types.go` maps check the actual key value's comparability,
  including interface fields inside described structs, before insertion.
  Non-comparable described binary/list keys return an error without panic;
  comparable scalar, null and described keys retain their decoding behavior.
- `stream/producer_unconfirmed.go` and producer admission callers share one
  atomic bounded confirmation-table owner. Whole batches larger than native
  capacity fail before any write; saturated admission directly selects caller
  cancellation, socket termination and producer stop without condition waiters
  or proxy goroutines. A still-pending publishing ID cannot replace its original
  confirmation record, but can be reused after genuine completion. Pre-write
  failures release only their own admission token; attempted/partial writes
  retain confirmation ownership and `WriteAdmittedError` ambiguity. Queued and
  direct publication use the same capacity owner, and synchronous error
  notification backpressure observes the operation context.
- `stream/buffer_writer.go` and native string callers reject strings longer
  than 65,535 encoded bytes before uint16 narrowing. Plain string/array/map
  encoders return `ErrProtocolStringTooLong`, preflight complete collections
  before mutating their output, and propagate every encoding failure. Native
  commands validate strings before length-based buffer allocation, retire any
  response/resource ownership on failure, and never transmit malformed frames.
  Publisher references use the same checked encoder; publication filter values
  are admitted before confirmation ownership or socket writes. MaxUint16-length
  strings retain their existing wire encoding; numeric/count/correlation writer
  policy is separate from this string-specific patch.
- Native numeric writers now admit nonnegative counts and byte lengths before
  uint32 narrowing, frame-buffer allocation or output mutation, propagating
  errors through command, header, collection and aggregate encoder callers.
  The filter null sentinel is explicitly unsigned; signed short/long wire-bit
  encodings retain their exact representation. Sub-entry counts have an
  explicit 65,535 guard. Requested TUNE frame sizes must be 8..MaxUint32 and
  heartbeat seconds 3..MaxUint32; zero/unlimited or overflowing client requests
  are refused, while broker zero retains the finite client request. Defaults
  remain 1 MiB / 60 seconds. This is an intentional native configuration
  compatibility tightening: callers relying on unlimited frame requests must
  supply a representable finite size.
- Correlation IDs use the uint32 wire namespace and allocate a free ID under
  the response-map mutex, skipping live owners on wrap and returning an error
  if exhausted. Operation cleanup releases only its original response owner.
  Empty private hash-routing partitions return an error; valid hash routing
  keeps the same deterministic modulo mapping without count narrowing.
  Exact-site scanner dispositions distinguish admitted lengths and intentional
  same-width wire bits from scheduling/broker-selection randomness; no blanket
  rule suppression or numeric clamp is applied.

`Producer.SendContext` uses the existing synchronous one-message batch write
path; broker confirmation remains asynchronous. Legacy `Send` retains its
bounded queue path. `WriteAdmittedError` wraps native write failures after an
actual Write attempt, preserves `errors.Is` cancellation classification, and
requires an ambiguous publication outcome rather than a not-sent outcome.

Legacy native `Close`/`Abort` are nonjoining terminal stop requests, safe for a
reader, heartbeat or message callback to invoke. `Environment.CloseContext(ctx)`
adds an external-owner join with a finite default when ctx has no deadline.
`Environment.WaitContext(ctx)` faithfully preserves the caller's wait budget,
including Background, and can resume joining after a previous timeout; timeout
never counts as reclaimed resources. External owners must release application
callback backpressure before joining and must not join their own callback task.
The SDK cannot forcibly terminate an uncooperative application callback. Private
SuperStream convenience/helper goroutines are not used by this adapter's manual
partition integration and are outside this reachable-task ownership slice.

These local patches are not a claim of complete adapter remediation: adapter
integration and final complete-diff review remain separate boundaries. An
aborted socket is terminal; retries
allocate fresh Clients rather than resetting an active socket's close/gate state.

Decoder admission follows the pinned RabbitMQ Server `v4.3.5`
[`PROTOCOL.adoc`](https://github.com/rabbitmq/rabbitmq-server/blob/v4.3.5/deps/rabbitmq_stream/docs/PROTOCOL.adoc)
and response serializer in
[`rabbit_stream_core.erl`](https://github.com/rabbitmq/rabbitmq-server/blob/v4.3.5/deps/rabbitmq_stream_common/src/rabbit_stream_core.erl),
with chunk layout/sendfile behavior cross-checked against Osiris revision
`12a430b11be2c2be3f26ce4f2d7268954c7ec02b`. This SDK only negotiates Publish v2;
incoming Deliver v2 is not negotiated or reinterpreted as v1. Defaults are 16 MiB
whole frame (including the size word), 2 MiB encoded AMQP message, 16 MiB actual
aggregate decoded chunk, 1,048,576 actual AMQP values per chunk, 65,536 records,
4,096 protocol collection items, AMQP
4,096 values per container / 65,536 total / depth 32, and 2 MiB per variable value.
The validated per-environment policy can lower or explicitly change these finite
limits; adapter payload/metadata limits must be converted to encoded-wire limits
with checked overhead, not reused as aggregate limits. Codec working buffers are
also finite: Snappy uses its fixed framed block budget, LZ4 at most two 4 MiB block
buffers, and zstd a configured window/memory budget (at least 1 MiB).
