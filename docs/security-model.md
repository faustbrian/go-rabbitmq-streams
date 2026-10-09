# Security model

Version: 2.0. Scope: root-v2 and RabbitMQ adapter-v2.

## Scope and trust boundaries

This model describes the security-hardening source on main, not a retrospective
qualification of previously published modules. Release and consumer verification
must establish which public version contains these controls.
The unchanged OpenTelemetry and existing v1 consumer cohorts are not covered
by a v2 release or automatically compatible with v2 nominal types.
RabbitMQ facade-v2 requires its own ordered publication after adapter-v2;
the existing published facade remains on its v1 dependency cohort.

Protected assets include credentials, message contents, delivery and publication
ownership, stored offsets, process availability, and observation confidentiality.
Connection configuration, broker frames, compressed chunks, AMQP values, topology,
and retained-range responses cross explicit trust boundaries. Application
credential providers, handlers, failure publishers and observers are trusted
collaborators, not sandboxed code.

## Controls

- Connection policy requires TLS unless the application explicitly selects the
  development plaintext policy. Broker authorization and topology provisioning
  remain deployment responsibilities.
- Root policy bounds message and metadata sizes, outstanding publications,
  handler concurrency, retry work and lifecycle waiting.
- The RabbitMQ adapter ships a private, attributed pinned client source closure;
  downstream builds do not depend on vendoring or replacement directives.
  [Source identity and maintenance procedure](../adapters/rabbitmq/THIRD_PARTY_RABBITMQ_STREAM.md)
  bind its upstream origin and local patches.
- Independent frame, message, decoded-chunk, record, protocol-item, AMQP-depth,
  container and aggregate-value budgets admit work before count-driven
  allocation or decompression. Unsupported annotation-key shapes are rejected
  before insertion into a Go map.
- Adapter operation cancellation reaches native dial, TLS, admission, RPC and socket
  waits. Shutdown signals owned resources before joining their workers;
  retired clients and reconnect opening remain part of that ownership.
- A possibly partial publication write remains ambiguous. A pre-write refusal
  does not claim publication, and confirmation ownership cannot be silently
  replaced by another pending publication using the same ID.
- Offset storage follows successful handler policy. Replay uses isolated
  cursors and checks retained-range and topology consistency; publishing-ID
  deduplication is not an end-to-end exactly-once guarantee.
- Native SDK logging is disabled and its meter provider is explicitly no-op.
  Application observations remain opt-in and must not contain secrets or payloads.
- Production source, including the private client closure, remains subject to
  dependency, static-security, secret and license scanning. Focused decoder and
  lifecycle regressions complement scanner results.
- Adapter coverage collects statement evidence for the public adapter and the
  attributed AMQP/stream implementation. The type-only message interface and
  empty logging compatibility functions remain built and scanned but have no
  statement denominator. Inherited-client statement counts do not substitute
  for the focused parser, cancellation, ownership, race and broker assertions.
  Root coverage retains its exact policy; no security gate is disabled.

## Residual responsibilities

| Risk | Owner and rationale | Mitigation | Review condition |
| --- | --- | --- | --- |
| Trusted application code can block despite cancellation; Go cannot terminate arbitrary callbacks. | Application integrator; callbacks execute in-process. | Honor contexts, return promptly and isolate untrusted code outside the process. A caller's shutdown timeout is not proof that a blocked collaborator terminated. | Revisit when collaborator or lifecycle ownership changes. |
| Finite default budgets can still exceed an application's memory or latency budget. | Application integrator; deployment capacities differ. | Configure lower limits and admission outside the library for untrusted workloads. | Revisit when capacity or workload exposure changes. |
| Broker identity, permissions, retention and durability are external authorities. | Broker operator; the client cannot establish broker-side policy or recover deleted history. | Use authenticated TLS, least privilege, explicit topology and retention monitoring; handle unavailable or changed replay ranges. | Revisit when broker policy or protocol support changes. |
| Local client patches can diverge from upstream fixes. | Library maintainers; a distributable source closure owns its maintenance burden. | Preserve attribution, review upstream deltas and reapply/test local patches before updating the pinned source. Use private reporting for suspected upstream vulnerabilities. | Review on upstream security releases, protocol changes or each client update. |

The model does not assert release readiness, production qualification or the
absence of unknown vulnerabilities.
