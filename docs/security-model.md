# Root security model

Version: 1.0. Scope: root-v2 candidate policy contracts on main. Publication,
hosted qualification and public-consumer verification are separate boundaries.

## Assets and boundaries

The root owns bounded messages, publication admission, delivery and replay
policy, offset decisions, error categories, and producer/consumer lifecycle.
Applications own credentials, topology, handlers and transport implementations.
The root does not implement RabbitMQ's wire protocol or validate broker framing
before a transport allocates or decodes it.

Treat delivered/replayed messages, metadata, partition topology and offsets as
untrusted at root admission. A transport or replay source is also a trusted
execution collaborator: it must honor its contract and return from operations.

## Root controls

- Validate message target, backing partition identity, explicit offset, payload
  and metadata limits before routing, handlers, failure publication or offset
  advancement. Reject unsupported delivery shapes rather than normalizing them
  into a different accepted event.
- Charge batch byte limits across payload and metadata; preserve partition
  order and advance offsets only after the corresponding successful handling.
- Bound publication admission and retain explicit confirmation ambiguity;
  broker deduplication is not end-to-end exactly-once application execution.
- Keep one terminal cleanup owner. Each shutdown caller bounds its own wait;
  a timed-out wait does not finalize cleanup or prove resources reclaimed.
  A later caller can join and observe the actual terminal result.
- Reject invalid replay topology before retaining it. Replay does not silently
  substitute a different retained history or offset range.

## Residual responsibilities

| Risk | Owner and mitigation | Review condition |
| --- | --- | --- |
| A trusted callback or contextless supplier operation never returns | Application/transport owner must release callback backpressure and implement cooperative bounded operations. Caller timeouts do not forcibly terminate Go goroutines. | On new callback/transport integration or lifecycle changes. |
| Broker wire decoding allocates before root admission | Protocol adapter owner must enforce independent frame, count, recursion, decompression and allocation budgets before decoding. Root publication alone is not protocol remediation. | On adapter release or protocol/dependency change. |
| External durability, topology and side-effect duplication | Operators/applications own broker durability and authorization, topology provisioning, idempotent side effects and safe replay policy. | On deployment, permission, topology or recovery changes. |
| Mixing root-v1 and root-v2 nominal types | Consumer owner must migrate a coherent dependency cohort; v1 adapters are not automatically compatible with v2 configurations or observations. | Before consumer migration or module release. |

These responsibilities are explicit limitations, not a claim that checks or
releases have passed. The root's delivery/admission and joinable-shutdown tests
are the executable policy evidence; adapter/provider and release checks must
qualify their own boundaries independently.
