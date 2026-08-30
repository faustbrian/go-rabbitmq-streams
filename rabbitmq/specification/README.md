# RabbitMQ Streams specification conformance

The adapter owns a bounded policy around the RabbitMQ Streams protocol and the
pinned RabbitMQ-supported Go client. It does not reimplement every protocol
command, claim every RabbitMQ feature, or claim independent non-Go client
agreement. Exact sources, client revision, server images, and digests are
recorded in `sources.lock.json`; authority monitoring is recorded in
`monitoring.json`.

The [specification decision register](../docs/specification-decisions.md)
records each selected behavior, alternative, consequence, evidence binding,
and reconsideration condition.

| Decision | Authority | Executable evidence | Provider or differential boundary |
| --- | --- | --- | --- |
| RABBITMQ-STREAM-DEC-001 | RabbitMQ Stream protocol 4.3.5 | AMQP mapping unit and fuzz evidence | Live RabbitMQ 4.3.5 provider agreement; independent non-Go differential decoding not assessed |
| RABBITMQ-STREAM-DEC-002 | RabbitMQ Stream protocol 4.3.5 | confirmation admission, terminal-state, and fuzz evidence | Live confirmation and publishing-ID provider evidence; lost-response recovery has no provider operation |
| RABBITMQ-STREAM-DEC-003 | RabbitMQ Stream protocol 4.3.5 | stored-start, bounded store, and reconnect evidence | Live stored-offset restart agreement; external-effect atomicity not claimed |
| RABBITMQ-STREAM-DEC-004 | RabbitMQ Streams 4.3 guide | retained-range, cursor, and start mapping evidence | Live retention and replay provider agreement; exact timestamp recovery not claimed |
| RABBITMQ-STREAM-DEC-005 | RabbitMQ Streams 4.3 guide | topology, hash, routing, and replay evidence | Three-node provider agreement; global order and cross-client hash equivalence not claimed |
| RABBITMQ-STREAM-DEC-006 | RabbitMQ Stream protocol 4.3.5 | TLS translation, credential cancellation, and authentication retry evidence | Live TLS, mTLS, authentication, and authorization provider agreement |

Run the offline specification gate with the archived canonical tooling revision
pinned by CI. The online form verifies that each monitored authority still has
the reviewed digest; a changed digest requires review and never changes adapter
behavior automatically.
