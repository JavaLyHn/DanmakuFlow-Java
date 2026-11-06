# DanmakuFlow Java Migration Implementation Plan

> Spec: `docs/superpowers/specs/2026-11-06-java-migration.md`

## Global Constraints

Use Java 21. Preserve Go wire compatibility. Do not run Go tests or load tests. Do not start infrastructure services or modify existing databases. Run at most one bounded Java test JVM at a time. Preserve upstream MIT attribution. Each coherent change gets its own verified commit and upload; never squash the whole migration into one commit.

## Task 1: Preserve source and progress

- [x] Record paused Go baseline and migration scope.
- [x] Import the unchanged upstream snapshot under `legacy/go`, with upstream commit and license attribution.
- [x] Verify source hashes and review staged changes. Commit documentation and source import separately.

## Task 2: Maven foundation

- [x] Create parent and protocol/gateway/persistence/tools child POMs. Configure Java 21, UTF-8, bounded serial tests and unified stable dependencies.
- [x] Resolve dependencies, run Maven validate/package without starting any services.
- [x] Document build command and commit only foundation changes.

## Task 3: Protocol compatibility

- [x] Write failing tests for uint64 boundaries, 101/102/103 wire shapes, null likes, malformed/oversized input, Unicode limits and spoofed identity.
- [x] Implement pure Java protocol codec/models using explicit server identity/time.
- [x] Run focused tests and commit protocol layer.

Interfaces: connection identity (uid/name/room); decoded client command; immutable server danmaku with event_id and Instant; codec methods for client commands, WS events and stored Kafka events. Gateway and persistence share this module.

## Task 4: Netty gateway core

- [x] Test handshake validation, room isolation, connection close idempotence, broadcast, slow-client and queue limits using EmbeddedChannel/bounded loopback functional checks.
- [x] Implement bounded Netty server, room membership and async message publication interfaces. No infrastructure calls in unit tests.
- [x] Verify tests and commit core.

Interfaces: async event publisher, async room bus/statistics provider, room service, lifecycle-managed Netty server. External adapters follow these interfaces, not test-only fake production services.

## Task 5: Redis and Kafka gateway adapters

- [x] Test publication order, failure paths and room-stat semantics before implementing adapters.
- [x] Add Lettuce Pub/Sub plus room statistics, Kafka acknowledgement-gated publication and Spring Boot configuration/lifecycle.
- [x] Verify bounded resources, subscription cleanup and graceful shutdown; commit adapters in coherent stages.

## Task 6: Durable persistence

- [x] Test duplicate event handling, batch failure and commit ordering before implementation.
- [x] Add non-destructive Flyway schema, JDBC transaction writer and Kafka batch consumer; never acknowledge before transaction commit.
- [x] Configure explicit MySQL credentials, finite batch and retry behavior. Validate SQL/config statically and test isolated transaction boundary; do not start MySQL or modify the user's databases.
- [x] Commit schema/writer and consumer separately where independently meaningful.

## Task 7: Client and operating instructions

- [x] Add small Java WebSocket interactive client with no automatic load generation.
- [x] Document separate gateway/persistence commands, environment variables, protocol, reliability limitations, future JMeter strategy and source provenance.
- [x] Run bounded Java test suite and package artifacts; independent review and corrections before final upload.
- [x] Update progress with commit/test evidence and explicitly separate verified behavior from pending real-infrastructure/performance validation.

## Review Focus

Check malformed/oversized/unsigned input; nonblocking event-loop work; room join/leave races and subscriptions; duplicate close; write-buffer/backpressure accounting; Kafka callback failures; database transaction/offset ordering; partial startup cleanup; shutdown thread leaks; secret handling; truthful verification/performance claims.
