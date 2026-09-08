# qits-eventstream-javalib — working notes

Read `README.md` first: it defines the public surface, the wire contract and the config surface.
This file is the working conventions on top of it, and the rules that bite. Every one of them was
bought by something going wrong, and the measurement is kept beside the rule because a rule without
its reason gets "simplified" by the next person.

## The rule that shapes everything

**A clone of this repo alone builds and tests green.** `./mvnw verify` — no monorepo, no docker, no
credentials, no prior `mvn install` anywhere, no submodule to initialise. Anything that would break
that is not a tradeoff to weigh, it is the thing this repo exists to make possible.

**One address is the whole exception**: `qits-db-core` comes from the platform Maven repository
(`<repositories>` in the pom), because the datasource defaults this jar ships name its
`PatientPgDriver` and a default naming a class every consumer must have is only a default if the jar
brings the class along. Runtime scope, never imported by a source file here — which is also how the
extraction rule below stays true. A clone builds green with that repository reachable, or offline
once the jar is in `~/.m2`; nothing else may follow it in.

That is why the pom duplicates versions instead of inheriting them, and why the suite stands up its
own stub qits-events (`StubEventsServer`, a Vert.x server answering the PUT, the list route and the
websocket upgrade) rather than needing a real one. There are no integration tests and no `skipITs` property: the
whole suite is surefire, and there is nothing here that only real infrastructure could exercise.

**The store being PostgreSQL does not change that answer.** `testdb/EmbeddedPg` starts **zonky's**
postgres — real binaries resolved as ordinary Maven artifacts, spawned as a child process — and
`testdb/EmbeddedPgConfigSource` hands its url, username and password to every `@QuarkusTest` at an
ordinal above `application.properties`, because the port is chosen at run time and cannot be written
into a file. The instance is tracked in a **system property** as well as a static field: a Quarkus
run loads config sources in more than one classloader, and the property is the one thing those
copies share. Testcontainers is not on this classpath and must not arrive; `quarkus.devservices.enabled=false`
says the same thing about the other way a container gets started.

**This is a library, so there is one maven module at the root and no `service/` split.** The repo
produces one jar. The pom is the parent and the module at once.

## Where things live

`eu.wohlben.qits.eventstream.*`:

- the root package — `QitsEvent`, `QitsEventBus`, `QitsEventListener`, `QitsRawEventListener`,
  `QitsDurableEventListener`, `CausationScope`, `CausationHeader`, `CausedRow`, `CausationStamp`,
  `Uncaused`. The public surface, and the only things a consumer should name.
  `CausationClientFilter` and `CausationServerFilter` sit beside them because they share
  `CausationScope`'s package-private `swap`; a consumer never names either.
- `control/` — `CanonicalJson`, `EventsPublisher`, `EventsQuery`, `Outbox`, `OutboxSweeper`,
  `RetrySchedule`, `EventDispatcher`, `EventStreamSubscriber`, `DurableFunnel`, `CatchupSweeper`,
  `SweepCensus`, `CatchupHealthCheck`, `EventstreamClock`, and the three wire records
  `EventEnvelope`, `EventFrame` and `EventPage`.
  `EventFrame` is public because a listener receives it; `EventPage` is not, because nobody outside
  the catch-up loop holds one. `SweepCensus` is public because a consumer may want the same facts
  its own readiness check reads; `CatchupHealthCheck` is public only because a `@Readiness` bean has
  to be.
- `entity/`, `persistence/` — the outbox row and the consumer watermark with their Panache
  repositories, in this jar's own named persistence unit, plus `ConsumedEvents`, which is native SQL
  over a table with no entity (its javadoc says why).
- `testdb/` (test sources only) — `EmbeddedPg` and `EmbeddedPgConfigSource`, the embedded postgres
  the suite runs the outbox against. It is under this library's package like everything else here,
  because the extraction rule admits no other.

**THE EXTRACTION RULE, which outlived the extraction.** No `eu.wohlben.qits.*` package other than
this one may appear in these sources, main or test. It was written inside qits-ci to keep lifting
this module out a `git mv` plus a pom; that is done, but the direction it protects is permanent — a
"temporary" dependency pointing back at a consumer is inherited by every other consumer of this jar,
and the split is undone. `ExtractionRuleTest` greps the sources, so it fails a build rather than a
review. Its javadoc argues the widening.

## The seven things that are easy to get wrong

- **The canonical form is a wire contract, not a formatting preference.** qits-events stores the
  `payload` string verbatim and compares it byte-for-byte to tell an idempotent replay (200) from a
  reused UUID (400), so two serializations of one event that differ by a space are a contradiction
  to the other side. `CanonicalJson` therefore builds its **own** `ObjectMapper` rather than
  injecting the CDI one — a consuming application's `ObjectMapperCustomizer`s must not be able to
  reach it — and sets every knob that could vary explicitly. Its class javadoc names each and why.
  Do not "fix" a serialization problem by injecting the CDI mapper.
- **`eventId` is fixed at construction and never regenerated.** It is the `{id}` of the PUT, which
  is the only reason a retry is safe: a request whose response was lost replays as a 200 instead of
  writing the event twice. An event class may hold it as an ordinary record component; the library
  keeps everything `QitsEvent` declares out of the payload, so identity travels in the envelope.
- **The publisher's `HttpClient` is pinned to HTTP/1.1.** The JDK default is HTTP/2 with an `h2c`
  upgrade, and an upgrade carrying a request body **delivers that body twice** — measured against
  the test stub, once through the server's upgrade handler and again as an HTTP/2 data frame ninety
  milliseconds later. Idempotency made it harmless and therefore invisible; it was a doubled request
  on every publish. Do not drop the `version(...)` line. The client is an **instance** field, not a
  static one: a static `HttpClient` is created at image build time and native-image refuses the heap
  it lands in.
- **The outbox is failure-path-only, and empty in a healthy process.** A publish that lands writes
  nothing; a row that is delivered on retry is deleted. So the row count is a health signal rather
  than a log, and the log is qits-events. The known hole — a crash between the inline attempt
  failing and the row committing — is named in `OutboxEvent`'s javadoc and deliberately left open.

  **The retry budget bounds REFUSALS, and nothing bounds an unreachable bus.** `EventsPublisher`
  answers with four outcomes, not three: delivered, rejected (the 400), refused (a response arrived
  and said no) and unreachable (no response at all). Only refusals draw on
  `qits.eventstream.max-attempts`; an unreachable attempt is rescheduled indefinitely and settles at
  the schedule's five-minute cap. Hence the two counters on the row — `attempts` spaces the backoff,
  `refusals` spends the budget — and hence the WARN in `OutboxSweeper`, which is the only notice an
  outage now gets and is emitted **once per sweep, not once per event**.

  Measured, 2026-08-10: a seed qits-ci dialled an alias that did not resolve, five
  `ConnectException`s later every row was `FAILED`, and those events never reached the log. There is
  no consumer-side bookkeeping that recovers a publish that was abandoned, which is why this is a
  rule and not a tuning parameter. Do not re-merge the two classes, and do not give `Delivery` a
  `retryable()` again: one predicate over two differently-retried outcomes is exactly the shape in
  which the distinction gets collapsed back.
- **Causation is stamped by the bus, in the envelope, and it never touches an event class.**
  `EventEnvelope` carries a nullable `parentId` and `QitsEventBus.publish` is the only place it is
  resolved. The precedence is the whole rule: **an explicit non-null argument wins; a null or absent
  one falls back to `CausationScope.current()`; outside any scope the event is a root.** So
  `publish(e)` *is* `publish(e, null)` — one implementation, one call shape — and the deliberate
  detach is spelled `CausationScope.with(null, …)`, which is a statement about a region rather than
  about one call. That asymmetry is settled, not an oversight.

  `QitsEvent` gained **no** fifth method and no event class gained a component, which is the
  decision rather than an omission. A record is immutable and its parent is known later than it is;
  a default method reading a thread-local would answer differently on the sweeper's thread an hour
  later, which is exactly what `eventId`'s stability argument forbids; and a fifth accessor would
  need a fifth `@JsonIgnore` in `CanonicalJson`'s mix-in — the one place this code has already been
  bitten silently. `CanonicalJson` and its mix-in are therefore **unchanged**, and a payload is
  byte-identical whether the event was published under a parent or not. That last property has a
  test, because it is the same lesson the mix-in taught.

  **The outbox's `parent_id` column is the load-bearing line of the whole feature.** qits-events
  compares `name` + `occurredAt` + `payload` + `parentId` + `environment` — `description` stays
  outside it — so a
  sweeper that rebuilt the envelope without the parent would send a *different* request than the
  attempt it is retrying: a 400 against its own landed first attempt, or a caused event quietly
  republished as a chain root. The parent is fixed when the envelope is built and stored with it,
  exactly as the payload is, and the sweep re-reads nothing ambient. `CausationStampingTest` sweeps
  inside a *different* scope to prove it.

  **`CausationScope` is public API for one reason**, and it is the reason to keep it public: this
  library tells listeners that "anything slow belongs on the listener's own executor", and a
  hand-off to an executor is precisely what drops the ambient value. A plain `ThreadLocal` does not
  follow work, deliberately — inheritance copies at thread *creation*, which pooled executors do
  long before any consumption. Advice a library gives has to come with the bridge that makes it
  safe; both forms are in `QitsEventListener`'s javadoc, which is where a listener author looks.

  **Causation crosses a REST hop as the `X-Qits-Causation-Id` header, and the filters are the whole
  registration.** `CausationClientFilter` writes `CausationScope.current()` into every REST-client
  request (a header the caller set itself wins, mirroring `publish(event, parentEventId)`);
  `CausationServerFilter` swaps the header's id into the scope for the resource method and restores
  the previous value on the response — establish-even-when-absent is what keeps a pooled worker
  from lending one request's cause to the next. Both are `@Provider`-discovered, so a consumer
  registers nothing and a consumer without REST instantiates neither. The header name is inside the
  gateway's reserved `X-Qits-*` namespace ON PURPOSE: qits-gateway strips the prefix from client
  traffic, so an outside caller cannot forge a cause. Absent and malformed both read as "no cause"
  — causation is advisory and must never fail a request. **The pom carries `jakarta.ws.rs-api` and
  never `quarkus-rest`**: the API jar lets the filters compile and stay inert, a compile-scope
  extension would bolt an HTTP server onto every consumer. `quarkus-rest`/`quarkus-rest-client`
  are test scope only, because the load-bearing assumption is a thread one — RESTEasy Reactive
  runs both filters and a blocking resource method on one worker — and
  `CausationRestPropagationTest` proves it on a real wire.

  **Causation reaches the rows through a JPA entity listener, opt-in per entity.** `CausedRow`
  (interface) + `@EntityListeners(CausationStamp.class)` + the entity's own nullable `causation_id`
  column and migration; the stamp fills a null from `CausationScope.current()`. The property that
  makes a ThreadLocal source safe here: **`@PrePersist` fires at `persist()`, on the calling
  thread, not later at flush** — `CausationRowStampingTest` closes the scope before the commit to
  prove it. An interface rather than a mapped superclass (entities spent their single inheritance
  on `PanacheEntity`), reached without reflection (native-image visible). Insert-only by decision —
  no `@PreUpdate`, the column is creation history — and the author's own value wins, the same
  precedence as everywhere else. Never a foreign key: the event lives in qits-events' store.
  `Uncaused` is the written opt-out; nothing here enforces completeness — that is qits-arch-rules'
  job (qits-integrations-quarkus-javalib), which matches these types BY NAME, so renaming `CausedRow`,
  `CausationStamp` or `Uncaused` breaks that suite's contract and every consumer's build with it.

  **No cycle guard and no self-parent repair, here or anywhere on this side.** A guard that catches
  only `A → A` cannot see `A → B → A` and its presence would tell a reader that cycles are handled;
  detection belongs where the graph is visible. qits-events does reject a self-edge, because that
  one is decidable from a single row.
- **There are three consuming seams, and the typed one is the one to reach for.** `QitsEventListener<E>`
  names an event *class* at compile time; `QitsRawEventListener` names a `Set<String>` of event
  *names* at runtime and receives the `EventFrame` itself. The raw one exists for consumers whose
  interest is genuinely unknowable at startup. A raw listener that could have named its event type
  is a typed listener with extra steps. `QitsDurableEventListener` is the third and has its own
  section below.

  **The subscribe frame is the union of both, and `"*"` collapses it.** `EventDispatcher.signatures()`
  unions every typed listener's signature with every raw listener's current set, sorted; the literal
  `"*"` anywhere in that union makes the whole frame `["*"]`. `SubscriptionUnionTest` exhausts the
  arithmetic; `EventStreamSubscriberTest` asserts once, on the wire, that this is the function
  actually used.

  **The bean set is resolved once; a raw listener's signature set is asked per subscribe and per
  frame.** That is what makes it dynamic, and it has one edge: a *widened* set takes effect for
  dispatch immediately but reaches qits-events only at the next reconnect, since the subscription
  lives on the connection and nothing re-dials on a listener changing its mind. So a consumer whose
  interest can grow should return `Set.of(ALL)` once and filter for itself, and the subscriber's "no
  signatures, no stream" rule then never surprises anyone.

  **Dispatch order is typed, raw, durable, and it is a contract rather than an accident.** Every path
  runs for a frame it wants, each listener gets it once, and containment is symmetric — a throw out
  of `onFrame`, or out of `signatures()`, costs that listener and nobody else, exactly as a throw
  out of `onEvent` does, because the caller is still a socket callback. **Causation is identical
  too**: every path runs inside the *same single* `CausationScope` of the arriving frame's id.
- **A durable listener's guarantee is exactly-once EFFECT, and every word of that is load-bearing.**
  Not exactly-once delivery: a handler can be *called* twice for one event, and the second call is
  the retry of one whose transaction rolled back. What happens once is the commit.

  **One funnel, both channels.** `DurableFunnel.offer` is the only way an event reaches a durable
  listener, and a live frame and a catch-up row take it identically: `selects` → claim
  `consumed_event` → call the handler, all inside one transaction. Do not add a second path. The
  claim is `insert … on conflict do nothing` rather than read-then-insert, because the two channels
  race by construction — the socket's worker thread and the scheduler's — and only the primary key
  closes that window.

  **A throw leaves the event owed, and the watermark is what enforces it.** The claim rolls back with
  the handler, `CatchupSweeper` stops paging for that listener, and the watermark stays at the end of
  the last *complete* page. A page half-processed is not progress. Equally: a `selects` that throws
  is a failure and not a "no", because a "no" would let the watermark pass an event the listener
  wanted, and the watermark never goes backwards.

  **A new consumer starts at the head of the log.** `replayFromEpoch()` is the opt-in and the default
  is false; anything else means the first deployment of every new subscriber re-acts on all history.
  `consumerId()` is storage, not a label — it keys both tables and must survive class renames.

  **Pruning mixes two clocks on purpose.** The claim carries `handled_at` (this consumer's clock) and
  the cut is derived from the watermark's `occurred_at` (the publisher's). A day of horizon is what
  makes that safe; shortening it to minutes would make host clock skew load-bearing.

  **A STALLED SWEEP CANNOT STALL FOREVER, and `synchronized` is what made it able to.** Measured,
  2026-09-08: qits-deployments self-deployed at 00:10:33; its `eventstream-catchup-startup` VIRTUAL
  thread went `catchUp()` → `read(consumerId)` → `ConsumerWatermarkRepository.findById` → Agroal
  `borrowValidation` → `PgConnection.isValid` and parked in a PostgreSQL socket read that never
  returned — a half-open TCP connection from the blue-green cutover overlap, no JDBC socket timeout.
  Narayana's reaper aborted the transaction at 00:11:45 (ARJUNA012117/012095/012381) and the thread
  did not move, because **a reaper abort does not unpark a socket read**. `catchUp()` was
  `synchronized` and the `@Scheduled` tick runs `ConcurrentExecution.SKIP`, so one parked thread held
  the monitor until 06:30: zero events consumed for six hours, every release of that window silently
  undeployed, every health check green. Green-while-dead, healed only by a restart.

  So: a `ReentrantLock`, and **a tick that finds it held runs the watchdog over the holder before it
  decides to skip** — the tick is the watchdog's heartbeat, and a heartbeat that goes straight to
  "skip" is what let this run. Past `qits.eventstream.sweep-stall-budget` the holder is named in an
  ERROR and interrupted, once per budget (a compare-and-set on the strike stamp is what bounds both
  the log volume and the re-interrupt cadence). **The interrupt measurement is the whole reason this
  is the shape it is**, Temurin 25.0.4.1, a thread parked in a blocking `java.net.Socket` read:
  a **virtual** thread is freed by `Thread.interrupt()` — unblocked in 1ms with
  `java.net.SocketException: Closed by interrupt`, interrupt status still set — and a **platform**
  thread is not, still blocked after 5s, because blocking-mode `SocketInputStream` reads are not
  interruptible off a virtual thread. The incident's thread was virtual, so the interrupt cures it;
  the `@Scheduled` tick is a platform thread, so a tick that wedges the same way is *not* freed and
  the ERROR plus `CatchupHealthCheck` going DOWN is the entire signal — the one that was missing.
  The complementary real cure is a JDBC `socketTimeout` on the datasource and is deliberately not in
  this jar.

  A long sweep is not a stalled one and must never be killed for being slow: a consumer draining a
  day of backlog is alive by definition, and only the budget being spent is the incident's shape.
  Equally, a listener that FAILED on a poison event does not stop a sweep counting as completed —
  that is the no-dead-letter design below, and it must not put a consumer DOWN. Only "the loop did
  not finish" does. `release()` clears the interrupt with `Thread.interrupted()` on purpose: the
  scheduler's thread is pooled and must not carry our interrupt into the next task.

## The store, and the lineage that restarted at V1

**The outbox runs on PostgreSQL, reached through the platform's generic resource contract.** The
three keys in `META-INF/microprofile-config.properties` are expressions over
`QITS_RESOURCE_EVENTSTREAM_URL` / `_USERNAME` / `_PASSWORD` with **no defaults**: a consumer declares
`resources: postgresql:eventstream:<database>` in its deployment spec, qits-deployments provisions
the role and the database before the cutover and injects the triple, and an unset variable dies at
Flyway naming itself instead of opening a store nobody meant. Nothing in the deployer is
eventstream-specific — mapping the generic variables is the *application's* job, and this jar is the
application for its own datasource.

**The H2 lineage (V1 + V2) was deleted rather than continued**, and one fact is what allowed it:
this table is empty in a healthy process. A publish that lands inline writes nothing and a row
delivered on retry is deleted, so the most an H2 file could have held at the moment of the move is a
few undelivered events — and the move is a re-bootstrap, which is where those go anyway. No postgres
database ever ran the H2 files, so no `V3__move_to_postgres.sql` had a reader. The fresh `V1__init.sql`
is those two migrations translated and merged: `text` instead of `clob`, and V2's `parent_id` folded
into the table with **no backfill**, since every database reaching it is empty. **A clean start is
not a precedent** — the ordinary rule (append, never edit an applied migration) is back from V1
onward, and V2 (`refusals`) and V3 (`consumed_event` + `consumer_watermark`) are that rule being
followed.

Two translation notes worth keeping:

- **`@Lob` is gone from `OutboxEvent`, replaced by `@Column(columnDefinition = "text")`.** On H2 the
  two agreed — `@Lob String` was a `clob` and the column was one. On PostgreSQL `@Lob` means a
  *large object*: Hibernate binds an oid and the insert fails against a `text` column. This is the
  only entity mapping the move had to change, and `OutboxFlowTest` asserts the payload round trip,
  so it is proved rather than reasoned.
- **The `check (status in …)` survives the translation**, where qits-deployments-platform-service
  dropped its enum checks. That was a defect answer, not a design one — H2 2.4.240 tied a compiled IN-set to its
  session and failed a valid insert with 23514 — and postgres has no such behaviour. Two values that
  the sweeper's own logic closes over are an invariant, not a catalogue that grows.

## What a consumer has to do, and what it must not forget

Registering a listener really is "add a bean" — no channel name, no annotation — and no
`@Unremovable` is needed, because `EventDispatcher`'s `Instance<QitsEventListener<?>>` is what ArC
counts as a use. That is asserted rather than trusted, since a removed listener subscribes to
nothing and says nothing about it: the suite keeps a listener injected nowhere and a raw listener
whose signature no `eventType()` produces, and requires both in the subscribe frame.

Beyond that there are three facts a consumer gets wrong once each.

- **The darkness belongs to the consumer, not to the library.** The jar ships
  `qits.eventstream.enabled=true` — a library that shipped dark is one whose first deployment
  discovers it was never wired up — and the consuming application's `application.properties` carries
  the `%dev`/`%test` `false`, exactly as it does for the OTel keys. Nothing else about the bus
  should be restated there: `qits.events.url`, the outbox datasource, the timeouts and the retry
  budget are ordinal-100 defaults in this jar, and a copy in the app's file is a second place to
  change.
- **Dark does not mean absent.** `enabled=false` stops publishing, sweeping and dialling; it does
  not stop the datasource. Quarkus opens the connection and runs Flyway at boot regardless, so a
  consumer's `src/test/resources/application.properties` must give `quarkus.datasource.eventstream`
  a database of its own — the resource variables below are a deployment fact and a suite has none.
  Measured, not assumed: this rule was bought when a suite without those lines migrated a real
  store and two builds on one host raced for it.

  **The deployment side of the same sentence cost a rollout, and the fix is now structural: adding
  this jar to a deployable adds a MANDATORY `resources:` line to its deployment spec.**
  `resources: postgresql:eventstream:<database>` in `.config/qits/deployments.yml` is what makes
  qits-deployments create the role and the database and inject
  `QITS_RESOURCE_EVENTSTREAM_URL` / `_USERNAME` / `_PASSWORD`. **The resource must be named
  `eventstream`** — the variable names follow the name, so any other spelling leaves this jar's
  expressions unresolved. The old failure was a shipped `${user.home}` default that a container with
  no `HOME` resolved to `?`: **a config default no JVM test exercises, failing only in the packaged
  artifact in its real environment.** The triple that replaced it has no default at all, which is
  what makes that class of bug unreachable — there is no fallback with a feature in it to lose, and
  a missing variable dies at Flyway naming itself.
- **The reflection registration is the consumer's, and it is the one that fails quietly.** A
  deployable that native-image-compiles must carry a `@RegisterForReflection` naming its own event
  classes, `EventEnvelope`, `EventFrame` **and** `CanonicalJson$QitsEventMixin` (by string name; it
  is not public). `EventWireReflection` in qits-ci-service is the worked example.

  Without it the binary throws Jackson's `No serializer found for class … you may need to configure
  reflection` on **every** publish — inside `CanonicalJson` and therefore *before* an envelope
  existed, so the event never reached the outbox either. Not a delayed delivery, a lost one, with a
  single WARN to say so.

  Nothing registers them automatically because **`CanonicalJson` builds its own `ObjectMapper` on
  purpose** (above, and not negotiable): the graph that mapper serializes is invisible to the build
  step that scans for what needs reflecting on. **The mix-in is in the list because two binaries
  were built to find out, and it is the worse of the two failures.** Jackson reads its `@JsonIgnore`s
  with `getDeclaredMethods()`; with the record types registered and the mix-in left out, a green
  build published a payload containing `"eventId":"00a32ad6-…"` — no crash, no log, identity present
  in a body that is supposed to carry none. A wire contract violation that breaks nothing visible is
  not a lesser bug than one that throws.

  The registration lives in the consuming deployable rather than in this jar because the deployable
  is what gets built into an image and it is the deployable that knows its own event classes. A JVM
  test can only guard the list's *completeness* — on a JVM these classes reflect whether anyone
  registered them or not. The correctness proof is the binary, running.

**The far end of that failure was mute, and that is fixed here.** `EventDispatcher` logged a frame
it could not read at DEBUG, so a binary that could not deserialize `EventFrame` would have consumed
the entire stream in silence for as long as it ran. It is a WARN now, naming the frame's `name` and
`id` when the text is JSON at all (a second, untyped read — `readTree` needs no reflection, which is
precisely why it still works when binding does not). An unknown *signature* stays DEBUG: that one is
ordinary traffic, since a subscription set is a filter rather than a promise.

## The rollout order, which is one-directional

**qits-events ships first.** Quarkus does not fail on unknown JSON properties, so a publisher that
stamps a field against a qits-events that has not yet learned it has its parents silently dropped —
every chain of that window recorded as roots, and causation cannot be backfilled from anything. The
other direction (a server that knows the field, a publisher that never sends it) is the
compatibility clause, since an absent `parentId` binds to null.

The same clause governs anything added to the envelope later. Add it to the server, deploy, then
add it here. `environment` — the tier stamped from `qits.environment`, `platform` where none is
configured — was added under exactly this rule, and its property name is deliberately spelled as a
literal here rather than imported from qits-integrations-quarkus-javalib (the extraction rule): grep
both repos on a rename.

## The suite

128 tests, all surefire, about twenty seconds — the extra few are one embedded postgres starting, and
`CatchupStallTest` is the slowest class in the suite because a stall watchdog can only be asserted
against a sweep that really is parked on another thread.
The database is `eventstream_test` on that instance, named for this repository rather than for a
module so a consumer's suite spawning its own postgres on the same host cannot mean the same one.
`clean-at-start` wipes the schema between Quarkus restarts, which is what keeps a suite sharing one
database across classes reproducible.

A second database, `eventstream_consumer_test`, backs the suite's DEFAULT persistence unit — the
consumer's stand-in, holding the `consumer/CausedThing` fixture so row stamping is proved in the
exact arrangement a service has. With a named unit present the default one needs its own package
claim, and the claims are prefix matches — which is why the fixture sits in its own `consumer`
subpackage the named unit's `entity` claim cannot reach. Its schema comes from Hibernate
(`schema-management.strategy=drop-and-create`), not Flyway: it is a fixture, and this jar owns no
migrations for it.

Four conventions in `src/test/resources/application.properties` are load-bearing:

- **`quarkus.http.test-port=0`.** This module registers no route of its own — `websockets-next` is
  here for its CLIENT — but the extension brings an HTTP server along and a `@QuarkusTest` starts
  it. Several test classes ask for different configurations, so Quarkus restarts between them, and
  on the default 8081 the next instance races the previous one's release. Port 0 takes a free one.
  Keep it: 8081 is occupied on more than one development host.
- **`quarkus.scheduler.enabled=false`.** Nothing may sweep behind a test's back: every outbox
  assertion drives `OutboxSweeper#sweep` by hand against a clock it moved, every catch-up assertion
  drives `CatchupSweeper#catchUp` against a log it is still seeding, and a background tick landing in
  the middle makes both non-deterministic. The jobs are still discovered at build time, so the
  `@Scheduled` wiring is real — it simply never fires inside a suite run.
- **`qits.eventstream.catchup-at-startup=false`.** The other half of the same rule, and it needs its
  own key because the startup sweep is a `StartupEvent` observer rather than a scheduled job. Without
  it a virtual thread would be reading the stub's log while a test arranges it.
- **The redial backoffs, shrunk to milliseconds.** The SHAPE is what is under test (a drop is
  followed by a new connection carrying a fresh subscribe frame), not the duration, and the shipped
  values are in the jar's own properties where they belong.

`EventstreamClock` is an `@ApplicationScoped` `@DefaultBean` producer, which is what lets `TestClock`
outrank it with no alternative, no priority and no profile. Everything time-dependent here is
arithmetic on instants, and the only way to test that without sleeping through it is to move the
clock.

`StubEventsServer` is the far end: a Vert.x server answering the PUT, the list route and the
websocket upgrade, with scripted responses and recorded requests. It is what makes the clone-alone
rule survivable, and it is the thing to extend when a test needs the server to behave a new way — not
a mock of `EventsPublisher`, which would stop the wire being under test.

**Its list route is the one place the stub decides anything, and that is deliberate.** A paging loop
can only be tested against something that pages: the properties under test are a cursor that neither
drops nor repeats a row across a boundary, and a `nextCursor` that is null on the last page *even
when the page came back full*. A canned answer cannot exhibit either. Everything else about the stub
stays dumb.

## Not yet here

- **No typed durable seam.** `QitsDurableEventListener` hands over the `EventFrame`, because one of
  its motivating consumers subscribes to `"*"` from configuration and a `Class<E>` cannot express
  that. A durable listener that knows its event type deserializes the payload itself with
  `CanonicalJson.payloadTo`. A typed variant is an addition, not a change, if enough consumers want
  one.
- **No dead-letter for a durable handler.** An event a handler keeps throwing on is offered again
  forever, and the watermark stays behind it — so one poison event stops that listener's catch-up
  while the live stream carries on. The log says so on every sweep. A bounded attempt count here
  would be the outbox's give-up bug in the other direction: the safe failure is to keep owing the
  event, and the loud one is the WARN.
