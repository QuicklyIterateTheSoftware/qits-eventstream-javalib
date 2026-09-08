# qits-eventstream-javalib

The platform's **event bus client**, published as the `qits-eventstream` jar. A service that carries
this jar can announce that something happened, and can be told when something happened elsewhere.
The far end is
[qits-events](https://github.com/QuicklyIterateTheSoftware/qits-events-platform-service), which stores the log and
broadcasts the stream; everything here is the client half of that.

**It enables event STREAMING. It does not do event sourcing.** There is no event store here, no
aggregate, no replay, and the one table it owns is a retry queue that is *empty* when things are
working. The module was called `eventsourcing` for its first four commits, inside qits-ci, and the
name is the only thing about it that changed on the way out. The superproject's
`eventsourcing-plan.md` and `event-causation-plan.md` — where the design is argued — still carry the
old spelling, because they are documents that were written rather than code that runs.

    <dependency>
        <groupId>eu.wohlben.qits</groupId>
        <artifactId>qits-eventstream</artifactId>
        <version>1.0.0-SNAPSHOT</version>
    </dependency>

Quarkus, JDK 25. A consumer needs no other wiring: the jar carries its own beans index, its own
config defaults and its own database.

## The whole public surface

Twelve types. Everything else in here is how they are kept.

| | |
|---|---|
| `QitsEvent` | something that happened. Implement it on a record. |
| `QitsEventBus` | `publish(event)` / `publish(event, parentEventId)`. Inject it. |
| `QitsEventListener<E>` | consume one event type. Implement it on a bean. |
| `QitsRawEventListener` | consume frames for names chosen at runtime. |
| `QitsDurableEventListener` | consume **exactly once**, catching up on what the stream missed. |
| `CausationScope` | the ambient cause, for carrying an edge across a thread. |
| `CausationHeader` | the same cause on the wire — `X-Qits-Causation-Id`, for carrying an edge across a service. |
| `CausedRow` | the same cause in a table: an entity whose rows record the event they were written because of. |
| `CausationStamp` | the JPA entity listener that fills a `CausedRow` at persist. |
| `@Uncaused` | the written opt-out, read by the qits-arch-rules suite and by nobody at runtime. |
| `CatchupSweeper` | explicitly catch up or rebuild one durable projection by its consumer id. |
| `CatchupResult` | whether that named catch-up reached the log head, or why it did not. |

Two `@Provider` filters, `CausationClientFilter` and `CausationServerFilter`, apply the header
automatically; nobody names them, so they are not in the table.

### Publishing

```java
public record BuildSuccessful(String repoId, String runId, Instant finishedAt) implements QitsEvent {
  // The interface's four methods are excluded from the payload, so the record's own components are
  // the whole wire body. An event may hold its eventId as an ordinary component; the mix-in hides
  // the accessor, and identity travels in the envelope.
}

@Inject QitsEventBus bus;

bus.publish(new BuildSuccessful(repoId, runId, finishedAt));
```

`publish` **never throws and never blocks for long**. It attempts the idempotent
`PUT /events/api/events/{id}` inline, and if that does not land within
`qits.eventstream.publish-timeout` (5s) the event goes to the outbox and a scheduled sweeper owns
delivery from there. A caller therefore never sees a delivery failure and never has to decide what
to do about one — which is what lets a publish sit in the middle of a business transition without
becoming a way for that transition to fail.

### Consuming

```java
@ApplicationScoped
public class BuildSuccessfulListener implements QitsEventListener<BuildSuccessful> {

  @Override public Class<BuildSuccessful> eventType() { return BuildSuccessful.class; }

  @Override public void onEvent(BuildSuccessful event) { ... }
}
```

That is the whole registration. No channel name, no annotation, no configuration. The subscriber
collects every listener bean at startup, subscribes to the union of their event names, and dials
qits-events' `/events/stream` on `StartupEvent`. **An application with no listener beans never dials
at all** — there is nothing to subscribe to.

**Delivery is at-most-once and the stream is live-only.** A listener is not a queue consumer: it
sees what is broadcast while it is connected, and events that occurred during a disconnect are not
replayed. So a listener may do anything that tolerates being skipped, and nothing that must happen
exactly once — for that, see *Consuming durably* below.

`onEvent` runs on a worker thread, one frame at a time, and a throw is logged and swallowed rather
than taking the socket down. Anything slow belongs on the listener's own executor — see *Causation*
below for what that costs and how to pay it.

### Consuming frames instead of types

`QitsRawEventListener` names a `Set<String>` of event *names* at runtime and receives the
`EventFrame` itself. **Reach for it only when the interest is genuinely unknowable at startup** — a
trigger engine whose selections live in files inside other repositories, an audit sink. A raw
listener that could have named its event type is a typed listener with extra steps.

The subscribe frame is the **union** of every typed listener's signature and every raw listener's
current set, sorted. The literal `"*"` (`QitsRawEventListener.ALL`) anywhere in that union collapses
the whole frame to `["*"]`: once one consumer wants everything, narrowing the wire buys nothing, and
the surplus frames are dropped in dispatch at no cost to anyone else.

`signatures()` is asked per subscribe **and per frame**, which is what makes the set dynamic. One
edge follows from that and is worth knowing: a *widened* set takes effect for dispatch immediately
but reaches qits-events only at the next reconnect, since the subscription lives on the connection
and nothing re-dials on a listener changing its mind. **A listener whose interest can grow should
return `Set.of(ALL)` once and filter for itself.**

Dispatch order is **typed first, raw second**, and it is a contract rather than an accident. Both
run for a frame both want, each listener gets it once, and containment is symmetric: a throw out of
`onFrame`, or out of `signatures()`, costs that listener and nobody else.

### Consuming durably

When missing an event would be a defect rather than a nuisance — a build that never triggers a
deployment, a release nobody rolls out — implement `QitsDurableEventListener` instead:

```java
@ApplicationScoped
public class BuildSuccessfulSubscriber implements QitsDurableEventListener {

  @Override public String consumerId() { return "deployments.build-successful"; }

  @Override public Set<String> signatures() { return Set.of("BuildSuccessful"); }

  @Override public boolean selects(EventFrame frame) { ... }   // optional; default is everything

  @Override public void onFrame(EventFrame frame) { ... }
}
```

**The guarantee is exactly-once *effect* per (listener, event id).** Every arrival — a live frame and
a row read back from the log — goes through one funnel: in a single transaction the library claims
the event in `consumed_event` and then calls `onFrame`. A second arrival finds the claim and is
dropped. A handler that throws rolls its claim back with it, so the event stays owed and is offered
again; the handler's own database writes join that transaction, so the effect and the claim commit
together.

**A catch-up sweep pages the log forward** from a per-listener watermark, every
`qits.eventstream.catchup-interval` and once at startup — which is the cutover case. That is what
makes a restart, a redeploy or a dropped connection a delay instead of a hole.

Four things follow, and each of them has bitten somebody:

- **`consumerId()` is storage.** It keys both tables, so it must survive class renames — which is why
  it is a string you choose. Changing it makes a brand-new consumer; reusing one makes a listener
  inherit another's history.
- **A new consumer starts at the head of the log**, not at the epoch. `replayFromEpoch()` is the
  opt-in for a consumer whose point is the whole history.
- **Only selected events are stored.** The watermark is what makes that safe: catch-up re-reads only
  what is above it, so widening a predicate later cannot resurrect ancient history. Claims below the
  watermark are pruned.
- **Ordering is yours.** Catch-up delivers late and out of stream order relative to live frames, and
  the library does not reorder. A handler whose effect is last-writer-wins must check the tip before
  acting — deploy only if this build is still the newest green one for its repository and branch.

#### The stall watchdog, and saying so out loud

**A sweep that stops coming back used to stop the module forever.** On 2026-09-08 a self-deploying
service left its `eventstream-catchup-startup` thread parked in a PostgreSQL socket read — a
half-open connection from the blue-green cutover overlap, with no JDBC socket timeout to end it.
`catchUp()` was `synchronized` and the scheduled tick runs `SKIP`, so that one thread held the
monitor for six hours: nothing was consumed, every release of the window silently failed to deploy,
and every health check stayed green. Only a restart healed it.

The sweeps are still serialized — two of them paging one watermark was never wanted — but on a lock
rather than a monitor, and **a tick that finds the lock held looks at the holder instead of queueing
behind it**. Past `qits.eventstream.sweep-stall-budget` it logs an ERROR naming the sweep, its
thread and how long it has held, then interrupts that thread. On a *virtual* thread that is a real
cure: measured on Temurin 25.0.4.1, a blocking socket read unblocks in 1ms with
`java.net.SocketException: Closed by interrupt`. On a *platform* thread it is not, which is why the
ERROR and the check below are the other half rather than a nicety. Nothing is left half-done by a
kill: the watermark only ever moves on a whole page, so the interrupted sweep's events are simply
still owed and the next sweep offers them again.

**`eventstream-catchup` is a `@Readiness` check this jar ships**, and DOWN means one of two things:
a sweep has held the lock past its budget, or no sweep has completed within the staleness horizon
(three catch-up intervals, never less than the stall budget). A sweep merely *running* is UP — a
consumer draining a day of backlog is alive by definition — and so is an application with the module
dark or with no durable listener at all, because a library must never make a consumer red over a
feature it does not use. With nothing completed yet the horizon runs from process start, which is
the boot grace: a service starting into a real backlog is UP until it has had a whole horizon to
finish its first sweep.

A consumer with `quarkus-smallrye-health` gets the check for free — no registration, it is discovered
from this jar's index — and a consumer without it gets an inert bean ArC removes. It is `@Readiness`
and deliberately not `@Liveness`: readiness takes the instance out of rotation, which is the honest
statement ("this process is not consuming"), while liveness asks for a restart, and a library that
shipped one would be a library that can restart its consumers' processes. The in-process cure is the
interrupt; a consumer that wants a restart on top of it wires this fact into its own liveness check.
The complementary real fix — a JDBC `socketTimeout` on the datasource — is not in this jar.

#### Rebuilding a projection at startup

An ordinary durable consumer retains its watermark and handled-event claims across restarts. That is
the right answer for an effect that must happen once, but it cannot rebuild a projection whose own
database was cleared: retained claims would make qits-eventstream skip the historic events that the
empty projection needs.

A listener whose purpose is a replayable projection returns `true` from `replayFromEpoch()`. Its
first ordinary sweep writes the epoch watermark **and pages to the current log head in that same
call**; it does not wait for the next `catchup-interval`. An application that must gate readiness on
that fact may inject `CatchupSweeper` and call:

```java
CatchupResult result = catchup.rebuildFromEpoch("platform-edge.deployment-active");
if (result.reachedLogHead()) {
  // the projection has processed the complete log snapshot; make routes visible
}
```

`rebuildFromEpoch` is allowed only for a listener that opted into `replayFromEpoch()`. It clears
that consumer's `consumer_watermark` and `consumed_event` claims together, writes the epoch, and
replays to a page explicitly marked final by qits-events. It also pauses live durable delivery while
the reset and replay run, so no stream callback can claim a row between them. It intentionally
re-invokes handlers: reset the projection's own store first, or make its handler an idempotent
replacement.

For a retained projection, `catchUp("consumer-id")` returns the same `CatchupResult` without
clearing state. `REACHED_HEAD` is the only success status; `UNAVAILABLE`, `FAILED` and `INCOMPLETE`
must keep a bootstrap gate closed. The existing count-returning `catchUp()` remains the scheduled
all-listener API for callers that do not need a readiness certificate.

### Causation

The envelope carries a nullable `parentId` — the event that caused this one — and `publish` is the
only place it is resolved. **The precedence rule, whole: an explicit non-null argument wins; a null
or absent one falls back to `CausationScope.current()`; outside any scope the event is a root.** So
`publish(e)` *is* `publish(e, null)`.

The dispatcher runs every listener for a frame inside one `CausationScope` of that frame's id, so a
listener that publishes *on the dispatch thread* records the edge with nobody passing an argument. A
hand-off to your own executor leaves that scope behind — a plain `ThreadLocal` does not follow work,
deliberately — so carry it:

```java
UUID cause = CausationScope.current();                     // on the dispatch thread
executor.submit(() -> CausationScope.with(cause, () -> bus.publish(followUp)));
```

or pass it: `bus.publish(followUp, cause)`. What is not an option is doing neither and believing the
chain was recorded: **a dropped parent is a root event, and that loss cannot be backfilled from
anything.**

`CausationScope.with(null, …)` is the deliberate detach — a statement about a region, as against
`publish(e, null)`, which only means "I have no argument to pass". The asymmetry is settled.

#### Across a service boundary

A chain does not end at a REST call. Two `@Provider` filters carry the scope over HTTP as the
`X-Qits-Causation-Id` header (`CausationHeader.NAME`), and a consumer registers nothing:

- `CausationClientFilter` — every REST-client request sent inside a scope carries the header. A
  header the caller set itself wins, mirroring `publish(event, parentEventId)`.
- `CausationServerFilter` — a request carrying the header runs its resource method inside
  `CausationScope` of that id, and the worker thread is left as it was found. Absent and malformed
  read as "no cause"; causation is advisory and never fails a request.

So event 1 in service A triggering a REST call whose handler in service B publishes event 2 records
`1 → 2` with neither service passing anything. The header name sits in the gateway's reserved
`X-Qits-*` namespace, so an outside caller cannot forge a cause: qits-gateway strips the prefix at
the edge, and service-to-service traffic never passes the gateway.

The filters need the REST server or client to exist — a consumer without them never instantiates
either — and they ride the request thread. A blocking resource method (the platform's shape) sees
the scope; an async method whose continuation migrates threads inherits the executor caveat above.
A caller building requests by hand stamps the header itself:

```java
UUID cause = CausationScope.current();
if (cause != null) request.header(CausationHeader.NAME, cause.toString());
```

#### Into the rows

A row written while a cause is ambient can record it, so tracing walks from a table back into the
chain — the persistence spelling of the same scope. Participation is per entity:

```java
@Entity
@EntityListeners(CausationStamp.class)
public class WorkspaceRow extends PanacheEntity implements CausedRow {

  @Column(name = "causation_id")
  public UUID causationId;

  @Override public UUID causationId() { return causationId; }
  @Override public void causationId(UUID id) { this.causationId = id; }
}
```

The service owns the migration that adds the nullable column, exactly as it owns its table.
`@PrePersist` fires when `persist()` is called, **on the calling thread** — not later at flush — so
the scope a request filter or the dispatcher established is still standing when the stamp reads it.
The familiar rules apply unchanged: a value the author set wins; outside any scope the column stays
null (a rootless row); and the stamp is **insert-only** — the column answers "which event caused
this row to exist", and updates do not rewrite creation history. The column can never be a foreign
key: the event it names lives in qits-events' store.

An interface plus a listener rather than a mapped superclass, because entities have spent their
single inheritance already — on `PanacheEntity` or a base of their own.

Nothing here warns about an entity that forgot to participate. That is the qits-arch-rules suite's
job (in qits-integrations-quarkus-javalib): its rules require every `@Entity` to either implement
`CausedRow` or carry `@Uncaused`, so forgetting fails a build and opting out is one reviewable
line.

## Configuration

Shipped as `META-INF/microprofile-config.properties` at **ordinal 100**, so the consuming
application (250) and the environment (300) override any of it. A library jar's own
`application.properties` would be ignored, which is why the defaults live where they do.

| key | default | |
|---|---|---|
| `qits.events.url` | `http://qits-events:8080` | scheme + host + port, **no path**. This module appends `/events/api/events/{id}` and `/events/stream` itself, swapping the scheme to `ws(s)` for the second. A path here yields a doubled one and a 404 nothing retries out of. |
| `qits.eventstream.enabled` | `true` | the master switch. |
| `qits.eventstream.publish-timeout` | `PT5S` | the inline attempt's deadline, after which the outbox owns the event. |
| `qits.eventstream.max-attempts` | `5` | the **refusal** budget, counting the inline attempt: five means the PUT plus four sweeps against a service that is answering. An attempt that got no answer spends none of it. |
| `qits.eventstream.sweep-interval` | `10s` | how often the sweeper looks. A floor on how late a retry can be, never a cause of an early one. |
| `qits.eventstream.redial-initial-backoff` | `PT1S` | doubled per consecutive failure. |
| `qits.eventstream.redial-max-backoff` | `PT30S` | the cap. |
| `qits.eventstream.catchup-interval` | `PT30S` | how often each durable consumer's watermark is paged forward. The worst-case lateness of an event the **stream** did not deliver. |
| `qits.eventstream.catchup-at-startup` | `true` | also sweep once at boot — the cutover cure. On its own thread, so it never delays a start. A test suite turns it off. |
| `qits.eventstream.sweep-stall-budget` | `PT5M` | how long one sweep may hold the catch-up lock before the watchdog names it in an ERROR, interrupts its thread and puts the `eventstream-catchup` readiness check DOWN. Generous on purpose: a long sweep draining a real backlog is normal, and only "no honest sweep runs this long" is the fault. |
| `qits.eventstream.prune-horizon` | `P1D` | how far below the watermark a handled-event claim is kept. Pure overlap; generous because the comparison mixes two clocks. |

The last four do nothing at all in an application with no durable listener.

The default `qits.events.url` is the qits-net alias, which is right for any deployment on that
network and wrong for a host-run process — a stack that publishes qits-events on a mapped localhost
port overrides it.

**The switch ships ON, and the darkness belongs to the consumer.** A library that shipped dark is a
library whose first deployment discovers it was never wired up. The `%dev` / `%test` `false` belongs
in the consuming application's `application.properties`, exactly where it goes for OTel — so a
`quarkus:dev` with no qits-events on the far side makes no dials rather than retries. Off means
`publish()` is a debug log, neither sweeper does anything, the subscriber never dials and no durable
listener is offered anything. There is no half-enabled state.

### The database, and the resource a deployment must declare

This jar owns its **own** named datasource, persistence unit and Flyway lineage — `eventstream`,
migrations at `db/eventstream/migration` — and never shares the consuming service's database or its
migration history. Three tables: `outbox_event` for the publishing half, `consumed_event` and
`consumer_watermark` for the durable-consuming one. **The store is PostgreSQL**, reached through the platform's generic resource
contract:

    quarkus.datasource.eventstream.db-kind=postgresql
    quarkus.datasource.eventstream.jdbc.url=${QITS_RESOURCE_EVENTSTREAM_URL}
    quarkus.datasource.eventstream.username=${QITS_RESOURCE_EVENTSTREAM_USERNAME}
    quarkus.datasource.eventstream.password=${QITS_RESOURCE_EVENTSTREAM_PASSWORD}

**The pool ships the platform's resilience baseline, so a consumer restates nothing** — the patient
driver, validation at borrow and a 15s acquisition timeout, all at ordinal 100 and all overridable
at 250 like everything else here:

    quarkus.datasource.eventstream.jdbc.driver=eu.wohlben.qits.db.PatientPgDriver
    quarkus.datasource.eventstream.jdbc.acquisition-timeout=15S
    quarkus.datasource.eventstream.jdbc.validate-on-borrow=true

`PatientPgDriver` delegates to `org.postgresql.Driver` and, while the database is not there, keeps
asking for up to 14s instead of failing at once — under the acquisition timeout, so a caller that
outlasts it still gets the database's real refusal. It comes with this jar (`qits-db-core`, runtime
scope), because a default that names a class is only a default if the jar brings the class along.
The doctrine and the measurements are in the superproject's `docs/project-setup-quinoa-angular.md`.

**Adding this jar to a deployable adds one line to its deployment spec.** The consuming repository
declares the resource in `.config/qits/deployments.yml` —

    resources: postgresql:eventstream:<database>

— and qits-deployments creates the role and the database before the cutover, then injects those
three variables into the container. **The resource must be named `eventstream`**: the variable names
follow the name, so a spec that calls it anything else leaves this jar's expressions unresolved.

**The triple has no defaults, and that is the refuse-to-boot stance.** An unset variable is an
unresolvable expression, so the process dies at Flyway naming what is missing rather than opening
some fallback store nobody meant. There is no local file to fall back to any more — which retires
the `${user.home}` default that once cost a rollout, since a container with no `HOME` resolved it to
`?` and only the packaged artifact in its real environment ever found out.

**`enabled=false` does not stop the datasource.** Quarkus opens the connection and runs Flyway at
boot regardless, so a consuming **test suite** must point `quarkus.datasource.eventstream` at a
database of its own — the resource variables are a deployment fact and a suite has none.

## What is on the wire

The `PUT` body, and — plus the row's id — what comes back out of the stream:

```json
{"description":null,"environment":"dev","name":"BuildSuccessful",
 "occurredAt":"2026-07-31T12:46:03Z","parentId":"6c3f2b1a-…",
 "payload":"{\"repoId\":\"…\",\"runId\":\"…\"}"}
```

`payload` is the event's own fields as a canonical JSON **string** — a string, not a nested object,
because the server stores and compares it verbatim and never has to parse it. Everything `QitsEvent`
declares is excluded from it: identity, causation and the environment travel in the envelope.

`environment` is the tier this process publishes from: `qits.environment` — the MicroProfile
spelling of the `QITS_ENVIRONMENT` the deployer injects into every environment-tier service — or
the literal `platform` where a deployment injects none, which is the platform plane's own
definition of itself. Resolved by `QitsEventBus` at the same single point the cause is, stored on
the outbox row for the same reason the parent is (the server compares it, so a sweep must resend
what the inline attempt sent), and never a `QitsEvent` method.

**Canonical means the string is a function of the value and of nothing else.** Keys sorted, no
insignificant whitespace, absent fields omitted rather than written as explicit nulls. qits-events
compares `name` + `occurredAt` + `payload` + `parentId` + `environment` byte-for-byte to tell an idempotent replay
(200) from a reused UUID (400), so two serializations of one event that differ by a space are, to
the far end, a contradiction. `CanonicalJson` therefore builds its **own** `ObjectMapper` and sets
every knob explicitly. Read AGENTS.md before touching any of it.

`eventId` is fixed at construction and **never regenerated**. It is the `{id}` of the PUT, which is
the only reason a retry is safe: a request whose response was lost replays as a 200 instead of
writing the event twice.

## The outbox

**Failure-path only, and empty in a healthy process.** A publish that lands writes nothing; a row
that is delivered on retry is deleted. So the row count is a health signal rather than a log — the
log is qits-events — and a monitoring check on this table is asking a real question.

Attempts are spaced `1s · 4^(n-1)`, capped at five minutes, held per row in `next_attempt_at`.

**Giving up is bounded by refusals, never by unreachability.** The two failures are different
evidence and the outbox treats them so:

| what came back | what it means | what the outbox does |
|---|---|---|
| 200 / 201 | in the log | delete the row |
| 400 | a UUID was reused | `FAILED` at once, with a WARN |
| any other status | something answered and said no | retry, and spend one of `max-attempts` |
| nothing at all | the bus is unreachable | retry **forever**, at the schedule's five-minute cap |

The last row is why the table has two counters. `attempts` is every try and is what the backoff is
spaced by; `refusals` counts only the tries that got an answer, and it is the only one the budget
bounds. The split was bought on 2026-08-10: a publisher aimed at an alias that did not resolve spent
its whole budget on `ConnectException`s and left `FAILED` rows behind — events that never reached
the bus, and a hole no consumer-side bookkeeping can recover. *The bus is the record* is only true if
reaching it is never abandoned.

An unreachable bus is reported once per sweep rather than once per event: a bus that is down is one
condition however many events are queued behind it, and rows come due at the backoff, so the notice
rate-limits itself to five-minutely as an outage lengthens.

The known hole is named in `OutboxEvent`'s javadoc and deliberately left open: a crash between the
inline attempt failing and the row committing loses the event.

## Building

    ./mvnw verify

**A clone of this repo alone builds and tests green** — no monorepo, no docker, no credentials, no
prior `mvn install` anywhere. That is the gate, and it is the reason this pom duplicates versions
instead of inheriting them. The suite starts its own stub qits-events on a Vert.x server and runs
the two stores on a **real postgres it spawns itself** — zonky's binaries, resolved as ordinary Maven
artifacts and started as a child process, never a container — so nothing is skipped for want of
infrastructure. 128 tests, about twenty seconds.

`.sdkmanrc` names `25.0.2-graalce`. The jar compiles into a consumer's GraalVM native image, but
**the consumer owns the reflection registration** — read AGENTS.md's section on it before shipping a
binary, because the failure it describes is silent.

## Working on it

`AGENTS.md` (symlinked as `CLAUDE.md`) is the rest: the rules that bite, each with the measurement
that bought it.
