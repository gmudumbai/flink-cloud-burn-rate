# Implementation Plan: `flink-cloud-burn-rate`

**Real-time cloud spend rate from CloudTrail resource-lifecycle events, built on Apache Flink.**

## Instructions for the implementing model

You are implementing this project **one phase at a time**. After completing a phase:

1. Run the acceptance check for that phase and paste the actual output.
2. Summarize what you built in 3–5 sentences, in plain language.
3. **Stop and wait.** The user will ask questions about the phase before you continue. Do not start the next phase until told to.

Conventions:
- Everything runs locally via Docker Compose. No AWS account, no cloud resources.
- Java 17, Maven, Apache Flink 1.20.x (latest 1.20 patch), `flink-connector-kafka` matching 1.20. Verify exact artifact versions against Maven Central before writing the `pom.xml`.
- Broker: Redpanda (Kafka-compatible, single container). Image: `redpandadata/redpanda`.
- Use the **DataStream API only**. Do not use the Table API or SQL.
- Keep everything in one Flink job. Keep code small and readable; this is a learning project, not a product.
- Commit at the end of every phase with a message like `phase 2: keyed resource lifecycle state`.
- If something in this plan conflicts with what actually works in the current Flink version, do what works and note the deviation.

Repo layout target:

```
flink-cloud-burn-rate/
├── README.md
├── docker-compose.yml
├── generator/
│   ├── generate_events.py
│   └── requirements.txt
├── pricing/
│   └── ec2-us-east-1.json
├── flink-job/
│   ├── pom.xml
│   └── src/main/java/com/example/burnrate/
│       ├── BurnRateJob.java
│       ├── model/CloudTrailEvent.java
│       ├── model/BurnRateUpdate.java
│       ├── ResourceLifecycleFunction.java
│       └── PricingLookup.java
│   └── src/test/java/com/example/burnrate/
└── docs/
```

Time budget: ~7 hours total. Phase times below are targets, not limits.

---

## Phase 0 — Skeleton: broker, Flink cluster, a job that prints (≈45 min)

### Goal
A running Flink cluster reading from a Kafka topic and printing raw messages. Nothing domain-specific yet.

### Build
1. `docker-compose.yml` with three services:
   - `redpanda` — single node, expose `9092` on host and an internal listener for containers. Enable auto-create topics.
   - `jobmanager` — `flink:1.20-java17` (or nearest), command `jobmanager`, expose `8081`.
   - `taskmanager` — same image, command `taskmanager`, `taskmanager.numberOfTaskSlots: 4`, depends on jobmanager.
   Use `FLINK_PROPERTIES` env for config. Mount `./flink-job/target` into the jobmanager so the built jar is visible for `flink run`.
2. Maven project in `flink-job/` with `flink-streaming-java`, `flink-connector-kafka`, `flink-clients` (provided scope where appropriate), and the shade plugin to build a fat jar.
3. `BurnRateJob.java`: read topic `cloudtrail-events` with `KafkaSource`, string deserializer, `.print()`, execute. Bootstrap server and topic from env vars with defaults.
4. A one-line script or README snippet to produce a test message with `rpk topic produce` inside the redpanda container.

### Acceptance check
- `docker compose up -d` → Flink UI reachable at `http://localhost:8081` showing 1 TaskManager with 4 slots.
- `mvn -f flink-job/pom.xml clean package` succeeds.
- Submit the jar (`docker compose exec jobmanager flink run /path/to/jar`). Job shows RUNNING in the UI.
- Produce a message via `rpk`; it appears in the TaskManager stdout log (`docker compose logs taskmanager`).

### Learning checkpoint (user asks about)
- What the JobManager and TaskManager each do; what a task slot is; what parallelism means.
- Why the job is submitted as a jar rather than run like a normal `main()`.
- Where `.print()` output actually goes and why.

---

## Phase 1 — Event generator: realistic CloudTrail lifecycle events (≈45 min)

### Goal
A Python script that streams CloudTrail-shaped EC2 lifecycle events into the topic, including deliberately out-of-order and late events.

### Build
1. `generator/generate_events.py` using `kafka-python` (or `confluent-kafka`). Args: `--rate` events/sec (default 5), `--accounts` (default 3), `--duration` seconds.
2. Emit JSON events mimicking real CloudTrail structure. Include at minimum:
   - `eventTime` (ISO 8601 UTC), `eventName` (`RunInstances` | `TerminateInstances`), `awsRegion`, `recipientAccountId`
   - `userIdentity.arn`
   - `responseElements.instancesSet.items[]` with `instanceId`, `instanceType`
   - a `tags` map with a `team` key (values like `platform`, `ml`, `web`)
   Look up the real CloudTrail `RunInstances` event shape and match field names; don't invent a schema.
3. Maintain an in-memory set of running instances so `TerminateInstances` references a real prior `RunInstances`. Instance types drawn from a small list that exists in `pricing/ec2-us-east-1.json` (Phase 2).
4. **Out-of-order simulation:** for ~10% of events, set `eventTime` 5–20 seconds in the past relative to the generator's clock. For ~2%, set it 60+ seconds in the past (these will become "late" in Phase 3). Send them immediately regardless.
5. Log a one-line summary per event to stdout so the user can watch.

### Acceptance check
- `python generator/generate_events.py --duration 30` produces ~150 events; the Flink job from Phase 0 prints them.
- Visually confirm some events have `eventTime` earlier than the preceding event.

### Learning checkpoint
- Difference between when an event happened (`eventTime`) and when Flink sees it. Why this gap is unavoidable in real systems.
- Why CloudTrail is a legitimate streaming source but the Cost and Usage Report is not.

---

## Phase 2 — Keyed state: per-resource lifecycle and burn-rate updates (≈90 min)

### Goal
The job tracks each instance's running state and emits a `BurnRateUpdate` whenever an account's committed hourly spend changes.

### Build
1. `pricing/ec2-us-east-1.json`: a small hand-built map `{ "m5.large": 0.096, "c5.xlarge": 0.17, ... }` for ~10 instance types (on-demand Linux us-east-1; approximate is fine, note the source in a comment).
2. `model/CloudTrailEvent.java` — POJO with a static `parse(String json)` using Jackson. Extract: `eventTime` (as epoch millis), `eventName`, `accountId`, `team`, list of `(instanceId, instanceType)`.
3. `PricingLookup.java` — loads the JSON from the classpath into a `Map<String, Double>` and exposes `hourlyRate(instanceType)`.
4. `ResourceLifecycleFunction.java` — a `KeyedProcessFunction` keyed by `instanceId`:
   - `ValueState<RunningInstance>` holding `instanceType`, `accountId`, `team`, `startTime`.
   - On `RunInstances`: if state is empty, set it and emit `BurnRateUpdate(accountId, team, +hourlyRate, eventTime)`. If already set, ignore (duplicate).
   - On `TerminateInstances`: if state present, emit `BurnRateUpdate(..., -hourlyRate, ...)` and clear state. If absent, emit to a side output `unmatchedTerminations` (we saw a stop with no start — real systems hit this).
   - Set a state TTL of, say, 24 hours so state can't grow forever.
5. In `BurnRateJob`: parse → `flatMap` one event into N per-instance records → `keyBy(instanceId)` → `ResourceLifecycleFunction` → `.print()`.
6. `model/BurnRateUpdate.java` — POJO: `accountId`, `team`, `deltaHourlyRate`, `eventTime`. Keep it a proper Flink POJO (public fields or getters/setters, no-arg constructor) so serialization is efficient.

### Acceptance check
- Run generator; job prints `+0.096 account=… team=platform` on starts and negative deltas on terminations.
- Restart the generator from scratch (so it references instances Flink hasn't seen) and confirm unmatched terminations go to the side output (print the side output separately with a prefix).

### Learning checkpoint
- What "keyed state" is and why keying by `instanceId` means each instance's state lives in exactly one place.
- Why the function must be a `KeyedProcessFunction` and not a plain `map`.
- What state TTL protects against.
- Why Flink cares whether `BurnRateUpdate` is a "POJO" (serialization).

---

## Phase 3 — Event time, watermarks, windows, late data (≈90 min)

### Goal
Aggregate burn-rate deltas into a running per-account committed hourly rate, using **event time** with proper watermarks, and route late events to a side output instead of silently dropping them.

### Build
1. Assign timestamps and watermarks on the parsed `CloudTrailEvent` stream: `WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(20))` with a timestamp assigner using `eventTime`. Add `.withIdleness(Duration.ofSeconds(10))` so a quiet partition doesn't stall watermarks.
2. After `ResourceLifecycleFunction`, `keyBy(accountId)` → 1-minute **tumbling event-time window** → aggregate the sum of `deltaHourlyRate` in the window → emit `(accountId, windowEnd, netDeltaInWindow)`.
3. Then a second `KeyedProcessFunction` keyed by `accountId` holding `ValueState<Double> runningRate` that adds each window's net delta and emits `(accountId, windowEnd, runningCommittedHourlyRate)`. (Splitting the window aggregate and the running total keeps each piece easy to reason about.)
4. Late data: `.allowedLateness(Duration.ofSeconds(30))` and `.sideOutputLateData(lateTag)`. Print the late side output with a `LATE:` prefix.
5. Make the watermark bound and lateness configurable via env vars so the user can experiment.

### Acceptance check
- Job prints a running committed hourly rate per account that steps up and down as the generator runs.
- The Flink UI → job → operator → "Watermarks" tab shows watermarks advancing.
- With the generator's 60+ second late events, some `LATE:` lines appear. Reduce bounded-out-of-orderness to 2 seconds and see more late output; raise to 90 seconds and see none (but note the delay in results).

### Learning checkpoint
- What a watermark is, literally. What "bounded out-of-orderness of 20s" promises and what it costs.
- Why a window only fires when the watermark passes its end, and what that means for result latency.
- Difference between allowed lateness and the side output; when each fires.
- Why `withIdleness` was needed (partition skew stalls the minimum watermark).
- Why the running total was split into a separate operator rather than folded into the window.

---

## Phase 4 — Checkpointing and recovery (≈45 min)

### Goal
Turn on checkpointing, kill the TaskManager mid-stream, and prove the job resumes with correct state.

### Build
1. In `BurnRateJob`: `env.enableCheckpointing(10_000)`, exactly-once mode, checkpoint storage on a filesystem path mounted as a shared Docker volume across jobmanager and taskmanager (e.g. `/tmp/flink-checkpoints`). Set `execution.checkpointing.min-pause` to a few seconds. Use the default HashMap state backend; mention RocksDB in the README as the production choice.
2. Enable restart strategy: fixed-delay, 3 attempts, 5s delay.
3. Write `docs/recovery-demo.md` with the exact steps: start job, run generator, note current running rate for one account, `docker compose kill taskmanager`, `docker compose up -d taskmanager`, watch job restart from the last checkpoint, confirm the running rate continues from where it was rather than resetting to 0.

### Acceptance check
- Perform the demo. Paste the log lines showing the checkpoint ID the job restored from, and the running-rate line before and after the kill.

### Learning checkpoint
- What a checkpoint contains and how Flink coordinates it across operators (barriers) without stopping the stream.
- What "exactly-once" actually guarantees here, and where it doesn't extend (the `print()` sink is not transactional).
- Why HashMap vs RocksDB state backend is a real production decision.
- What would be lost if checkpointing were off.

---

## Phase 5 (stretch) — Broadcast state for live pricing updates (≈45 min)

Only if time remains. This is the highest-value "advanced" concept in the project.

### Build
1. Second Kafka topic `pricing-updates` with messages `{ "instanceType": "m5.large", "hourlyRate": 0.10 }`.
2. Replace `PricingLookup` with a `BroadcastProcessFunction`: the lifecycle stream is the keyed side, the pricing stream is the broadcast side, `MapState<String, Double>` as the broadcast state initialized from the JSON file.
3. Produce a price change via `rpk` and show subsequent `RunInstances` using the new rate.

### Learning checkpoint
- Why broadcast state is the right pattern for "small, slowly changing reference data joined against a fast stream," and why a regular join or an external lookup would be worse.

---

## Phase 6 — Test, CI, README (≈45 min)

### Build
1. One JUnit test using `MiniClusterWithClientResource` (or the Flink test harness for `KeyedProcessFunction`) that feeds a `RunInstances` then `TerminateInstances` for the same instance and asserts a `+rate` then `-rate` update.
2. `.github/workflows/ci.yml`: Java 17 setup, `mvn -B package`, on push and PR.
3. `README.md` with these sections, in this order:
   - One-sentence pitch.
   - **The problem:** CUR arrives hours later; CloudTrail arrives in seconds; this job estimates committed hourly spend in real time from lifecycle events. Two short paragraphs, plain language.
   - Architecture diagram (a Mermaid block is fine: generator → Redpanda → Flink [parse → lifecycle state → window → running total] → stdout).
   - **Run it:** three commands.
   - **What you'll see:** sample output lines, and a screenshot of the Flink UI.
   - **Design decisions:** event time not processing time; bounded out-of-orderness of 20s and why; keyed state by instanceId; state TTL; checkpointing to filesystem; HashMap backend.
   - **Recovery demo** (link to `docs/recovery-demo.md`).
   - **Not here / next steps:** reconciliation against a real CUR file, Savings Plans and RI math, EBS and other services, RocksDB backend, a real sink. Be honest and specific.
4. Add GitHub topics: `apache-flink`, `stream-processing`, `finops`, `cloudtrail`, `java`.

### Acceptance check
- `mvn test` green locally. CI badge in README.
- README run instructions work from a clean clone.

### Learning checkpoint (interview-framing pass)
The user will ask the model to articulate, for each design decision in the README, the one-sentence answer they'd give an interviewer and the tradeoff they accepted. This is the interview prep pass.
