# flink-cloud-burn-rate

Real-time cloud spend rate from CloudTrail resource-lifecycle events, built on Apache Flink.

## Phase 0: skeleton

Build the job jar (uses a throwaway Maven container since no local JDK/Maven is required on the host):

```bash
docker run --rm -v "$PWD":/workspace -v maven-repo:/root/.m2 -w /workspace/flink-job maven:3.9-eclipse-temurin-17 mvn -B clean package
```

(The whole project is mounted, not just `flink-job/`, because the build pulls `pricing/ec2-us-east-1.json` onto the classpath from the sibling `pricing/` directory — see `flink-job/pom.xml`'s `<resources>`.)

Start the cluster:

```bash
docker compose up -d
```

Check the Flink UI at http://localhost:8081 — it should show 1 TaskManager with 4 slots.

Create the topic before the first submit (Flink's `KafkaSource` describes the
topic on startup, which does not trigger redpanda's auto-create — only a
produce does):

```bash
docker compose exec redpanda rpk topic create cloudtrail-events
```

Submit the job:

```bash
docker compose exec jobmanager flink run /opt/flink/usrlib/flink-cloud-burn-rate.jar
```

Produce a test message:

```bash
docker compose exec redpanda rpk topic produce cloudtrail-events
```

Type a line and press Enter, then Ctrl+D. Watch it show up in:

```bash
docker compose logs taskmanager
```

## Phase 1: event generator

Streams CloudTrail-shaped `RunInstances`/`TerminateInstances` events into the
topic, with ~10% of events backdated 5-20s and ~2% backdated 60-179s to
simulate real out-of-order/late delivery.

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r generator/requirements.txt
python generator/generate_events.py --duration 30
```

Deviation from the plan: `kafka-python` (the plan's suggested library) does
not import on current Python (3.12+) due to an unmaintained `six` vendoring
bug. Used the drop-in community fork `kafka-python-ng` instead.

Watch events arrive in Flink with `docker compose logs -f taskmanager`.

## Phase 2: keyed state and burn-rate updates

The job now parses each CloudTrail event, flattens it into one record per
instance, keys by `instanceId`, and tracks running/stopped state in
`ResourceLifecycleFunction`. Rebuild and resubmit as above.

Deviations from the plan hit along the way:
- **Jackson version clash at runtime.** `flink-connector-kafka` transitively
  pulls `jackson-core`/`jackson-annotations` 2.15.2, while our own
  `jackson-databind` was 2.17.2. Once shaded into one jar this caused a
  `NoSuchMethodError` at job startup. Fixed by explicitly pinning
  `jackson-core` and `jackson-annotations` to the same version as
  `jackson-databind` in `pom.xml`.
- **`mvn clean` breaks the live bind mount.** If the Flink containers are
  already running and you `mvn clean package` (which deletes and recreates
  `flink-job/target`), the `./flink-job/target` bind mount inside the
  containers goes stale — `flink run` reports the jar doesn't exist even
  though it's on the host. Fix: `docker compose restart jobmanager
  taskmanager` after any `mvn clean` while the stack is already up.

To see the `unmatchedTerminations` side output fire, cancel and resubmit the
Flink job (`flink cancel <jobId>` then `flink run -d ...`) while the
generator is still running — the fresh job has no memory of instances
started before the restart, so terminations for them are routed to
`UNMATCHED_TERMINATION` lines instead of guessing a rate to subtract. This
is a preview of why Phase 4's checkpointing matters.
