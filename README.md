# flink-cloud-burn-rate

Real-time cloud spend rate from CloudTrail resource-lifecycle events, built on Apache Flink.

## Phase 0: skeleton

Build the job jar (uses a throwaway Maven container since no local JDK/Maven is required on the host):

```bash
docker run --rm -v "$PWD/flink-job":/app -v maven-repo:/root/.m2 -w /app maven:3.9-eclipse-temurin-17 mvn -B clean package
```

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
