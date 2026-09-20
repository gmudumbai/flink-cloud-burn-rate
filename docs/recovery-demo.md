# Recovery demo

Proves the job resumes from its last checkpoint after a TaskManager crash,
instead of losing all keyed state (as it did back in Phase 0/2 when we
killed things without checkpointing enabled).

## Steps

1. Rebuild and start the stack:

   ```bash
   docker run --rm -v "$PWD":/workspace -v maven-repo:/root/.m2 -w /workspace/flink-job maven:3.9-eclipse-temurin-17 mvn -B clean package
   docker compose up -d
   docker compose exec redpanda rpk topic create cloudtrail-events
   ```

2. Restart the Flink containers once (see the Phase 2 note on `mvn clean`
   breaking the bind mount), then submit the job and note its JobID:

   ```bash
   docker compose restart jobmanager taskmanager
   docker compose exec jobmanager flink run -d /opt/flink/usrlib/flink-cloud-burn-rate.jar
   ```

3. Start the generator and let it run for a few minutes so at least one
   checkpoint completes (every 10s) and at least one 1-minute window fires:

   ```bash
   python generator/generate_events.py --duration 180
   ```

4. Note the current running rate for one account from the logs:

   ```bash
   docker compose logs taskmanager | grep 'account=100000000000' | tail -1
   ```

5. Confirm a checkpoint has completed:

   ```bash
   curl -s http://localhost:8082/jobs/<jobId>/checkpoints | python3 -m json.tool
   ```

6. Kill the TaskManager mid-stream:

   ```bash
   docker compose kill taskmanager
   ```

7. Bring it back:

   ```bash
   docker compose up -d taskmanager
   ```

8. Watch the job restart (Flink's fixed-delay restart strategy: 3 attempts,
   5s delay) and resume:

   ```bash
   docker compose logs -f taskmanager
   ```

9. Confirm the running rate for the same account **continues from where it
   was** (plus whatever the generator sent in the meantime) rather than
   resetting to 0 — this is the actual proof that checkpointed state
   survived the crash.

## What to paste as the acceptance check

- The checkpoint ID/path the job restored from (visible in the JobManager
  log around `Restoring job ... from Checkpoint`).
- The running-rate log line for one account immediately before the kill.
- The running-rate log line for the same account shortly after recovery,
  showing it picked up from the pre-kill value rather than 0.
