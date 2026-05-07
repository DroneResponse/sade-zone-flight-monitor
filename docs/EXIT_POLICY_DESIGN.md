# Flight Monitor — Data Flow & Exit Policy

## 1. Current system

### Entry points
- **SADE → `POST /flight-monitor/register-session`** — `approval_handler.py` → `ActiveSessionRegistry`.
- **Drone → MQTT `status_message` / `update_drone`** — `mqtt_client.py` (subscribes to both) → `workers.py` → `DroneStateTracker`.
- **SADE → `POST /flight-monitor/exit-request`** — `exit_handler.py` → stamps exit intent onto `DroneState`.

### Tracking (in-memory only)
- **`ActiveSessionRegistry`** — session metadata: `flight_session_id`, `drone_id`, `requested_exit_time_utc`, `test_overrides`, etc.
- **`DroneStateTracker`** — live per-session accumulators: altitude min/max, voltage in/out, `distance_flown_m`, `first_seen`/`last_seen`, `exit_requested_at`, `exit_reason`.

### Exit points (all POST to SADE `/tracker-session-finalized`)

| Case | Trigger | Source |
|---|---|---|
| **A1** | Terminal MQTT status (`mission_completed`) | Drone self-declares done via telemetry |
| **A2** | Exit-request received + 5 min telemetry silence | SADE exit + grace period |
| **A3** | Same as A2 but no telemetry was ever received | Synthetic minimal `DroneState` cretaed and then finalize |
| **A4** | Stub mode (`test_overrides` present) — fires 5 s after registration | Dev/local testing |

Case A1 also writes the CSV mission row.

### Exit policy gaps
- **No deadline enforcement** — `requested_exit_time_utc` is ignored.
- **No cleanup for silent-SADE sessions** — if no terminal MQTT arrives and no exit-request is sent, the session leaks in memory until process restart. This is intentional for now in the development period, but needs to be changed for when it is activley running on AWS.
- **Finalization authority is split** — the drone (via terminal MQTT) can close its own session, even though SADE is the system of record (I think it would be better if SADE is the final decision maker on closing a drones session...)

---

## 2. New plan (based on Friday meeting with everyone today)

### Policy
1. Every registered session is **expected to receive a SADE exit-request**.
2. If no exit-request arrives, the Flight Monitor **keeps ingesting telemetry indefinitely** until a different trigger (that we will chooes, need to discuss) — no drone-driven auto-close.
3. When `requested_exit_time_utc` passes without an exit-request, the session is **flagged** (warning logged, exposed on `/health`, recorded on the registry entry).
4. IMPORTANT: THIS IS THE PART OF THE POLICY THAT WILL REMOVE THE MEMORY LEAK. If the breach persists for ______________ (ex: **24 h past deadline**) with no exit-request, the Flight Monitor **force-finalizes** as a memory-safety backstop.
5. **SADE owns finalization authority** — terminal MQTT messages are informational only and are used to populate the events list that is sent in the finalized mission summary.

### Detection mechanism
- Single **periodic sweeper** coroutine (every ~60 s) iterates the active session registry:
  - `now > requested_exit_time_utc` AND `exit_requested_at is None` AND `exit_deadline_breached_at is None` → stamp breach fields, `LOGGER.warning`, increment `/health` counter.
  - `now > requested_exit_time_utc + 24h` AND still no exit-request → invoke the finalize path with a deadline-breach indicator in the payload.
- No per-session scheduled tasks — one sweeper, idempotent, restart-survivable by construction.

### New exit points

| Case | Trigger | Notes |
|---|---|---|
| **B1** | Exit-request + 5 min telemetry silence | Replaces A2 / A3 unchanged |
| **B2** | Stub mode (`test_overrides`) — 5 s | Replaces A4 unchanged |
| **B3** | Deadline + 24 h auto-finalize | **New** — force-close backstop |

**Removed:** Case A1 (terminal MQTT status finalize). Terminal telemetry still writes the CSV row for local tetsing but does **not** POST to SADE in the actual active system.

### Storage additions
- `ActiveFlightSession.exit_deadline_breached_at: str | None`
- `ActiveFlightSession.exit_deadline_breach_reason: str | None`
- `GET /health` gains `sessions_past_deadline: int`

### Final Notes / Things to think about
- **Enforcment of `INCIDENT 1111-001`** ("Drone did not exit zone") in the finalization payload. Automatically stamp it to every drone who did not send a goodbye before the time they said in their entry request? The incident code doc notes it applies "following subsequent investigation," so question is if attribution is deferred to a SADE-side workflow later on. The breach is still visible in logs and on `/health`.
- **No force-close before 24 h past deadline.** The policy is flag-and-observe, not punish.

---

## 3. Explicit Differences in Current vs. New

| Concern | Current | New |
|---|---|---|
| Finalize authority | Drone (terminal MQTT) + SADE (exit-request) + stub timer | SADE (exit-request) + 24 h safety backstop |
| Terminal MQTT behavior | Triggers SADE finalize + CSV write | CSV write only; no finalize |
| Session with no exit-request | Leaks in memory until process restart | Flagged at deadline; auto-closed 24 h past deadline |
| Deadline enforcement | None | Periodic sweeper; warning + `/health` counter + registry flag |
| Detection mechanism | N/A | Single sweeper coroutine, every 60 s |
| `requested_exit_time_utc` | Ignored | Load-bearing — drives all deadline logic |
| Memory-leak ceiling | None | Hard 24 h past stated deadline |
| INCIDENT emission on breach | N/A | Deliberately not auto-emitted — attribution belongs to SADE-side humans |
| Registry fields added | — | `exit_deadline_breached_at`, `exit_deadline_breach_reason` |
| `/health` | `active_sessions` | + `sessions_past_deadline` |
