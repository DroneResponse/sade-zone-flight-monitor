# Flight Monitor — Implementation-Side Contract

**Last Updated On:** 2026-05-07

This document covers what **this Flight Monitor service** implements: the
HTTP endpoints it exposes to SADE, the outbound POST it makes to SADE, the
retry/idempotency behaviour, and the background sweeper that closes the
loop on sessions SADE never finalises.

For the **payload-level contract** (exact JSON shapes for register-session,
exit-request, and tracker-session-finalized), the authoritative reference
is [SADE_CONTRACT.md](./SADE_CONTRACT.md). That file is owned by the SADE
side and is the source of truth when the two diverge.

> **Note**: an earlier version of this file (dated 2026-04-20) duplicated
> the payload examples from `SADE_CONTRACT.md` and described the older
> finalize shape with `actual_start_time` / `battery_start_pct` / no
> `events` array. That shape is no longer used. The current finalize
> payload uses the events-based shape documented in `SADE_CONTRACT.md`
> (2026-04-22). This doc has been rewritten to complement, not duplicate,
> that one.

---

## Endpoints exposed by the Flight Monitor

All three are served by the FastAPI app in [app/api/server.py](../app/api/server.py)
on port 8000 (override with `API_PORT`). Authentication is **not yet
implemented** — see "Open security gap" below.

### `POST /flight-monitor/register-session`

Receives an approved-session command from the SADE outbox. Registration
implicitly means the session is approved; there is no separate decision
field.

| Status | Meaning |
|---|---|
| `202 Accepted` | Session registered, telemetry tracking active |
| `409 Conflict` | The drone already has an active session, registration rejected |

Payload shape: see `SADE_CONTRACT.md` § *Register Session*.

When the payload includes a non-null `test_overrides` object, the real
MQTT telemetry path is bypassed and a background task POSTs a stub
finalization 5 s after registration. Used for stub testing without a real
drone.

### `POST /flight-monitor/exit-request`

Notifies the Flight Monitor that a drone has left the zone early or the
session needs to close out. SADE is the authoritative source for "this
session should close"; the Flight Monitor only watches what was approved.

| Status | Meaning |
|---|---|
| `202 Accepted` | Session found, grace period started |
| `404 Not Found` | No active session for `flight_session_id` (already finalized or never registered); safe to retry |

Payload shape: see `SADE_CONTRACT.md` § *Exit Request*.

The session stays active during the grace period — MQTT telemetry continues
to be accepted and tracked. After 5 minutes of telemetry silence (measured
from the last observed `last_seen` change, not from when the exit-request
arrived), the session is finalized.

### `GET /health`

Returns `{status, active_sessions, sessions_past_deadline, sessions_stranded}`.
The two flag counters are populated by the periodic sweeper (see below).
Existing consumers that only read `status` or `active_sessions` are not
affected by the new fields.

---

## Outbound: `POST /tracker-session-finalized`

The Flight Monitor sends one finalization report per session to the URL
configured by `TRACKER_FINALIZED_URL`. The pipeline refuses to start if
that env var is unset while `FINALIZE_TO_API=true`.

Payload shape: see `SADE_CONTRACT.md` § *Tracker Session Finalized*. All
trigger cases below converge on the same shape — `build_finalization_payload`
in [app/sending/tracker_finalizer.py](../app/sending/tracker_finalizer.py)
is the one source of truth, run from a real `DroneState` (when telemetry
was observed) or a synthetic minimal one (when it wasn't).

### Trigger cases

| Case | Trigger | Source |
|---|---|---|
| **Exit-request grace** | `POST /flight-monitor/exit-request` followed by 5 min of telemetry silence (measured from the last `last_seen` change) | SADE-driven |
| **Stub finalization** | Registration with non-null `test_overrides` — fires 5 s after registration | dev/test |
| **Force-close backstop** | Session has been carrying a deadline-breach or stranded flag for 24 h with no exit-request | sweeper |

Terminal MQTT statuses (`mission_completed` / `complete` / `done` / …) do
**not** trigger this POST. They write a local CSV row for diagnostics and
log "awaiting SADE exit-request or sweeper", but the session stays alive
until SADE drives it out — drones cannot self-close their session.

### Retry semantics

`post_tracker_session_finalized` retries up to 2 times with `[1s, 2s]`
exponential backoff. 5xx responses and network/transport errors are
retried; 4xx responses are not (the payload is invalid — retrying won't
change the outcome). Business-level `FAILED` responses are logged but
not retried.

SADE deduplicates on `flight_session_id`, so retrying the same payload is
safe.

---

## Periodic session sweeper

A single background coroutine started in the FastAPI lifespan handler runs
every `SWEEPER_INTERVAL_SECONDS` (60 s in production) and does three checks
per tick over the active session set:

| Check | Trigger | Action |
|---|---|---|
| Deadline-breach flag | `now > requested_exit_time` and `exit_requested_at is None` and not already flagged | Stamp `exit_deadline_breached_at`; log WARNING; bump `/health.sessions_past_deadline` |
| Stranded flag | DroneState exists, telemetry silent > 10 min, `exit_requested_at is None`, not already flagged | Stamp `stranded_flagged_at`; log WARNING; bump `/health.sessions_stranded` |
| Force-close backstop | Either flag has been on the session for > 24 h with `exit_requested_at` still None | Run the canonical finalize sequence (POST + clear); log WARNING with the originating flag |

Both flag fields are one-shot edge detectors — once stamped, the session
is not re-flagged on subsequent sweeps. A session can carry both flags;
the force-close timer starts from whichever fired first.

Thresholds are tunable in [app/api/server.py](../app/api/server.py):

```python
SWEEPER_INTERVAL_SECONDS = 60.0
STRANDED_SILENCE_THRESHOLD_SECONDS = 600.0     # 10 min
FORCE_CLOSE_THRESHOLD_SECONDS = 86400.0        # 24 h
```

Adjust them with their corresponding `TEST_*_SECONDS` overrides in the
integration test if you change them.

---

## Open security gap

**No authentication on the webhook endpoints.** Any caller that can reach
port 8000 can register or close out a session. For production this needs
at minimum a shared-secret header check; ideally mTLS or IAM-signed
requests depending on the deployment shape. Tracked as item #2 in the
priority list — see the README's design-decisions section for context.

---

## v1 invariants (still current)

- Outbound registration and exit-intent delivery from SADE are asynchronous
  through the SADE outbox.
- Inbound `/tracker-session-finalized` from the Flight Monitor to SADE is
  synchronous HTTP with bounded retries.
- `flight_session_id` is the only required cross-system correlation key.
- The Flight Monitor is authoritative for actual start/end timing in the
  finalization payload (via the `events` array's `FLIGHT_SEGMENT` entries).
- SADE never sends a separate stop/deregister command — the only ways a
  session leaves the registry are:
  1. SADE sends `/flight-monitor/exit-request` → grace period elapses → finalize
  2. Registration includes `test_overrides` → stub finalize 5 s later
  3. Sweeper force-closes after 24 h of unattended flag

---

## Cross-references

- [SADE_CONTRACT.md](./SADE_CONTRACT.md) — authoritative payload shapes
- [../README.md](../README.md) — operational details, configuration, deployment
- [../docs/EXIT_POLICY_DESIGN.md](../docs/EXIT_POLICY_DESIGN.md) — design narrative for the exit-policy rework
- [REFERENCE_TABLES.md](./REFERENCE_TABLES.md) — event types, incident codes
- [IDEMPOTENCY_RECOMMENDATIONS.md](./IDEMPOTENCY_RECOMMENDATIONS.md) — idempotency conventions
