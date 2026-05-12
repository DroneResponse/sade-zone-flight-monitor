# Message Shapes — Flight Monitor I/O

Single reference for every message that crosses the Flight Monitor's
process boundary: what comes in (HTTP webhooks from SADE, MQTT telemetry
from drones) and what goes out (the finalization POST back to SADE).

The Pydantic models in `app/api/` are the source of truth for the
inbound HTTP shapes; the MQTT parser in `app/ingestion/workers.py` and
the payload builder in `app/sending/tracker_finalizer.py` are the source
of truth for the MQTT-in and HTTP-out shapes. This doc summarises them
together — when the code diverges from what's written here, the code
wins.

---

## 1. Inbound HTTP — webhooks from SADE

All three endpoints accept `Content-Type: application/json`. Validation
is Pydantic; unknown fields are accepted and ignored. When inbound mTLS
is configured, the client must present a cert signed by
`API_CA_CERT_PATH` — see [README.md](../README.md) for the env-var
contract.

### `POST /flight-monitor/register-session`

Pydantic model: `RegisterSessionPayload` in
[app/api/approval_handler.py](../app/api/approval_handler.py).

```jsonc
{
  "flight_session_id": "550e8400-e29b-41d4-a716-446655440000",  // REQUIRED — only required field
  "pilot_id":          "pilot-abc",                              // optional
  "drone_id":          "Orange",                                 // optional, but recommended (registry correlation)
  "organization_id":   "org-xyz",                                // optional
  "sade_zone_id":      "zone-123",                               // optional — surfaced in /dashboard
  "requested_entry_time": "2026-05-12T17:00:00+00:00",          // optional, ISO 8601
  "requested_exit_time":  "2026-05-12T18:00:00+00:00",          // optional, ISO 8601 — fed to deadline-breach sweeper
  "requested_operation": {                                       // optional, opaque to the Flight Monitor
    "operation_type": "INSPECTION",
    "priority":       "NORMAL"
  },
  "test_overrides": {                                            // optional — see §test_overrides below
    "telemetry_summary": { ... },
    "events":            [ ... ]
  },
  "submitted_at": "2026-05-12T16:59:58+00:00"                   // optional, ISO 8601
}
```

**Responses:**

| HTTP | `action` | Meaning |
|---|---|---|
| `202 Accepted` | `registered` | Session activated; telemetry tracking begins immediately |
| `409 Conflict` | `rejected` | The drone already has an active session |

```jsonc
// 202 body
{ "action": "registered", "flight_session_id": "...", "drone_id": "..." }

// 409 body
{ "action": "rejected", "reason": "drone already has active session", "flight_session_id": "..." }
```

**`test_overrides` semantics.** When non-null, the MQTT telemetry path is
bypassed entirely. A background task waits 5 s, builds a canned
finalization payload from the overrides (filling missing fields with
safe defaults), POSTs it to `/tracker-session-finalized`, and marks the
session complete. Shape inside `test_overrides`:

```jsonc
{
  "telemetry_summary": {
    "altitude_min_m":   12.0,
    "altitude_max_m":   95.0,
    "distance_flown_m": 1450.0
  },
  "events": [
    {
      "type":         "FLIGHT_SEGMENT",
      "time_in_utc":  "2026-05-12T17:01:00Z",
      "time_out_utc": "2026-05-12T17:35:00Z",
      "battery_state_in":  { "system_charge_pct": null, "slots": [{ "slot_id": "A", "voltage_v": 16.8 }] },
      "battery_state_out": { "system_charge_pct": null, "slots": [{ "slot_id": "A", "voltage_v": 14.2 }] }
    }
  ]
}
```

### `POST /flight-monitor/exit-request`

Pydantic model: `ExitRequestPayload` in
[app/api/exit_handler.py](../app/api/exit_handler.py).

```jsonc
{
  "flight_session_id": "550e8400-e29b-41d4-a716-446655440000",  // REQUIRED
  "reason":       "drone_left_early",                            // optional; primary expected value
  "requested_at": "2026-05-12T17:42:00+00:00"                   // optional, ISO 8601; defaults to server-side now()
}
```

**Responses:**

| HTTP | `action` | Meaning |
|---|---|---|
| `202 Accepted` | `accepted` | Session found; grace period begins. Telemetry keeps flowing during the grace window. |
| `404 Not Found` | `not_found` | No active session for this `flight_session_id`. Safe to retry. |

```jsonc
// 202 body
{
  "action":            "accepted",
  "flight_session_id": "...",
  "drone_id":          "...",
  "message":           "Exit request accepted. Finalization report will be sent to SADE after grace period."
}

// 404 body
{
  "action":            "not_found",
  "flight_session_id": "...",
  "message":           "No active session found for this flight_session_id."
}
```

### `GET /health`

No request body.

```jsonc
{
  "status":                  "ok",
  "active_sessions":         3,
  "sessions_past_deadline":  0,  // bumped by the periodic sweeper when requested_exit_time has passed without an exit-request
  "sessions_stranded":       0   // bumped by the sweeper when telemetry has been silent >10 min without an exit-request
}
```

### `GET /dashboard` / `GET /dashboard/data`

`/dashboard` returns HTML; `/dashboard/data` returns the JSON snapshot
the page polls every 7 s. Shape of `/dashboard/data`:

```jsonc
{
  "report_time_utc": "2026-05-12T17:36:04.066597+00:00",
  "thresholds": {
    "stranded_silence_seconds":     600.0,
    "force_close_threshold_seconds": 86400.0
  },
  "totals": { "active_sessions": 1, "sessions_past_deadline": 0, "sessions_stranded": 0 },
  "zones": [
    {
      "sade_zone_id": "zone-123",
      "sessions": [
        {
          "flight_session_id":         "...",
          "drone_id":                  "Orange",
          "pilot_id":                  "pilot-abc",
          "registered_at":             "2026-05-12T17:00:00+00:00",
          "requested_entry_time":      "2026-05-12T17:00:00+00:00",
          "requested_exit_time":       "2026-05-12T18:00:00+00:00",
          "exit_requested_at":         null,
          "exit_deadline_breached_at": null,
          "stranded_flagged_at":       null,
          "status":                    "FLYING",                       // EXIT_REQUESTED | FLYING | LANDED | WAITING | ACTIVE
          "flags":                     [],                             // ["past_deadline", "stranded", ...]
          "live": {
            "has_telemetry":            true,
            "last_seen":                "2026-05-12T17:30:00+00:00",
            "seconds_since_last_seen":  4.2,
            "altitude_m":               92.5,
            "...":                      "..."
          }
        }
      ]
    }
  ]
}
```

---

## 2. Inbound MQTT — drone telemetry

The Flight Monitor subscribes to a comma-separated topic list — default
`status_message,update_drone` (both names are in active use across the
fleet). Each message is a JSON object published with QoS 0. The parser
that pulls structured fields out is `parse_queue_message` in
[app/ingestion/workers.py](../app/ingestion/workers.py).

### On-the-wire JSON

Real captured shape (from
[docs/actual_drone_update_message_format.txt](actual_drone_update_message_format.txt)):

```jsonc
{
  "uavid": "Orange",                            // drone_id (registry correlation key)
  "status": {
    "status":          "STANDBY",               // mission_status fallback when top-level "mission_status" is absent
    "mode":            "LOITER",
    "onboard_pilot":   "ReceiveMission",        // not consumed
    "state_type":      "ReceiveMission",        // not consumed
    "speed":           0.01,                    // not consumed (candidate for GPS-jitter filtering)
    "location": {                               // → position, altitude min/max, distance_flown_m (haversine)
      "latitude":  41.7547272,
      "longitude": -86.191418,
      "altitude":  254.19506772412188
    },
    "armed": false,                             // AUTHORITATIVE arm state — drives FLIGHT_SEGMENT detection
    "battery": {
      "voltage": 16.200000762939453,            // → voltage_in / voltage_out → per-segment slots[].voltage_v
      "current": 1.0,                           // not consumed
      "level":   1.0                            // not consumed (candidate for system_charge_pct once units confirmed)
    },
    "geofence":          false,                 // not consumed (candidate INCIDENT signal)
    "heartbeat_status":  "CONTINUE",            // not consumed
    "drone_attitude":    { "x": ..., "y": ..., "z": ..., "w": ... },  // not consumed
    "drone_heading":     97.31,                 // not consumed
    "gimbal_attitude":   null,                  // not consumed
    "gimbal_heading":    null,                  // not consumed
    "air_lease_state":   "IDLE"                 // not consumed
  },
  "timestamp": "2026-04-24T21:03:42.105+00:00"  // not consumed — Flight Monitor stamps its own first_seen / last_seen
}
```

### Fields the worker extracts

| Source field (try in order) | Extracted as | Used for |
|---|---|---|
| `drone_id` / `droneId` / `uavid` / `uavID` / `uav_id` | `drone_id` | Registry lookup; drops the message when missing |
| `mission_status` then `status.status` | `mission_status` | Local CSV diagnostic; **does NOT close the session** |
| `mode` then `status.mode` | `mode` | CSV `entry_conditions` column |
| `status.location.{latitude,longitude,altitude}` (with `lat`/`lon`/`lng`/`alt` aliases) | `position` | `start_position`, altitude min/max, `distance_flown_m` (haversine accumulator) |
| `status.armed` (must be JSON bool) | `armed` | FLIGHT_SEGMENT detection — `false/None → true` opens a segment, `true → false` closes it |
| `status.battery.voltage` | `voltage` | `voltage_in` (first reading), `voltage_out` (most recent during open segment) |

Any field not in this table is observed and ignored. A message with no
recognised `drone_id` is dropped with a WARNING log.

### Terminal mission statuses

`mission_status` ∈ {`complete`, `completed`, `done`, `finished`,
`mission_complete`, `mission_completed`, `mission_finished`} writes a
local CSV row for diagnostics but **does not close the session** — SADE
owns finalization. The session stays in the registry until SADE sends an
exit-request, a 24 h force-close fires, or a `test_overrides` stub
finalize completes. See
[FLIGHT_MONITOR_CONTRACT.md](../SADE_AWS_API_INFORMATION/FLIGHT_MONITOR_CONTRACT.md)
for the three exit paths.

---

## 3. Outbound HTTP — `POST /tracker-session-finalized`

Target URL: `TRACKER_FINALIZED_URL` env var (required when
`FINALIZE_TO_API=true`). When the URL is `https://` and
`API_SERVER_CERT_PATH` + `API_SERVER_KEY_PATH` are set, the same systems
identity used for inbound mTLS is presented as the client cert; SADE's
server cert is verified against `TRACKER_CA_CERT_PATH` (or the system
trust store if unset).

Built by `build_finalization_payload` in
[app/sending/tracker_finalizer.py](../app/sending/tracker_finalizer.py)
for the real-telemetry path and `build_stub_finalization_payload` for
the `test_overrides` path — both converge on the same shape.

### Payload shape

```jsonc
{
  "flight_session_id": "550e8400-e29b-41d4-a716-446655440000",
  "report_time_utc":   "2026-05-12T17:42:00Z",                // when this report was generated, "YYYY-MM-DDTHH:MM:SSZ"

  "telemetry_summary": {                                       // session-wide aggregates
    "altitude_min_m":   12.0,
    "altitude_max_m":   95.0,
    "distance_flown_m": 1450.0                                 // haversine across all GPS fixes
  },

  "events": [
    // One FLIGHT_SEGMENT per arm/disarm window observed via status.armed.
    // Legacy firmware (no status.armed): one synthetic segment first_seen → last_seen.
    {
      "type":         "FLIGHT_SEGMENT",
      "time_in_utc":  "2026-05-12T17:01:00Z",                  // status.armed False/None → True
      "time_out_utc": "2026-05-12T17:35:00Z",                  // status.armed True → False (auto-closed at last_seen if still open at finalize time)
      "battery_state_in": {
        "system_charge_pct": null,                             // SADE-confirmed "unknown" signal; flips to a real value once firmware emits percentage
        "slots": [ { "slot_id": "A", "voltage_v": 16.8 } ]     // always single-slot until firmware emits per-slot voltages
      },
      "battery_state_out": {
        "system_charge_pct": null,
        "slots": [ { "slot_id": "A", "voltage_v": 14.2 } ]
      }
    },
    // Zero or one EXIT_REQUEST event: present only when SADE sent /flight-monitor/exit-request before finalize.
    {
      "type":     "EXIT_REQUEST",
      "time_utc": "2026-05-12T17:35:30Z",
      "reason":   "drone_left_early"                           // "unspecified" if SADE's exit-request omitted reason
    }
    // INCIDENT events are defined in REFERENCE_TABLES.md but NOT emitted yet — blocked on signal-to-code mapping.
  ]
}
```

### Trigger cases (all converge on the same shape)

| Case | Trigger | Payload source |
|---|---|---|
| Exit-grace | `POST /flight-monitor/exit-request` followed by 5 min of telemetry silence | Real `DroneState` |
| Stub finalization | Registration with non-null `test_overrides` — fires 5 s after registration | `test_overrides` (synthetic defaults fill gaps) |
| No-telemetry-ever | Exit-grace fires but no MQTT message ever arrived | Synthetic minimal `DroneState` (`first_seen` = `requested_entry_time` or now) |
| Force-close backstop | Session carried a sweeper flag for >24 h with no exit-request | Real `DroneState` if any, else synthetic |

### Retry & idempotency

Up to **2 retries** with backoff `[1s, 2s]`. Retried on 5xx and
network/transport errors. **Not** retried on 4xx (payload invalid —
retrying won't change the outcome) or on business-level
`status: FAILED` (SADE accepted the call but couldn't finalize, e.g.
unknown session — retrying won't help either).

Retrying is safe because SADE deduplicates on `flight_session_id`. See
[FLIGHT_MONITOR_CONTRACT.md](../SADE_AWS_API_INFORMATION/FLIGHT_MONITOR_CONTRACT.md)
for the full retry semantics and
[SADE_CONTRACT.md](../SADE_AWS_API_INFORMATION/SADE_CONTRACT.md) for
SADE's response shape.

---

## Cross-references

- [SADE_CONTRACT.md](../SADE_AWS_API_INFORMATION/SADE_CONTRACT.md) — authoritative payload shapes (SADE-owned)
- [FLIGHT_MONITOR_CONTRACT.md](../SADE_AWS_API_INFORMATION/FLIGHT_MONITOR_CONTRACT.md) — implementation-side contract (status codes, retry, sweeper)
- [REFERENCE_TABLES.md](../SADE_AWS_API_INFORMATION/REFERENCE_TABLES.md) — event types, incident codes
- [actual_drone_update_message_format.txt](actual_drone_update_message_format.txt) — raw telemetry samples and field-by-field mapping notes
- [README.md](../README.md) — env vars, mTLS config, deployment
