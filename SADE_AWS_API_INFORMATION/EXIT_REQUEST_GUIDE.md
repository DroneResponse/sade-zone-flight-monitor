# Flight Monitor Exit Request

**Last Updated:** 2026-04-21

## What this does

When a drone leaves a SADE zone early (before its authorized window ends), the cloud system can notify the Flight Monitor by sending an HTTP POST to `/flight-monitor/exit-request`. The Flight Monitor will:

1. Continue accepting MQTT telemetry for the drone during a **5-minute grace period**
2. Monitor for telemetry silence — if no new messages arrive for 5 minutes, the drone is considered gone
3. Build a finalization report from all telemetry accumulated during the flight (including the grace period)
4. POST the finalization report back to SADE at `/tracker-session-finalized`
5. Clean up the session

The grace period ensures the Flight Monitor captures the drone's final telemetry as it departs the zone, rather than cutting off data collection immediately.

If the drone keeps transmitting after the exit request, the Flight Monitor logs a warning every 30 seconds until the drone goes silent. The 5-minute silence countdown only begins once telemetry actually stops.

If no telemetry was received at all before the grace period expires, the Flight Monitor still sends a finalization callback with an empty telemetry summary so SADE can close out the session.

## Endpoint

```
POST /flight-monitor/exit-request
```

## Request format

```json
{
  "flight_session_id": "8c189f91-0348-4a15-a6f0-23377dca7834",
  "reason": "drone_left_early",
  "requested_at": "2026-04-21T19:00:00Z"
}
```

| Field | Type | Required | Description |
|---|---|---|---|
| `flight_session_id` | string | **Yes** | The session ID from the original `register-session` call. This is the only cross-system correlation key. |
| `reason` | string or null | No | Why the exit was requested. Primary value is `"drone_left_early"`. Other possible values: `"window_expired"`, `"operator_recall"`. Logged for audit purposes. |
| `requested_at` | string or null | No | ISO 8601 timestamp of when the exit was decided. If omitted, the Flight Monitor records the time it received the request. |

## Responses

**202 Accepted** — session found, grace period started:

```json
{
  "action": "accepted",
  "flight_session_id": "8c189f91-0348-4a15-a6f0-23377dca7834",
  "drone_id": "drone-001",
  "message": "Exit request accepted. Finalization report will be sent to SADE after grace period."
}
```

**404 Not Found** — session already completed or was never registered:

```json
{
  "action": "not_found",
  "flight_session_id": "8c189f91-0348-4a15-a6f0-23377dca7834",
  "message": "No active session found for this flight_session_id."
}
```

A 404 is not an error — it means the session was already finalized (either the drone completed its mission normally via MQTT, or a previous exit request already handled it). It is safe to retry.

## Example curl

```bash
curl -X POST http://localhost:8000/flight-monitor/exit-request \
  -H "Content-Type: application/json" \
  -d '{
    "flight_session_id": "8c189f91-0348-4a15-a6f0-23377dca7834",
    "reason": "drone_left_early",
    "requested_at": "2026-04-21T19:00:00Z"
  }'
```

## How it fits the session lifecycle

```
1. SADE → Flight Monitor    POST /flight-monitor/register-session    (start monitoring)
2. Drone → MQTT → Pipeline  telemetry flows, state accumulates
3. SADE → Flight Monitor    POST /flight-monitor/exit-request        (begin exit grace period)
4.                           5 minutes of telemetry silence...
5. Flight Monitor → SADE    POST /tracker-session-finalized          (callback with telemetry)
```

Step 3 is only needed when the drone leaves early. If the drone completes its mission normally then it will automatically close out based on logic implemented in the flight monitoring system.

## Grace period behavior

- **Duration**: 5 minutes of telemetry silence (configurable in `server.py`)
- **Check interval**: every 30 seconds
- **Telemetry keeps flowing**: the session stays active during the grace period, so MQTT messages from the drone are still processed and tracked
- **Silence timer resets**: if new telemetry arrives, the 5-minute countdown restarts
- **Warning logs**: if the drone keeps transmitting after the exit request, a warning is logged every 30 seconds noting that the drone has not actually left yet
- **Natural completion**: if the drone sends a terminal status (e.g. `mission_completed`) during the grace period, the normal pipeline finalization handles it and the grace period task exits cleanly

## Notes

- The 202 response returns immediately. The grace period runs in the background.
- The finalization payload sent to SADE includes actual start/end times and a telemetry summary (altitude min/max, battery voltage start/end) based on all telemetry observed, including during the grace period.
