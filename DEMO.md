# SADE Flight Monitor — Live Demo Guide

This document is the runbook for the live, narrated demo of the SADE
Flight Monitor system, intended to be presented to a non-technical or
partly-technical audience. Read it once before the demo so you know
what's about to happen, and keep it open in a tab during the demo as a
recovery aid in case anything misbehaves.

---

## Contents

1. [What this demo is](#what-this-demo-is)
2. [Before you start](#before-you-start)
3. [What to have open during the demo](#what-to-have-open-during-the-demo)
4. [Running the demo](#running-the-demo)
5. [What happens, phase by phase](#what-happens-phase-by-phase)
6. [What gets posted to "SADE"](#what-gets-posted-to-sade)
7. [Stopping early / recovery](#stopping-early--recovery)
8. [What this demo does NOT show](#what-this-demo-does-not-show)

---

## What this demo is

The demo is a single self-contained Python script — `scripts/run_demo.py` —
that boots the entire Flight Monitor system in one process and walks two
simulated drones through their full lifecycle in front of the audience.
It pauses on Enter at every key milestone so you can talk between phases.

What the audience sees, in order:

- the service booting (FastAPI webhook server, MQTT pipeline, periodic
  sweeper, plus a tiny in-process "SADE catcher" so we can see the
  finalize POSTs),
- two drones being **registered** via SADE's webhook,
- those drones **publishing live MQTT telemetry** which the pipeline
  ingests,
- one drone going through a **multi-flight sequence** (arms, flies,
  lands, re-arms, flies again) — proving the FLIGHT_SEGMENT detection
  recorded both flights as separate events,
- SADE sending **exit-requests** for each drone, the **grace period**
  elapsing, and the **final mission report** being POSTed to the SADE
  catcher (which prints it in a green-bordered block),
- a **clean shutdown** with the registry confirming zero active
  sessions.

Everything runs locally — no AWS, no real broker beyond the local
Mosquitto, no external network dependencies. The whole thing takes
5-10 minutes with talking, ~90 seconds in `--auto` mode.

---

## Before you start

### One-time prerequisites

Already done on the demo laptop:

- Python 3.13 venv at `./venv/` with all dependencies installed.
- Mosquitto installed via `brew install mosquitto`.

### The morning of the demo

1. **Confirm the broker is running**:
   ```bash
   brew services list | grep mosquitto
   ```
   You want the line to read `started`. If not:
   ```bash
   brew services start mosquitto
   ```

2. **Confirm the demo script runs end-to-end without you touching it**.
   This is the dress rehearsal — do it once at least an hour before.
   ```bash
   cd ~/Desktop/Sade/sade-zone-flight-monitor
   ./venv/bin/python scripts/run_demo.py --auto
   ```
   Expect: ~90 seconds of output ending with
   ```
   ✓ Demo complete.  Catcher received 2 finalization payload(s).
   ```
   If it ends with anything else, see [Stopping early / recovery](#stopping-early--recovery).

3. **Close anything else listening on ports 8000 or 8765**:
   ```bash
   lsof -nP -iTCP:8000 -sTCP:LISTEN
   lsof -nP -iTCP:8765 -sTCP:LISTEN
   ```
   Both should print nothing. If something is listed, kill that process.

---

## What to have open during the demo

You want **two windows side by side** on a single monitor (or one each
on a dual-monitor setup):

### Window 1 — Terminal

- A terminal at the repo root: `cd ~/Desktop/Sade/sade-zone-flight-monitor`
- Make the font big enough that the audience can read it from the back
  of the room (typically 16-20 pt).
- This is where you'll **type the run command** and where the **phase
  banners**, **arm-state log lines**, and **green-bordered SADE catcher
  output** will appear.

### Window 2 — Browser

- Open `http://localhost:8000/dashboard` in a fresh browser tab.
- Until you start the demo, this URL won't load — that's expected.
- Once Phase 1 boots the service, the page becomes available and
  auto-refreshes every 7 seconds.
- This is where the audience sees the **live registry + telemetry
  state** as a table, with status badges (FLYING, LANDED, EXIT_REQUESTED,
  WAITING).

You'll be **switching attention between these two windows** at every
phase. The demo's pauses are designed to give you time to point at the
dashboard between phases.

---

## Running the demo

From the repo root, in your prepared terminal:

```bash
./venv/bin/python scripts/run_demo.py
```

Default mode is **interactive** — at each phase boundary the script
prints a `⏸` line and waits for you to press **Enter** to advance.
This lets you talk to the audience for as long as you want at each
checkpoint.

Other invocations (handy to know):

| Command | When to use it |
|---|---|
| `./venv/bin/python scripts/run_demo.py` | The demo (interactive — Enter to advance) |
| `./venv/bin/python scripts/run_demo.py --auto` | Dress rehearsal / smoke test (~90s, no Enter required) |
| `./venv/bin/python scripts/run_demo.py --verbose` | Show every per-message worker log line (very chatty — usually NOT what you want for a demo) |

---

## What happens, phase by phase

The script prints colour-coded phase banners (`PHASE 0 ... PHASE 8`) so
you always know where you are. Each phase is described below: what
appears in the **terminal**, what appears on the **dashboard**, and what
to **say** to the audience.

### Phase 0 — Pre-flight

| Where | What you'll see |
|---|---|
| Terminal | Three green check-marks: Mosquitto reachable, port 8000 free, port 8765 free |
| Dashboard | Not loadable yet |

> "First we sanity-check the environment — the broker has to be running
> and our two ports have to be free."

### Phase 1 — Boot the SADE Flight Monitor

| Where | What you'll see |
|---|---|
| Terminal | "SADE catcher listening on http://127.0.0.1:8765" → "Patched timing constants" → "FastAPI server up" → "Pipeline subscribed to MQTT topic 'status_message'" |
| Dashboard | Now loadable; shows "No active sessions." |
| Pause | "Boot complete — verify dashboard is reachable." |

> "We've started the service. The dashboard is live but empty —
> no flights have been registered yet. The 'SADE catcher' is a tiny
> stand-in for the real SADE backend so we can see what we'd POST."

**Switch to the browser** and confirm the page loaded with the empty
state, then press Enter.

### Phase 2 — Register two flight sessions

| Where | What you'll see |
|---|---|
| Terminal | Two POSTs: one for `drone-alpha`, one for `drone-bravo`. Each prints a green check-mark + the assigned `flight_session_id`. `/health` reports `active_sessions: 2`. |
| Dashboard | After the next 7-second poll: a Zone section labelled "Zone: demo-zone-001" with two rows, both showing the **WAITING** badge (no telemetry yet). |
| Pause | "Both sessions in registry — see WAITING badges on the dashboard." |

> "SADE has approved two flights and registered them with us. They're
> in the registry but neither drone has started transmitting yet, so
> they show as WAITING."

### Phase 3 — Drone-alpha arms and starts publishing telemetry

| Where | What you'll see |
|---|---|
| Terminal | "Publisher connected" → after a few seconds, "drone-alpha ARMING" → an INFO log line: `Arm-state transition: ARMED. flight_session_id=... segment_index=0 time_in_utc=...` |
| Dashboard | drone-alpha's row flips to a green **FLYING** badge; live altitude / voltage / distance start updating. |
| Pause | "drone-alpha flying." |

> "Drone-alpha just took off. The pipeline saw `status.armed` flip from
> false to true and opened a new flight segment. The dashboard reflects
> that within 7 seconds. Battery and altitude on the dashboard update
> with every refresh."

### Phase 4 — Drone-bravo arms

| Where | What you'll see |
|---|---|
| Terminal | A second `Arm-state transition: ARMED` for `drone-bravo`. |
| Dashboard | Two FLYING rows in the same zone. |
| Pause | "Both drones in flight." |

### Phase 5 — Drone-alpha lands and re-arms (multi-segment demo)

| Where | What you'll see |
|---|---|
| Terminal | "drone-alpha DISARMING" → `Arm-state transition: DISARMED` log → "drone-alpha ARMING again" → second `Arm-state transition: ARMED` (note `segment_index=1` this time). |
| Dashboard | drone-alpha's status flips to LANDED briefly, then back to FLYING. The "Segments" column reads `1 + 1 open` (one closed segment, one open). |
| Pause | "drone-alpha now has one closed segment + one open." |

> "drone-alpha just landed to swap batteries — but the SADE session is
> still alive; SADE owns finalization. A few seconds later it took off
> again. The internal segments list now has one closed flight and one
> open one. **When this session is eventually finalized, the report
> will carry both flights as separate FLIGHT_SEGMENT events** — that's
> the multi-segment piece."

### Phase 6 — SADE sends exit-request for drone-bravo

| Where | What you'll see |
|---|---|
| Terminal | "drone-bravo DISARMING and going silent" → "Exit-request accepted" → "Exit-grace task running. Will finalize after 8s of silence." → After 8 seconds, a **green-bordered SADE RECEIVED FINALIZATION REPORT** block listing the FLIGHT_SEGMENT and battery in/out. |
| Dashboard | drone-bravo's status flips to a yellow **EXIT_REQUESTED** badge during the grace period, then disappears entirely once finalized. |
| Pause | "drone-bravo fully closed out." |

> "SADE has decided drone-bravo's session should close. We don't
> finalize immediately — we wait for the drone to actually go quiet
> (8 seconds for the demo, 5 minutes in production), then we POST one
> final report with the full mission summary. **Look at the catcher
> output — that's exactly what SADE receives**: the flight-session id,
> the time window, the battery in/out, all derived from the live
> telemetry the worker accumulated."

### Phase 7 — Same exit flow for drone-alpha

| Where | What you'll see |
|---|---|
| Terminal | Same flow: disarm, exit-request, 8-second grace, green-bordered catcher output. **This one shows TWO FLIGHT_SEGMENT events** — one per arm/disarm cycle from Phase 5. |
| Dashboard | Empty — registry has zero active sessions. |
| Pause | "All drones finalized." |

> "drone-alpha's payload has two FLIGHT_SEGMENT events because it flew
> twice during this session — landing to recharge between flights
> doesn't end the session, only SADE's exit-request does. The battery
> voltages on each segment are independent — you can see the drop
> across each individual flight."

### Phase 8 — Shutdown

| Where | What you'll see |
|---|---|
| Terminal | "Cancelling pipeline task" → "Pipeline shut down" → "FastAPI shut down" → "Catcher shut down" → "Demo complete. Catcher received 2 finalization payload(s)." |
| Dashboard | Becomes unreachable (server is gone). |

> "Clean shutdown. Two finalization payloads received, exactly the
> number of sessions we registered. The system is back to zero state."

---

## What gets posted to "SADE"

The "SADE catcher" is a tiny localhost HTTP server that mocks SADE's
real `/tracker-session-finalized` endpoint. Each finalization POST is
pretty-printed to the terminal in a **green-bordered block** like this:

```
┌─ SADE RECEIVED FINALIZATION REPORT ─────────────────────────────
│  flight_session_id : demo-drone-alpha-...
│  report_time_utc   : 2026-05-07T...
│  altitude_min/max  : 0.0 / 98.7 m
│  distance_flown    : 28 m
│  events            : 2
│    [1] FLIGHT_SEGMENT  21:39:32 → 21:39:56
│        battery: 16.5 V → 15.9 V
│    [2] FLIGHT_SEGMENT  21:40:02 → 21:40:42
│        battery: 15.9 V → 14.9 V
└─────────────────────────────────────────────────────────────────
```

This is byte-for-byte the same payload shape the real SADE backend
expects (per `SADE_AWS_API_INFORMATION/SADE_CONTRACT.md`). The catcher
returns a 200 with a fake `reputation_record_id` so the Flight Monitor
doesn't trigger its retry path — exactly as real SADE would on success.

---

## Stopping early / recovery

### To stop the demo cleanly mid-run

Press **Ctrl-C** in the terminal. The script will print a yellow
"Interrupted — cleaning up" line and tear down all four components.

### If a port is already in use at startup

The pre-flight check (Phase 0) will tell you exactly which port + how
to find the offending process:

```bash
lsof -nP -iTCP:8000 -sTCP:LISTEN
```

Note the PID, then `kill <PID>`. Re-run the demo.

### If the dashboard says "refresh failed"

The header turns red and the message updates. Most likely cause: the
demo finished (Phase 8 cleanly stopped the server) and you're still
looking at the page. Refresh the page; it'll continue trying to reach
the now-stopped server. If the demo is still running and the page is
red, something is wrong with the FastAPI task — check the terminal for
errors.

### If the catcher block doesn't appear after the grace period

By default the grace is 8 seconds; the script waits up to 23 s before
giving up. If the catcher block is missing, the most likely cause is
the pipeline crashed early. Look at the terminal scrollback for any
red-text `✗` lines or Python tracebacks. If there's something
unrecoverable, Ctrl-C and re-run.

### If the dashboard is blank

The dashboard polls `/dashboard/data` every 7 seconds. Wait one full
poll cycle. If still blank after that:
- check the terminal for FastAPI errors,
- try `curl http://localhost:8000/health` from another terminal to
  confirm the server is responding,
- as a last resort, hit reload in the browser.

---

## What this demo does NOT show

Worth knowing in case the audience asks:

- **Auth** — the webhook endpoints and the dashboard have no auth. This
  is intentionally deferred for the in-house version; the future
  customer-facing build will add it.
- **Real AWS / SADE** — everything is local; the catcher mocks SADE.
  The end-to-end test against real AWS lives at
  `scripts/run_e2e_aws_test.py` and is run separately on staging.
- **Real drone hardware** — telemetry comes from a 90-line Python
  publisher in the demo script, not real PX4 / Gazebo. Real captures
  from actual drones live in `docs/actual_drone_update_message_format.txt`.
- **Production timing** — the demo patches the timing constants down
  by 5-10× so each phase finishes in seconds. In production, the grace
  period is 5 minutes, the stranded threshold is 10 minutes, and the
  force-close backstop is 24 hours.
- **The deadline-breach flag and the stranded flag** appear on the
  dashboard but the demo doesn't deliberately trigger them. Worth
  mentioning when you point at the totals row at the top of the
  dashboard ("if a drone overstays its window or goes silent without
  an exit-request, it shows up here").
