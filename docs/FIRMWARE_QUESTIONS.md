# Firmware Team Questions — MQTT Telemetry Observations

Source: raw telemetry samples captured on 2026-04-24 (three drones: Orange, Blue, Aqua). Full samples in [actual_drone_update_message_format.txt](actual_drone_update_message_format.txt).

Two items below are potentially load-bearing for the Flight Monitor and warrant a short conversation before we keep building on the current assumptions.

---

## 1. Two different MQTT topics in use across the fleet

### Observation
- Drones **Orange** and **Aqua** publish telemetry to topic `update_drone`.
- Drone **Blue** publishes the *same-shaped* payload to topic `status_message`.

### Impact
The Flight Monitor subscribes only to `update_drone`. Every telemetry packet from Blue is being silently dropped at the broker's subscription boundary — it never reaches the Flight Monitor's ingestion queue, session registry, or finalization path. If any production drone publishes to `status_message` in flight, we have no record of it and no ability to finalize its session.

### Questions for firmware
1. Is `status_message` a legacy topic that should be retired, or is it intentionally different from `update_drone`?
2. If intentional, what distinguishes the two? Is the payload schema identical on both topics (the Blue sample suggests yes) or do they diverge?
3. Is it acceptable to standardize the whole fleet on `update_drone`?

### Our options depending on the answer
- **Standardize on `update_drone`** (preferred, single source of truth).
- **Subscribe to both topics** on the Flight Monitor side if `status_message` has to stay for other consumers. Small code change; adds no structural complexity if payload shape is identical.

---

## 2. Publisher-side duplicate retransmits with identical timestamps

### Observation
Drone **Aqua** emitted the same telemetry payload **three times in ~0.9 ms**, all carrying the same `"timestamp": "2026-04-24T21:03:42.601+00:00"` and byte-identical field values (location, battery, heading, attitude). The broker-side receipt times differ only in sub-millisecond MQTT-client logging jitter. Because the drones publish at QoS 0 ("fire and forget"), this is not MQTT-layer redelivery — it's the drone's application layer retransmitting.

### Impact on the Flight Monitor
- **Distance accumulator:** unaffected. Great-circle distance between identical GPS coordinates is zero.
- **`message_count`:** inflated by the duplication factor. We currently treat message count as a rough drone-activity metric; duplicates overstate it.
- **Future incident detection:** any rate-of-change heuristic (sudden altitude drop, spike in attitude rate, etc.) that runs on raw message stream will see flat regions that are artifacts of retransmission, not real drone behavior. Dedupe by `(uavid, timestamp)` would be needed before feeding data into such a heuristic.
- **SADE finalization payload:** not materially affected today — min/max accumulators are idempotent against duplicates.

### Questions for firmware
1. Is the retransmission intentional (application-layer at-least-once delivery to compensate for QoS 0)?
2. If intentional, what's the dedupe key on the consumer side — is `(uavid, timestamp)` guaranteed unique per logical telemetry frame, or could two distinct frames share a timestamp in bursty-publish conditions?
3. If unintentional, is this a publisher bug that should be fixed upstream?
4. What's the expected duplication factor per frame (is 3× typical, or was this sample unusual)?

### Our options depending on the answer
- **Intentional + `(uavid, timestamp)` is a safe dedupe key** — add dedupe on the ingestion side; documented, no firmware work needed.
- **Intentional but timestamp collisions are possible** — need a different dedupe signal (e.g., a monotonic sequence number on each telemetry frame). Would be a firmware ask.
- **Unintentional** — firmware-side fix; Flight Monitor does nothing.

---

## Summary

Neither item is an immediate blocker, but both shape decisions we're about to make:

- **#1 (dual topics)** blocks any incident/alerting work on the Blue drone and any drone that also emits on `status_message`. High-priority to resolve before the dashboard ships, because a dashboard that silently excludes part of the fleet is worse than no dashboard.
- **#2 (duplicate publishes)** is latent today but becomes load-bearing the moment we add any rate-of-change-based detection (arm-state segment boundaries, incident codes, low-altitude alerts). Worth resolving before that work starts so the detectors don't have to be retrofitted with dedupe.
