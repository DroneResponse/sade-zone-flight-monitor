#!/usr/bin/env python3
import json
import threading
from typing import Callable, Dict, Any, Optional

import paho.mqtt.client as mqtt


class DroneMqttClient:
    """
    Generic MQTT client that listens for DroneResponse status messages
    and calls a callback with parsed state.

    1. connect to a broker
    2. subscribe to a drone status topic
    3. parse incoming messages into structured fields

    on_state callback signature:
        (uavid: str,
         location: Dict[str, Any],
         drone_attitude: Dict[str, Any],
         drone_heading: float,
         state_info: Dict[str, Any]) -> None

    
    """

    # Define how MQTT client is initialized
    def __init__(
        self,
        broker: str = "localhost",
        port: int = 1883,
        topic: str = "update_drone",
    
        on_state: Optional[
            Callable[[str, Dict[str, Any], Dict[str, Any], float, Dict[str, Any]], None]
        ] = None,
    ):
        self.broker = broker
        self.port = port
        self.topic = topic

    

        self.on_state = on_state
        

        self._client: Optional[mqtt.Client] = None
        self._thread: Optional[threading.Thread] = None

    # ------------------------------------------------------------------
    # Utility
    # ------------------------------------------------------------------
    # Helper function for printing numbers cleanly
    def _fmt(self, v, prec=3) -> str:
        if v is None:
            return "None"
        try:
            return f"{float(v):.{prec}f}"
        except Exception:
            return str(v)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    # Function to start a MQTT Client: connect, subscribe, and begin listening
    def start(self) -> None:
        client = mqtt.Client()
        self._client = client

        def on_connect(client, userdata, flags, rc, properties=None):
            # Paho calls on_connect automatically once the TCP/MQTT connection completes.
            print("MQTT connected with rc =", rc)

            # Main status topic (drone state)
            client.subscribe(self.topic, qos=0)
            print(f"Subscribed to topic: {self.topic}")


        def on_message(client, userdata, msg):
            try:
               
                # --- Normal drone status topic ---
                if msg.topic != self.topic:
                    # Unknown/unhandled topic; just log
                    print(f"[MQTT] Unhandled topic {msg.topic}: {msg.payload!r}")
                    return

                # Note: payloads are raw bytes, need to decode the bytes to a string, then parse the json/string into a python dictionary
                payload = msg.payload.decode("utf-8", errors="ignore")
                data = json.loads(payload)

                # Extract information from 
                uavid = data.get("uavid") or data.get("uavID") or "Unknown"
                status = data.get("status", {}) or {}
                location = status.get("location", {}) or {}
                drone_attitude = status.get("drone_attitude", {}) or {}
                drone_heading = status.get("drone_heading", None)
                battery = status.get("battery", {}) or {}

                lat = location.get("latitude")
                lon = location.get("longitude")
                alt = location.get("altitude")

                ax = drone_attitude.get("x")
                ay = drone_attitude.get("y")
                az = drone_attitude.get("z")
                aw = drone_attitude.get("w")

                # Print area for debugging, toggle on/off with True/False
                if False:
                    print(
                        f"MQTT topic={msg.topic} "
                        f"uav={uavid} "
                        f"lat={self._fmt(lat,6)} lon={self._fmt(lon,6)} alt={self._fmt(alt,2)} "
                        f"heading={self._fmt(drone_heading,2)} "
                        f"att=({self._fmt(ax,3)},{self._fmt(ay,3)},"
                        f"{self._fmt(az,3)},{self._fmt(aw,3)})"
                    )

                # Create dictionary of messgae metadata
                state_info = {
                    "status": status.get("status", ""),
                    "mode": status.get("mode", ""),
                    "onboard_pilot": status.get("onboard_pilot", ""),
                    "air_lease_state": status.get("air_lease_state", ""),
                    "voltage": battery.get("voltage", None),
                }

                # Note to self: Come back and figure out what this is doing
                if self.on_state is not None:
                    self.on_state(
                        uavid,
                        location,
                        drone_attitude,
                        float(drone_heading) if drone_heading is not None else 0.0,
                        state_info,
                    )

            except Exception as ex:
                print("MQTT parse error:", ex)
                print("  Raw payload:", repr(msg.payload))

        client.on_connect = on_connect
        client.on_message = on_message

        def loop():
            try:
                # Open connection to the broker
                client.connect(self.broker, self.port, keepalive=30)
                # Run an event loop (loop_forever) to:
                # constantly listen to the network socket processes incoming MQTT packets
                # automatically call (triggers) your callback functions when certain events happen
                # It says: Sit here forever, watch the network connection, and react to events as they happen.”Those events include:
                #       connection established → call on_connect
                #       message received → call on_message
                #       keepalive timeout → send ping
                #       reconnect needed → reconnect
                client.loop_forever(retry_first_connection=True)
            except Exception as ex:
                print("MQTT loop exception:", ex)

        # Note: A daemon thread is a background thread that is meant to support the main program, not keep it alive.
        t = threading.Thread(target=loop, name="mqtt-loop", daemon=True)
        self._thread = t
        t.start()

    def publish(self, topic: str, payload: str, qos: int = 0, retain: bool = False) -> None:
        """
        Convenience wrapper to publish MQTT messages through the same client.

        payload should be a string (JSON, etc.) – encoding is handled by paho.
        """
        if self._client is None:
            print("[DroneMqttClient] publish called but client is None")
            return
        try:
            info = self._client.publish(topic, payload, qos=qos, retain=retain)
            # You can call info.wait_for_publish() from the caller if you need blocking.
        except Exception as ex:
            print("[DroneMqttClient] publish error:", ex)

    def stop(self) -> None:
        try:
            if self._client is not None:
                self._client.loop_stop()
                self._client.disconnect()
        except Exception:
            pass

def main() -> int:
    import argparse
    import time

    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Run DroneMqttClient listener")
    parser.add_argument("--broker", default="localhost", help="MQTT broker host")
    parser.add_argument("--port", type=int, default=1883, help="MQTT broker port")
    parser.add_argument("--topic", default="update_drone", help="Drone status topic")
    args = parser.parse_args()

    def on_state(uavid, location, drone_attitude, drone_heading, state_info):
        lat = location.get("latitude")
        lon = location.get("longitude")
        alt = location.get("altitude")
        mode = state_info.get("mode", "")
        status = state_info.get("status", "")
        voltage = state_info.get("voltage", None)

        print(
            f"[STATE] uav={uavid} "
            f"lat={lat} lon={lon} alt={alt} "
            f"heading={drone_heading:.2f} "
            f"mode={mode} status={status} voltage={voltage}"
        )

    # Make mqtt client
    c = DroneMqttClient(
        broker=args.broker,
        port=args.port,
        topic=args.topic,
        on_state=on_state,
    )

    # Connects + subscribes + begins listening in the background thread
    c.start()
    print(
        f"DroneMqttClient running. broker={args.broker}:{args.port} "
        f"topic={args.topic} (Ctrl+C to stop)"
    )

    try:
        while True:
            time.sleep(1.0)
    except KeyboardInterrupt:
        print("\nStopping...")
    finally:
        c.stop()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())