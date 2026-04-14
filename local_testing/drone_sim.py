#!/usr/bin/env python3
"""Local drone simulator helpers for MQTT testing.

This module keeps the original interactive simulator behavior available while
adding a simple autonomous telemetry publisher used by ``run_local_test.py``.
The new publisher emits realistic-looking ``update_drone`` payloads that match
what the ingestion pipeline already expects.
"""

from __future__ import annotations

import importlib.util
from importlib.machinery import SourceFileLoader
import json
import math
import random
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable


def print_info(message: str) -> None:
    """Small local replacement for the unavailable ``smp.utils.print_info``."""
    print(f"[INFO] {message}")


def _load_local_mqtt_client_class():
    """Load the existing MQTT wrapper from the local resource file."""
    module_path = Path(__file__).with_name("mqtt_publisher_client")
    loader = SourceFileLoader("local_mqtt_publisher_client", str(module_path))
    spec = importlib.util.spec_from_loader(loader.name, loader)
    if spec is None:
        raise ImportError(f"Unable to load MQTT client module from {module_path}")

    module = importlib.util.module_from_spec(spec)
    loader.exec_module(module)
    return module.MQTTClient


MQTTClient = _load_local_mqtt_client_class()


class DroneSimulatorBase:
    """Small command-topic simulator retained for manual local experiments."""

    def __init__(self, drone_id, mqtt_client):
        self.drone_id = drone_id
        self.mqtt_client = mqtt_client

        # Keep the existing topic subscription behavior for command testing.
        self.subscribe_to_commands()

        self.current_task = None
        self.task_history = []

    def subscribe_to_commands(self):
        """Subscribe to the command topics used by the manual simulator."""
        self.mqtt_client.subscribe(f"drone/{self.drone_id}/task/new", self.on_message)
        self.mqtt_client.subscribe(f"drone/{self.drone_id}/task/cancel-current", self.on_message)
        self.mqtt_client.subscribe(f"drone/{self.drone_id}/task/end-task-loop", self.on_message)
        self.mqtt_client.subscribe(f"drone/{self.drone_id}/mission-spec", self.on_message)

    def on_message(self, client, userdata, message):
        """Route command messages to the appropriate handler."""
        if message.topic.endswith("new"):
            self.on_new_task_received(message)
        elif message.topic.endswith("cancel-current"):
            self.on_task_cancelled(message)
        elif message.topic.endswith("end-task-loop"):
            self.on_end_task_loop(message)
        elif message.topic.endswith("mission-spec"):
            self.on_mission_spec_received(message)

    def on_mission_spec_received(self, message):
        print_info(f"(DRONE {self.drone_id}) STARTUP MISSION")
        print(message.payload.decode())

    def on_new_task_received(self, message):
        task_info = json.loads(message.payload.decode())
        self.current_task = task_info
        self.task_history.append(task_info)
        print_info(f"(DRONE {self.drone_id}) TASK RECEIVED")
        print(message.payload.decode())

    def on_task_cancelled(self, message):
        print_info(f"(DRONE {self.drone_id}) TASK CANCELLED MESSAGE RECEIVED")
        print(message.payload.decode())
        self.send_task_outcome("task_cancelled", False)
        self.send_ready_message()

    def on_end_task_loop(self, message):
        print_info(f"(DRONE {self.drone_id}) END OF TASK LOOP RECEIVED")
        print(f"End task loop command received for drone {self.drone_id}: {message.payload.decode()}")

    def send_ready_message(self):
        """Publish a ready-for-task message for manual command-topic testing."""
        topic = f"drone/{self.drone_id}/task/ready"
        message = {
            "uavid": self.drone_id,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()),
        }
        self.mqtt_client.publish(topic, json.dumps(message))
        print_info(f"(DRONE {self.drone_id}) READY MESSAGE SENT")

    def send_task_outcome(self, outcome, is_done):
        """Publish a task outcome message for manual simulator workflows."""
        topic = f"drone/{self.drone_id}/task/outcome"
        task_id = self.current_task["task_id"] if self.current_task else "local-test-task"
        message = {
            "uavid": self.drone_id,
            "task_id": task_id,
            "outcome": outcome,
            "is_done": is_done,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()),
        }
        self.mqtt_client.publish(topic, json.dumps(message))
        print_info(f"(DRONE {self.drone_id}) OUTCOME MESSAGE SENT")
        print(message)


class DroneMenu:
    """Simple interactive menu retained for the original manual simulator."""

    def __init__(self, drone_simulator: DroneSimulatorBase):
        self.drone_simulator = drone_simulator
        self.menu_actions = [
            ("Send 'Ready' Message", self.drone_simulator.send_ready_message),
            (
                "Send Successful Task Outcome",
                lambda: self.drone_simulator.send_task_outcome(outcome="no_detection", is_done=True),
            ),
            ("Send Failure Task Outcome", self.send_failure_task_outcome),
            ("Exit", self.exit_menu),
        ]
        self.running = True

    def exit_menu(self):
        self.running = False
        print("Exiting Drone Simulation Menu.")

    def send_failure_task_outcome(self):
        print("\nSelect Failure Reason:")
        print("1. Low Battery")
        print("2. False Match")
        print("3. User Response Timeout")
        print("4. Subject Found")
        print("5. Other")

        failure_reason_choice = input("Enter your choice (1-5): ").strip()
        failure_reasons = {
            "1": "low_battery",
            "2": "false_match",
            "3": "user_response_timeout",
            "4": "subject_found",
            "5": "other",
        }
        failure_reason = failure_reasons.get(failure_reason_choice, "unknown")

        if failure_reason == "other":
            custom_reason = input("Enter the failure reason: ").strip()
            failure_reason = custom_reason or "unknown"

        self.drone_simulator.send_task_outcome(outcome=failure_reason, is_done=False)

    def display_menu(self):
        """Run the simple command-line menu loop."""
        while self.running:
            time.sleep(0.3)
            print("\nDrone Simulation Menu:")
            for idx, (desc, _) in enumerate(self.menu_actions, 1):
                print(f"{idx}. {desc}")

            choice = input("\nEnter the number of the action you want to perform: ").strip()
            try:
                choice_num = int(choice)
                if 1 <= choice_num <= len(self.menu_actions):
                    action = self.menu_actions[choice_num - 1][1]
                    action()
                else:
                    print("Invalid choice. Please try again.")
            except ValueError:
                print("Please enter a valid number.")
            except Exception as exc:
                print(f"An error occurred while executing the action: {exc}")


@dataclass
class DroneTelemetryProfile:
    """Static per-drone configuration used by the autonomous telemetry publisher."""

    drone_id: str
    home_latitude: float
    home_longitude: float
    altitude_m: float
    heading_offset_deg: float


class SimulatedDronePublisher:
    """Background publisher that emits realistic telemetry to ``update_drone``."""

    def __init__(
        self,
        profile: DroneTelemetryProfile,
        *,
        broker: str = "localhost",
        port: int = 1883,
        topic: str = "update_drone",
        publish_interval: float = 1.0,
        logger: Callable[[str], None] | None = None,
    ) -> None:
        self.profile = profile
        self.topic = topic
        self.publish_interval = publish_interval
        self.logger = logger or (lambda message: None)

        # Reuse the existing MQTT wrapper for all telemetry publishing.
        self.mqtt_client = MQTTClient(broker, port, client_id=f"sim-{profile.drone_id}")

        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._message_index = 0
        self._battery_voltage = random.uniform(16.1, 16.8)

    def start(self) -> None:
        """Connect to MQTT and begin publishing telemetry on a daemon thread."""
        self.mqtt_client.connect()
        if not self.mqtt_client.wait_until_connected(timeout=5.0):
            raise RuntimeError(f"Timed out connecting drone publisher for {self.profile.drone_id}")

        self._thread = threading.Thread(
            target=self._publish_loop,
            name=f"drone-publisher-{self.profile.drone_id}",
            daemon=True,
        )
        self._thread.start()
        self.logger(f"Started simulated drone publisher for {self.profile.drone_id}")

    def stop(self) -> None:
        """Publish a mission-finished update, then stop background publishing cleanly."""
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=self.publish_interval + 2.0)

        # Send one explicit mission-finished update so ingestion writes the final row now.
        completion_payload = self._build_payload(mission_status="mission_completed", status_label="landed", mode="IDLE")
        self.mqtt_client.publish(self.topic, json.dumps(completion_payload), qos=0)
        time.sleep(0.2)

        self.mqtt_client.stop()
        self.logger(f"Stopped simulated drone publisher for {self.profile.drone_id}")

    def _publish_loop(self) -> None:
        """Emit one telemetry update per interval until the runner shuts down."""
        while not self._stop_event.is_set():
            payload = self._build_payload()
            self.mqtt_client.publish(self.topic, json.dumps(payload), qos=0)
            self._message_index += 1
            self._stop_event.wait(self.publish_interval)

    def _build_payload(
        self,
        *,
        mission_status: str = "on_mission",
        status_label: str = "tracking",
        mode: str = "AUTO",
    ) -> dict:
        """Build a realistic telemetry message matching the ingestion schema."""
        seconds = self._message_index * self.publish_interval

        # Create a smooth circular drift around the home location.
        orbit_angle = (seconds / 18.0) + math.radians(self.profile.heading_offset_deg)
        latitude = self.profile.home_latitude + (0.00045 * math.sin(orbit_angle))
        longitude = self.profile.home_longitude + (0.00055 * math.cos(orbit_angle))
        altitude = self.profile.altitude_m + (6.0 * math.sin(seconds / 7.0))
        heading = (self.profile.heading_offset_deg + (seconds * 18.0)) % 360.0

        # Slowly drain battery to make output feel more realistic over a local test run.
        self._battery_voltage = max(14.2, self._battery_voltage - 0.004)

        return {
            "uavid": self.profile.drone_id,
            "mission_status": mission_status,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "status": {
                "status": status_label,
                "mode": mode,
                "onboard_pilot": "local_sim",
                "air_lease_state": "granted",
                "location": {
                    "latitude": round(latitude, 6),
                    "longitude": round(longitude, 6),
                    "altitude": round(altitude, 2),
                },
                "drone_attitude": {
                    "x": round(0.02 * math.sin(seconds / 5.0), 4),
                    "y": round(0.02 * math.cos(seconds / 5.0), 4),
                    "z": round(0.03 * math.sin(seconds / 6.0), 4),
                    "w": 0.999,
                },
                "drone_heading": round(heading, 2),
                "battery": {
                    "voltage": round(self._battery_voltage, 3),
                },
            },
        }


def build_default_profiles(drone_count: int) -> list[DroneTelemetryProfile]:
    """Create a small fleet of local drones with slightly different home positions."""
    base_latitude = 39.7684
    base_longitude = -86.1581
    profiles = []
    for index in range(drone_count):
        profiles.append(
            DroneTelemetryProfile(
                drone_id=f"drone-{index + 1:02d}",
                home_latitude=base_latitude + (index * 0.0012),
                home_longitude=base_longitude - (index * 0.0011),
                altitude_m=110.0 + (index * 12.0),
                heading_offset_deg=index * 40.0,
            )
        )
    return profiles


def drone_main():
    """Run the original manual simulator menu for a single drone."""
    if len(sys.argv) != 2:
        print("Usage: python local_testing_recources/drone_sim.py <drone_id>")
        sys.exit(1)

    drone_id = sys.argv[1]
    mqtt_client = MQTTClient("127.0.0.1", 1883, client_id=f"menu-{drone_id}")
    mqtt_client.connect()
    mqtt_client.wait_until_connected(timeout=5.0)

    drone_simulator = DroneSimulatorBase(drone_id, mqtt_client)
    drone_menu = DroneMenu(drone_simulator)

    try:
        drone_menu.display_menu()
    except KeyboardInterrupt:
        print("\nInterrupted by user. Exiting...")
    finally:
        mqtt_client.stop()
        sys.exit(0)


if __name__ == "__main__":
    drone_main()
