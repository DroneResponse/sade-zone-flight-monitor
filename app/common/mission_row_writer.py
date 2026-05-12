"""Local CSV writer for mission summary rows.

This writer is self-contained and appends mission summary rows to local disk,
creating the file with headers when needed.
"""

from __future__ import annotations

import csv
import json
import os
from threading import Lock
from typing import Any

from app.monitoring.mission_row_schema import ALL_COLUMNS


class MissionCsvWriter:
    """Append mission summary rows to a local CSV file.

    An empty ``out_path`` disables CSV output entirely — both header
    creation and ``write_row`` become no-ops.  This matches the
    ``MISSION_ROWS_OUT=""`` convention documented in ``.env.example``
    and the Dockerfile (CSV is a diagnostic-only output; the SADE
    finalize POST is the production path).
    """

    def __init__(self, out_path: str) -> None:
        self.out_path = out_path
        self._lock = Lock()
        if not self.out_path:
            return
        self._ensure_csv_header()

    def _ensure_csv_header(self) -> None:
        """Create output CSV with header row if missing or empty."""
        if os.path.exists(self.out_path) and os.path.getsize(self.out_path) > 0:
            return

        with open(self.out_path, "w", newline="", encoding="utf-8") as file_obj:
            writer = csv.DictWriter(file_obj, fieldnames=ALL_COLUMNS)
            writer.writeheader()

    def write_row(self, row: dict[str, Any]) -> None:
        """Append one schema-complete row to the local CSV file."""
        if not self.out_path:
            return

        row_to_write = dict(row)

        for key in ALL_COLUMNS:
            row_to_write.setdefault(key, None)

        # CSV cells cannot store dict/list directly; serialize JSON-shaped fields.
        for key in ("battery.voltage_in", "battery.voltage_out", "incidents"):
            if isinstance(row_to_write.get(key), (dict, list)):
                row_to_write[key] = json.dumps(row_to_write[key])

        with self._lock:
            with open(self.out_path, "a", newline="", encoding="utf-8") as file_obj:
                writer = csv.DictWriter(file_obj, fieldnames=ALL_COLUMNS)
                writer.writerow(row_to_write)
