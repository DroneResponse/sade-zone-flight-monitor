"""Unit tests for app.common.mission_row_writer."""

from __future__ import annotations

import csv
from pathlib import Path

from app.common.mission_row_writer import MissionCsvWriter
from app.monitoring.mission_row_schema import ALL_COLUMNS


def test_writer_creates_header_when_file_missing(tmp_path: Path) -> None:
    out_path = tmp_path / "mission_rows.csv"

    MissionCsvWriter(out_path=str(out_path))

    assert out_path.exists()
    with out_path.open(newline="") as fh:
        rows = list(csv.reader(fh))
    assert rows == [list(ALL_COLUMNS)]


def test_writer_appends_row_with_all_columns(tmp_path: Path) -> None:
    out_path = tmp_path / "mission_rows.csv"
    writer = MissionCsvWriter(out_path=str(out_path))

    writer.write_row({"session_id": "f-1", "uav_id": "d-1"})

    with out_path.open(newline="") as fh:
        reader = csv.DictReader(fh)
        rows = list(reader)
    assert len(rows) == 1
    assert rows[0]["session_id"] == "f-1"
    assert rows[0]["uav_id"] == "d-1"


def test_empty_out_path_disables_writer() -> None:
    """``MISSION_ROWS_OUT=""`` is the documented "disabled" signal.

    Construction must not touch the filesystem and ``write_row`` must
    silently no-op so the pipeline can run without CSV output.
    """
    writer = MissionCsvWriter(out_path="")

    assert writer.out_path == ""

    # write_row is a no-op — no exception even with extras that would
    # otherwise raise via csv.DictWriter's strict fieldnames check.
    writer.write_row({"session_id": "f-1", "extra_unknown_field": "x"})
