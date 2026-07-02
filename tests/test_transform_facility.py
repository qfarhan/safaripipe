import csv
import json
import subprocess
import sys
from pathlib import Path

from json_transform.transform_facility_to_csv import (
    convert,
    iter_source_records,
    resolve_field,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = PROJECT_ROOT / "src" / "json_transform" / "transform_facility_to_csv.py"


def _hit(batch_id, facility_id, name=None):
    return {
        "_id": f"{facility_id}-doc",
        "_source": {
            "header": {"batchId": batch_id, "messageId": f"{facility_id}-msg"},
            "facility": {"facilityId": facility_id, **({"facilityName": name} if name else {})},
        },
    }


def test_iter_source_records_unwraps_es_hits(tmp_path):
    path = tmp_path / "in.jsonl"
    path.write_text(
        json.dumps(_hit("B1", "F1")) + "\n\n" + json.dumps(_hit("B1", "F2")) + "\n",
        encoding="utf-8",
    )
    records = list(iter_source_records(str(path)))
    assert [r["facility"]["facilityId"] for r in records] == ["F1", "F2"]
    assert all("_source" not in r for r in records)  # unwrapped, blank line skipped


def test_resolve_field_paths_constants_and_external():
    record = {"facility": {"facilityId": "F1"}}
    assert resolve_field({"intermediate_mapping": "facility.facilityId"}, record) == "F1"
    assert resolve_field({"intermediate_mapping": "C360"}, record) == "C360"
    assert resolve_field({"intermediate_mapping": "????"}, record) is None
    assert resolve_field({"intermediate_mapping": "external mapping"}, record) is None


def test_convert_flags_missing_required_fields():
    fields = [
        {"name": "FACILITY_ID", "intermediate_mapping": "facility.facilityId", "required": True},
    ]
    _, issues = convert({"facility": {}}, fields)
    assert [issue[0] for issue in issues] == ["FACILITY_ID"]


def test_script_end_to_end(tmp_path):
    # Exactly how etl.eod_runner invokes it: <command...> <input.jsonl> <output.csv>.
    src = tmp_path / "facility-B1.jsonl"
    src.write_text(
        "\n".join(json.dumps(_hit("B1", f"F{i}", name=f"Facility {i}")) for i in range(3)) + "\n",
        encoding="utf-8",
    )
    out = tmp_path / "facility.csv"

    completed = subprocess.run(
        [sys.executable, str(SCRIPT), str(src), str(out)],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr
    with out.open(encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 3
    assert rows[0]["BATCH_ID"] == "B1"
    assert rows[0]["FACILITY_ID"] == "F0"
    assert rows[0]["SOURCE_SYSTEM"] == "C360"  # constant mapping
