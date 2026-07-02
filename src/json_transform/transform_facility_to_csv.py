#!/usr/bin/env python3
"""
transform_facility_to_csv.py
============================
Per-feed transform for the "facility" EOD feed (invoked by etl.eod_runner).

Reads the JSON-lines file produced by etl.es_lookup (one Elasticsearch hit per
line, the record under "_source"), builds one CSV row per record using the
field definitions in `facility_fields.yaml`, and writes the CSV.

Field resolution per `intermediate_mapping` (same contract as json_to_csv.py):
  • a dotted JSON path (e.g. "facility.facilityId") -> extracted from _source
  • "external mapping" / "????" / other unresolved   -> external_mapping() stub
  • a bare literal (e.g. "C360")                     -> used as a constant

Feed-specific logic (derived columns, reference-data lookups, filtering)
belongs HERE — each feed owns its transform script; shared plumbing stays in
json_to_csv.py.

Usage:
    python transform_facility_to_csv.py <input.jsonl> <output.csv> [fields.yaml]
"""
from __future__ import annotations
import csv
import json
import os
import re
import sys

import yaml

# Works both as a plain script (script dir on sys.path) and as an import from
# the src/ package root (pytest's pythonpath).
try:
    from json_transform.json_to_csv import coerce, get_path
except ImportError:  # pragma: no cover - script invocation path
    from json_to_csv import coerce, get_path

HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_FIELDS_YAML = os.path.join(HERE, "facility_fields.yaml")

# Unlike the CTRTrade template (anchored to trade./header. prefixes), a dotted
# mapping here is any word.word[...] chain — feeds differ in their root keys.
JSON_PATH_RE = re.compile(r"^[A-Za-z0-9_]+(?:\.[A-Za-z0-9_]+)+$")
UNRESOLVED = {"", "????", "missing field", "external mapping"}


def external_mapping(field: dict, record: dict):
    """Resolve a field that is NOT a direct JSON path (reference-data lookups,
    generated IDs). Stub for now — implement facility-specific lookups here."""
    return None


def resolve_field(field: dict, record: dict):
    im = (field.get("intermediate_mapping") or "").strip()
    if im.lower() in UNRESOLVED:
        return external_mapping(field, record)
    if JSON_PATH_RE.match(im):
        return get_path(record, im)
    return im  # bare literal constant


def iter_source_records(jsonl_path: str):
    """Yield each record from an es_lookup JSONL file: one ES hit per line,
    the document under _source (falling back to the line itself for plain
    JSON-lines input)."""
    with open(jsonl_path, encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            hit = json.loads(line)
            yield hit.get("_source", hit) if isinstance(hit, dict) else hit


def convert(record: dict, fields: list[dict]):
    out, issues = {}, []
    for field in fields:
        value = resolve_field(field, record)
        if value is None or value == "":
            if field.get("required"):
                issues.append((field["name"], field.get("exception_level", "ERROR"),
                               field.get("exception_message", "missing required value")))
        out[field["name"]] = coerce(value, field.get("type", "str"))
    return out, issues


def main() -> int:
    if len(sys.argv) < 3:
        print(__doc__)
        return 2
    src_path, out_path = sys.argv[1], sys.argv[2]
    yaml_path = sys.argv[3] if len(sys.argv) > 3 else DEFAULT_FIELDS_YAML

    with open(yaml_path, encoding="utf-8") as handle:
        spec = yaml.safe_load(handle)
    fields = spec["fields"]
    columns = [f["name"] for f in fields]

    rows, all_issues = [], []
    for i, record in enumerate(iter_source_records(src_path)):
        rec, issues = convert(record, fields)
        rows.append(rec)
        all_issues.extend((i, *iss) for iss in issues)

    with open(out_path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=columns)
        w.writeheader()
        w.writerows(rows)

    print(f"Wrote {out_path}  ({len(rows)} record(s), {len(columns)} columns)")
    if all_issues:
        print(f"\n{len(all_issues)} field issue(s) (required value missing / unresolved):")
        for ridx, name, lvl, msg in all_issues:
            print(f"  record {ridx}  [{lvl:7}] {name}: {msg}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
