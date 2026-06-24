#!/usr/bin/env python3
"""
json_to_csv.py
==============
Read a source JSON file, build one CSV record per trade using the field
definitions in `fields.yaml`, and write the CSV.

For each field the value is resolved from its `intermediate_mapping`:
  • a JSON path  (e.g. "trade.book.bookId")          -> extracted from the JSON
  • "external mapping" / "????" / "missing CTR field"-> external_mapping() placeholder
  • a bare literal (e.g. "MUREXSA")                   -> used as a constant

The CSV columns are exactly the `name` values from fields.yaml, in order.

Usage:
    python json_to_csv.py [source.json] [fields.yaml] [out.csv]
Defaults: source_json.json, fields.yaml, output.csv  (alongside this script)
"""
from __future__ import annotations
import sys, os, re, json, csv
import yaml

HERE = os.path.dirname(os.path.abspath(__file__))
JSON_PATH_RE = re.compile(r"(?:trade|header)(?:\.[A-Za-z0-9_]+)+")
UNRESOLVED = {"", "????", "missing ctr field", "ctr generated", "ctr generated id"}


# ─────────────────────────────────────────────────────────────────────────
# PLACEHOLDER — external / reference-data mappings (to be defined later)
# ─────────────────────────────────────────────────────────────────────────
def external_mapping(field: dict, record: dict):
    """Resolve a field that is NOT a direct JSON path.

    Covers the spec's "external mapping" rows (eGL / RO / Legal-Entity lookups,
    CTR-generated IDs, and the unresolved "????" / "missing CTR field" rows).

    TODO: implement the real lookups here. For now it is a no-op stub so the
    pipeline runs end-to-end.

    Args:
        field:  the full field definition dict from fields.yaml
                (name, intermediate_mapping, source_file, type, ...)
        record: the source JSON record currently being converted
    Returns:
        the mapped value, or None if it cannot be resolved yet.
    """
    # e.g. dispatch on field["name"] or field["intermediate_mapping"] later:
    #   if field["name"] == "OWN_ENTITY_ID": return lookup_egl(record)
    #   if field["name"] == "DW_ID":         return next_ctr_id()
    return None


# ─────────────────────────────────────────────────────────────────────────
# JSON extraction
# ─────────────────────────────────────────────────────────────────────────
def get_path(record: dict, dotted: str):
    """Navigate a dotted JSON path; return None if any segment is absent."""
    cur = record
    for seg in dotted.split("."):
        if isinstance(cur, dict) and seg in cur:
            cur = cur[seg]
        else:
            return None
    return cur


def resolve_field(field: dict, record: dict):
    """Return (value, how) for one field of one record."""
    im = (field.get("intermediate_mapping") or "").strip()
    low = im.lower()

    if low == "external mapping" or low in UNRESOLVED:
        return external_mapping(field, record), "external"

    m = JSON_PATH_RE.search(im)          # embedded or pure JSON path
    if m:
        return get_path(record, m.group(0)), "json_path"

    return im, "constant"                # bare literal, e.g. "MUREXSA"


# ─────────────────────────────────────────────────────────────────────────
# light type coercion for CSV output
# ─────────────────────────────────────────────────────────────────────────
def coerce(value, ftype: str):
    if value is None:
        return ""                        # ERROR/missing -> written as null/empty
    try:
        if ftype == "int":
            return int(value)
        if ftype == "decimal":
            return float(value)
        if ftype == "bool":
            return bool(value)
    except (ValueError, TypeError):
        pass                             # leave as-is if it will not coerce
    return value


# ─────────────────────────────────────────────────────────────────────────
# record iteration + conversion
# ─────────────────────────────────────────────────────────────────────────
def iter_records(doc):
    """Yield each source trade record, handling the Elasticsearch wrapper,
    a bare list, or a single object."""
    if isinstance(doc, dict) and "hits" in doc and isinstance(doc["hits"], dict):
        for hit in doc["hits"].get("hits", []):
            yield hit.get("_source", hit)
    elif isinstance(doc, list):
        yield from doc
    else:
        yield doc


def convert(record: dict, fields: list[dict]):
    """Build one output record (dict name->value) and a list of issues."""
    out, issues = {}, []
    for field in fields:
        value, how = resolve_field(field, record)
        if value is None or value == "":
            if field.get("required"):
                issues.append((field["name"], field.get("exception_level", "ERROR"),
                               field.get("exception_message", "missing required value")))
        out[field["name"]] = coerce(value, field.get("type", "str"))
    return out, issues


def main():
    src_path = sys.argv[1] if len(sys.argv) > 1 else os.path.join(HERE, "source_json.json")
    yaml_path = sys.argv[2] if len(sys.argv) > 2 else os.path.join(HERE, "fields.yaml")
    out_path = sys.argv[3] if len(sys.argv) > 3 else os.path.join(HERE, "output.csv")

    spec = yaml.safe_load(open(yaml_path, encoding="utf-8"))
    fields = spec["fields"]
    columns = [f["name"] for f in fields]          # CSV columns = field names, in order

    doc = json.load(open(src_path, encoding="utf-8"))

    rows, all_issues = [], []
    for i, record in enumerate(iter_records(doc)):
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


if __name__ == "__main__":
    main()
