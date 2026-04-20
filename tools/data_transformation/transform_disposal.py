#!/usr/bin/env python3
"""
transform_disposal.py
Parse CalRecycle disposal tonnage xlsx files (2019-2025)
→ data/processed/disposal/by_jurisdiction.json
→ data/processed/disposal/jurisdiction_county_map.json

Output format (by_jurisdiction.json):
  {"Berkeley": [{"year":2019,"quarter":1,"landfill":..,"total":..}, ...], ...}

Total disposed = Landfill + Transformation + EMSW (Green Material ADC tracked separately).
"""
import json
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from lib import read_sheet, to_float

ROOT = Path(__file__).parent.parent.parent
SRC  = ROOT / "data/raw/disposal-tonnage"
OUT  = ROOT / "data/processed/disposal"

# Column indices (0-based): A=Year, B=Quarter, C=Jurisdiction, D=County,
# E=Landfill, F=Transformation, G=EMSW, H=Green Material ADC
C_YEAR    = 0
C_QTR     = 1
C_JURS    = 2
C_COUNTY  = 3
C_LANDF   = 4
C_TRANS   = 5
C_EMSW    = 6
C_GREEN   = 7


def safe(row, i):
    return row[i] if i < len(row) else ''


def main():
    files = sorted(SRC.glob("calrecycle-disposal-*.xlsx"))
    if not files:
        sys.exit(f"ERROR: No disposal xlsx files found in {SRC}")

    print(f"Found {len(files)} disposal files")

    by_jurs = defaultdict(list)
    county_map = {}

    for f in files:
        print(f"  Parsing {f.name} ...")
        rows = read_sheet(str(f), 1)  # Sheet1

        for row in rows[1:]:  # skip header row
            jurs = safe(row, C_JURS).strip()
            if not jurs:
                continue
            try:
                year    = int(float(safe(row, C_YEAR)))
                quarter = int(float(safe(row, C_QTR)))
            except (ValueError, TypeError):
                continue

            county     = safe(row, C_COUNTY).strip()
            landfill   = to_float(safe(row, C_LANDF))
            transform  = to_float(safe(row, C_TRANS))
            emsw       = to_float(safe(row, C_EMSW))
            green_adc  = to_float(safe(row, C_GREEN))

            county_map[jurs] = county
            by_jurs[jurs].append({
                'year':           year,
                'quarter':        quarter,
                'landfill':       round(landfill,  2),
                'transformation': round(transform, 2),
                'emsw':           round(emsw,      2),
                'greenADC':       round(green_adc, 2),
                'total':          round(landfill + transform + emsw, 2),
            })

    # Sort chronologically within each jurisdiction
    for jurs in by_jurs:
        by_jurs[jurs].sort(key=lambda r: (r['year'], r['quarter']))

    unique = len(by_jurs)
    print(f"  {unique} unique jurisdictions across all years")

    OUT.mkdir(parents=True, exist_ok=True)

    out_jurs = OUT / "by_jurisdiction.json"
    out_jurs.write_text(json.dumps(dict(sorted(by_jurs.items())), indent=2))
    print(f"  → {out_jurs}")

    out_county = OUT / "jurisdiction_county_map.json"
    out_county.write_text(json.dumps(dict(sorted(county_map.items())), indent=2))
    print(f"  → {out_county}")


if __name__ == '__main__':
    main()
