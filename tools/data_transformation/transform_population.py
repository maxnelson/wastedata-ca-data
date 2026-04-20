#!/usr/bin/env python3
"""
transform_population.py
Parse CA DOF E-4 xlsx → data/processed/population.json

Output format:
  {"Berkeley": {"county": "Alameda", "pop": {"2020": 127560, ..., "2025": 128348}}, ...}

Keys are CalRecycle-compatible canonical jurisdiction names.
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from lib import read_sheet

ROOT = Path(__file__).parent.parent.parent
SRC  = ROOT / "data/raw/population/e4-2021-2025.xlsx"
OUT  = ROOT / "data/processed/population.json"

# Cities where " City" is genuinely part of the name, not a DOF disambiguation suffix
REAL_CITY_WITH_CITY = {
    'Union City', 'Crescent City', 'Daly City', 'Foster City',
    'Cathedral City', 'Culver City', 'Suisun City', 'National City',
}

# DOF name → CalRecycle-compatible canonical name overrides
ALIASES = {
    'Angels City': 'Angels Camp',  # DOF uses "Angels City"; real name is Angels Camp
}

SKIP_EXACT = {
    'California', 'Incorporated State Total', 'Balance of State Total',
    'State Total', 'Incorporated', 'County Total', 'County/City',
}


def normalize(raw: str) -> str:
    """Strip trailing whitespace and disambiguation ' City' suffix where appropriate."""
    name = raw.strip()
    if name.endswith(' City') and name not in REAL_CITY_WITH_CITY:
        name = name[:-5]
    return ALIASES.get(name, name)


def main():
    print(f"Reading {SRC.name} ...")
    rows = read_sheet(str(SRC), 3)  # Sheet 3 = "Table 2 City County"

    # Find the header row to locate year columns dynamically
    year_cols = {}  # {col_index: "YYYY"}
    for row in rows:
        if row and row[0].strip() == 'County/City':
            for i, h in enumerate(row):
                if '/' in h:
                    try:
                        year_cols[i] = str(int(h.rsplit('/', 1)[-1]))
                    except ValueError:
                        pass
            break

    if not year_cols:
        sys.exit("ERROR: Could not find header row in E-4 sheet")

    print(f"  Years: {sorted(year_cols.values())}")

    result = {}
    current_county = None
    city_count = 0
    unincorp_count = 0

    for row in rows:
        if not row:
            continue
        name = row[0].strip()
        if not name:
            continue

        # County section header (e.g. "Alameda County")
        if name.endswith(' County') and 'Balance' not in name:
            current_county = name[:-7]
            continue

        # Unincorporated area row
        if name == 'Balance of County':
            if current_county:
                pop = _extract_pop(row, year_cols)
                if pop:
                    key = f"{current_county} County (Unincorporated)"
                    result[key] = {'county': current_county, 'pop': pop}
                    unincorp_count += 1
            continue

        # Skip metadata / subtotal rows
        if name in SKIP_EXACT or name.startswith('Table 2') or name.startswith('About'):
            continue

        # City row must have population data
        if not (len(row) > 1 and row[1].strip()):
            continue

        pop = _extract_pop(row, year_cols)
        if pop and current_county:
            result[normalize(name)] = {'county': current_county, 'pop': pop}
            city_count += 1

    print(f"  {city_count} cities + {unincorp_count} unincorporated areas")

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(result, indent=2))
    print(f"  → {OUT}")


def _extract_pop(row, year_cols):
    pop = {}
    for i, year in year_cols.items():
        if i < len(row) and row[i].strip():
            try:
                pop[year] = int(float(row[i]))
            except ValueError:
                pass
    return pop


if __name__ == '__main__':
    main()
