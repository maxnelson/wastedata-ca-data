#!/usr/bin/env python3
"""
transform_characterization.py
Parse commercial + residential waste characterization xlsx files
→ data/processed/characterization/{jurisdiction-slug}.json

Output per file:
  {
    "commercial": {
      "total": 64707.1,
      "categories": {"Organic": {"tons": 25944.9, "pct": 40.1}, ...}
    },
    "residential": {
      "total": 15234.0,
      "categories": {"Organic": {"tons": 7800.0, "pct": 51.2}, ...}
    }
  }

Commercial: col 0=Material Category, col 4=Total Disposed Tons, col 5=Material Tons Disposed
Residential: col 0=Material Category, col 7=Total Residential Tons (summed across all rows)
"""
import json
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from lib import read_sheet, to_float, slugify

ROOT    = Path(__file__).parent.parent.parent
SRC     = ROOT / "data/raw/waste-characterization"
OUT_DIR = ROOT / "data/processed/characterization"

# CalRecycle Material Category → 8 app display categories
# Discovered by inspecting berkeley_commercial.xlsx column A values
CATEGORY_MAP = {
    'Other Organic':                   'Organic',
    'Paper':                           'Paper & Cardboard',
    'Plastic':                         'Plastic',
    'Inerts and Other':                'Construction & Inerts',
    'Metal':                           'Metal',
    'Glass':                           'Glass',
    'Special Waste':                   'Special Waste',
    'Electronics':                     'Special Waste',   # ~1%, merged
    'Household Hazardous Waste (HHW)': 'Special Waste',   # ~0.2%, merged
    'Mixed Residue':                   'Mixed Residue',
}

# Canonical app category order (matches Home.jsx CATEGORIES)
APP_CATEGORIES = [
    'Organic', 'Paper & Cardboard', 'Plastic', 'Construction & Inerts',
    'Metal', 'Mixed Residue', 'Special Waste', 'Glass',
]


def find_header_row(rows):
    """Return the index of the first row whose col-0 value is 'Material Category'."""
    for i, row in enumerate(rows):
        if row and row[0].strip() == 'Material Category':
            return i
    return None


def parse_commercial(xlsx_path):
    """
    Aggregate Material Tons Disposed (col 5) by Material Category (col 0).
    Total Disposed Tons (col 4) is constant across all rows for a jurisdiction.
    Returns (categories_dict, total_tons) or (None, None) on failure.
    """
    rows = read_sheet(str(xlsx_path), 2)  # Sheet 2 = data sheet
    header_idx = find_header_row(rows)
    if header_idx is None:
        return None, None

    cat_tons = defaultdict(float)
    total_disposed = None
    unknown = set()

    for row in rows[header_idx + 1:]:
        if not row or not row[0].strip():
            continue
        calrecycle_cat = row[0].strip()
        if calrecycle_cat == 'Material Category':
            continue

        total    = to_float(row[4] if len(row) > 4 else '')
        mat_tons = to_float(row[5] if len(row) > 5 else '')

        if total > 0 and total_disposed is None:
            total_disposed = total

        app_cat = CATEGORY_MAP.get(calrecycle_cat)
        if app_cat:
            cat_tons[app_cat] += mat_tons
        else:
            unknown.add(calrecycle_cat)

    if unknown:
        print(f"    WARN unmapped categories in {xlsx_path.name}: {unknown}")

    if not total_disposed:
        return None, None

    categories = {
        cat: {
            'tons': round(cat_tons.get(cat, 0.0), 1),
            'pct':  round(cat_tons.get(cat, 0.0) / total_disposed * 100, 2),
        }
        for cat in APP_CATEGORIES
    }
    return categories, round(total_disposed, 1)


def parse_residential(xlsx_path):
    """
    Aggregate Total Residential Tons (col 7) by Material Category (col 0).
    No single 'total' column exists — computed as sum of all category tons.
    Returns (categories_dict, total_tons) or (None, None) on failure.
    """
    rows = read_sheet(str(xlsx_path), 2)
    header_idx = find_header_row(rows)
    if header_idx is None:
        return None, None

    cat_tons = defaultdict(float)

    for row in rows[header_idx + 1:]:
        if not row or not row[0].strip():
            continue
        calrecycle_cat = row[0].strip()
        if calrecycle_cat == 'Material Category':
            continue

        res_tons = to_float(row[7] if len(row) > 7 else '')
        app_cat = CATEGORY_MAP.get(calrecycle_cat)
        if app_cat and res_tons:
            cat_tons[app_cat] += res_tons

    total = sum(cat_tons.values())
    if not total:
        return None, None

    categories = {
        cat: {
            'tons': round(cat_tons.get(cat, 0.0), 1),
            'pct':  round(cat_tons.get(cat, 0.0) / total * 100, 2),
        }
        for cat in APP_CATEGORIES
    }
    return categories, round(total, 1)


def main():
    county_dirs = sorted(d for d in SRC.iterdir() if d.is_dir())
    print(f"Found {len(county_dirs)} county directories")
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    written = 0
    skipped = 0

    for county_dir in county_dirs:
        comm_dir = county_dir / 'commercial'
        res_dir  = county_dir / 'residential'
        if not comm_dir.exists():
            continue

        for comm_file in sorted(comm_dir.glob("*_commercial.xlsx")):
            slug = comm_file.stem.replace('_commercial', '')

            # Skip countywide / sample / unincorporated aggregate files
            skip_keywords = ('countywide', 'sample', 'business-group')
            if any(kw in slug for kw in skip_keywords):
                continue

            comm_cats, comm_total = parse_commercial(comm_file)
            if not comm_cats:
                skipped += 1
                continue

            output = {'commercial': {'total': comm_total, 'categories': comm_cats}}

            res_file = res_dir / f"{slug}_residential.xlsx" if res_dir.exists() else None
            if res_file and res_file.exists():
                res_cats, res_total = parse_residential(res_file)
                if res_cats:
                    output['residential'] = {'total': res_total, 'categories': res_cats}

            (OUT_DIR / f"{slug}.json").write_text(json.dumps(output, indent=2))
            written += 1

    print(f"  Written: {written}  |  Skipped: {skipped}")
    print(f"  → {OUT_DIR}/")


if __name__ == '__main__':
    main()
