#!/usr/bin/env python3
"""
build_processed.py
Join disposal + population + characterization → data/processed/jurisdictions.json

For each jurisdiction with Q1 2024 disposal data, computes:
  - population (matched from DOF E-4 by name)
  - per-capita lbs/person/day (Q1 of latest available year, 90-day quarter)
  - YoY % change vs same quarter prior year
  - characterization categories (if downloaded)

Run after all three transform scripts have completed.
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from lib import slugify

ROOT      = Path(__file__).parent.parent.parent
PROCESSED = ROOT / "data/processed"

LATEST_YEAR  = 2024
DAYS_Q1      = 90       # January + February + March
LBS_PER_TON  = 2000

# CalRecycle jurisdiction name → DOF population key overrides
# Add entries here when a join fails due to naming mismatch.
JURS_TO_POP_ALIASES = {
    # Example: 'Unincorporated Alameda County': 'Alameda County (Unincorporated)',
}


def load(path):
    return json.loads(path.read_text()) if path.exists() else {}


def per_capita(total_tons, population, days=DAYS_Q1):
    """Compute lbs per person per day for a quarterly disposal total."""
    if not population:
        return None
    return round((total_tons * LBS_PER_TON) / population / days, 2)


def main():
    disposal   = load(PROCESSED / "disposal/by_jurisdiction.json")
    county_map = load(PROCESSED / "disposal/jurisdiction_county_map.json")
    pop_data   = load(PROCESSED / "population.json")
    char_dir   = PROCESSED / "characterization"

    if not disposal:
        sys.exit("ERROR: disposal/by_jurisdiction.json missing — run transform_disposal.py first")
    if not pop_data:
        print("WARN: population.json missing — per-capita will be null for all jurisdictions")

    print(f"Building jurisdictions.json ...")
    print(f"  Disposal jurisdictions:    {len(disposal)}")
    print(f"  Population entries:        {len(pop_data)}")
    char_count = len(list(char_dir.glob("*.json"))) if char_dir.exists() else 0
    print(f"  Characterization files:    {char_count}")

    # Lowercase lookup map for fuzzy population matching
    pop_lookup = {k.lower(): k for k in pop_data}

    result = {}
    pop_matched   = 0
    char_matched  = 0
    no_q1_latest  = 0

    for jurs_name, records in sorted(disposal.items()):
        # Derive county from the county map
        county = county_map.get(jurs_name, '')

        def get_q(year, quarter):
            return next((r for r in records
                         if r['year'] == year and r['quarter'] == quarter), None)

        q1_latest = get_q(LATEST_YEAR, 1)
        q1_prior  = get_q(LATEST_YEAR - 1, 1)

        if not q1_latest:
            no_q1_latest += 1
            continue

        # Population — try exact name, then alias, then case-insensitive
        pop_key = JURS_TO_POP_ALIASES.get(jurs_name, jurs_name)
        canon   = pop_data.get(pop_key) or pop_data.get(pop_lookup.get(pop_key.lower(), ''))
        population = canon['pop'].get(str(LATEST_YEAR)) if canon else None
        if population:
            pop_matched += 1

        # Year-over-year change
        yoy = None
        if q1_prior and q1_prior['total']:
            delta = q1_latest['total'] - q1_prior['total']
            yoy   = round(delta / q1_prior['total'] * 100, 2)

        entry = {
            'name':          jurs_name,
            'slug':          slugify(jurs_name),
            'county':        county,
            'pop2024':       population,
            'q1Total2024':   q1_latest['total'],
            'q1Total2023':   q1_prior['total'] if q1_prior else None,
            'yoy':           yoy,
            'perCapita':     per_capita(q1_latest['total'], population),
            'hasCharacterization': False,
        }

        # Characterization — match by slug
        char_file = char_dir / f"{slugify(jurs_name)}.json" if char_dir.exists() else None
        if char_file and char_file.exists():
            char = json.loads(char_file.read_text())
            entry['hasCharacterization'] = True
            entry['commercial']  = char.get('commercial', {}).get('categories', {})
            if 'residential' in char:
                entry['residential'] = char['residential'].get('categories', {})
            char_matched += 1

        result[jurs_name] = entry

    pop_total  = sum(1 for v in result.values() if v['pop2024'])
    char_total = sum(1 for v in result.values() if v['hasCharacterization'])
    print(f"\n  Jurisdictions written:     {len(result)}")
    print(f"  Skipped (no Q1 {LATEST_YEAR} data): {no_q1_latest}")
    print(f"  Population matched:        {pop_total} / {len(result)}")
    print(f"  Characterization matched:  {char_total} / {len(result)}")

    out = PROCESSED / "jurisdictions.json"
    out.write_text(json.dumps(result, indent=2))
    print(f"\n  → {out}")

    # Print a sample for spot-checking
    sample_keys = [k for k in result if k in ('Berkeley', 'Oakland', 'Los Angeles')]
    for k in sample_keys:
        v = result[k]
        print(f"\n  Sample — {k}:")
        print(f"    county:      {v['county']}")
        print(f"    pop2024:     {v['pop2024']:,}" if v['pop2024'] else "    pop2024:     None")
        print(f"    q1Total2024: {v['q1Total2024']:,.1f} tons")
        print(f"    yoy:         {v['yoy']}%")
        print(f"    perCapita:   {v['perCapita']} lbs/person/day")
        print(f"    hasChar:     {v['hasCharacterization']}")


if __name__ == '__main__':
    main()
