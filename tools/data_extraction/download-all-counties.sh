#!/usr/bin/env bash
# download-all-counties.sh
#
# Downloads waste characterization data (commercial + residential) for all
# remaining California counties. Skips any county whose output directory
# already contains files.
#
# Usage:
#   bash tools/data_extraction/download-all-counties.sh
#   bash tools/data_extraction/download-all-counties.sh 2>&1 | tee logs/download.log
#
# Resume-safe: already-downloaded counties are detected by the presence of
# files in data/raw/waste-characterization/<county-slug>/ and skipped.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
FETCHER="$SCRIPT_DIR/fetch-calrecycle-waste-characterization.cjs"
DATA_DIR="$SCRIPT_DIR/../../data/raw/waste-characterization"

# All 58 CA counties in order — Alameda (1) is already complete, listed last
# so the skip logic handles it and it doesn't block progress on others.
COUNTIES=(
  "Alpine"
  "Amador"
  "Butte"
  "Calaveras"
  "Colusa"
  "Contra Costa"
  "Del Norte"
  "El Dorado"
  "Fresno"
  "Glenn"
  "Humboldt"
  "Imperial"
  "Inyo"
  "Kern"
  "Kings"
  "Lake"
  "Lassen"
  "Los Angeles"
  "Madera"
  "Marin"
  "Mariposa"
  "Mendocino"
  "Merced"
  "Modoc"
  "Mono"
  "Monterey"
  "Napa"
  "Nevada"
  "Orange"
  "Placer"
  "Plumas"
  "Riverside"
  "Sacramento"
  "San Benito"
  "San Bernardino"
  "San Diego"
  "San Francisco"
  "San Joaquin"
  "San Luis Obispo"
  "San Mateo"
  "Santa Barbara"
  "Santa Clara"
  "Santa Cruz"
  "Shasta"
  "Sierra"
  "Siskiyou"
  "Solano"
  "Sonoma"
  "Stanislaus"
  "Sutter"
  "Tehama"
  "Trinity"
  "Tulare"
  "Tuolumne"
  "Ventura"
  "Yolo"
  "Yuba"
  "Alameda"
)

# Mirrors the slugify() function in the fetcher script
slugify() {
  echo "$1" \
    | tr '[:upper:]' '[:lower:]' \
    | sed "s/[()]//g" \
    | sed "s/[^a-z0-9]\{1,\}/-/g" \
    | sed "s/^-\|-$//g"
}

has_files() {
  local dir="$1"
  [ -d "$dir" ] && [ -n "$(find "$dir" -type f \( -name "*.xlsx" -o -name "*.json" \) 2>/dev/null | head -1)" ]
}

TOTAL=${#COUNTIES[@]}
DONE=0
SKIPPED=0
FAILED=0
FAILED_COUNTIES=()

echo ""
echo "=================================================="
echo " CalRecycle Waste Characterization — Batch Download"
echo " Counties: $TOTAL"
echo " Started:  $(date)"
echo "=================================================="
echo ""

for county in "${COUNTIES[@]}"; do
  slug=$(slugify "$county")
  county_dir="$DATA_DIR/$slug"

  if has_files "$county_dir"; then
    echo "--- SKIP: $county (already downloaded)"
    SKIPPED=$((SKIPPED + 1))
    continue
  fi

  echo ""
  echo ">>> [$((DONE + SKIPPED + FAILED + 1))/$TOTAL] Starting: $county"
  echo "    $(date)"

  if node "$FETCHER" "$county"; then
    DONE=$((DONE + 1))
    echo "    ✓ $county complete"
  else
    FAILED=$((FAILED + 1))
    FAILED_COUNTIES+=("$county")
    echo "    ✗ $county FAILED — continuing with next county"
  fi
done

echo ""
echo "=================================================="
echo " Batch complete: $(date)"
echo " Downloaded: $DONE  |  Skipped: $SKIPPED  |  Failed: $FAILED"
if [ ${#FAILED_COUNTIES[@]} -gt 0 ]; then
  echo " Failed counties:"
  for c in "${FAILED_COUNTIES[@]}"; do echo "   - $c"; done
  echo " Re-run this script to retry failed counties."
fi
echo "=================================================="
echo ""
