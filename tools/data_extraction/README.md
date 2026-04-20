# CalRecycle Waste Characterization Data Downloader

Downloads waste composition data (by material type) for all jurisdictions within a California county from CalRecycle's Waste Characterization Tool.

## Setup

```bash
# Create a directory for the tool
mkdir calrecycle-downloader && cd calrecycle-downloader

# Copy fetch-waste-data.js into this directory, then:
npm init -y
npm install axios cheerio
```

## Usage

```bash
# Step 1: Discover all available counties and their IDs
node fetch-waste-data.js --discover

# Step 2: Download data for a specific county
node fetch-waste-data.js "Alameda"
node fetch-waste-data.js "San Francisco"
node fetch-waste-data.js "Los Angeles" --delay 10000
```

## Options

| Flag | Description | Default |
|------|-------------|---------|
| `--discover` | List all counties | — |
| `--delay <ms>` | Milliseconds between requests | 7000 |
| `--output <dir>` | Output directory | `./data/waste-characterization` |

## Output Structure

```
data/waste-characterization/
  counties.json                              # County list (from --discover)
  alameda/
    commercial/
      alameda-countywide_commercial.xlsx     # Material breakdown, all streams
      berkeley_commercial.xlsx
      oakland_commercial.xlsx
      ...
    residential/
      alameda-countywide_residential.xlsx    # Residential disposal composition
      berkeley_residential.xlsx
      oakland_residential.xlsx
      ...
```

## Debugging

The script tries multiple URL patterns for the Excel export endpoint since we
can't know the exact pattern without testing against the live site. If the first
attempt fails, it will:

1. Try several common ASP.NET export URL patterns
2. Inspect the page HTML for export links
3. Save debug HTML files so you can manually identify the right pattern

### If exports fail:

1. Open the CalRecycle tool in your browser:
   https://www2.calrecycle.ca.gov/WasteCharacterization/MaterialTypeStreams
2. Open browser DevTools (F12) → Network tab
3. Select a county/jurisdiction and click "Export to Excel"
4. In the Network tab, find the download request and copy its URL
5. Update the `exportUrls` array in `downloadExcel()` with the correct pattern

## Rate Limiting

The default 7-second delay between requests means a county with 20 jurisdictions
takes about 5 minutes (2 downloads × 20 jurisdictions × 7 seconds). This is
intentionally conservative to be respectful to public infrastructure.

All 58 counties at this rate would take several hours. Consider running them in
batches over a few sessions.
