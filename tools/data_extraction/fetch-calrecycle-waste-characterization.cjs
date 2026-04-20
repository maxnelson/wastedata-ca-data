#!/usr/bin/env node

/**
 * CalRecycle Waste Characterization Data Downloader (v2)
 *
 * Downloads commercial material type and residential waste composition data
 * from CalRecycle's Waste Characterization Tool for all jurisdictions
 * within a specified California county.
 *
 * Usage:
 *   node fetch-waste-data.js "Alameda"
 *   node fetch-waste-data.js "San Francisco" --delay 10000
 *   node fetch-waste-data.js --discover
 *   node fetch-waste-data.js "Alameda" --format json
 *
 * Dependencies:
 *   npm install axios cheerio
 */

const axios = require("axios");
const cheerio = require("cheerio");
const fs = require("fs");
const path = require("path");

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const CONFIG = {
  baseUrl: "https://www2.calrecycle.ca.gov/WasteCharacterization",
  delayMs: 7000,
  outputDir: path.join(__dirname, "..", "..", "data", "raw", "waste-characterization"),
  maxRetries: 3,
  retryDelayMs: 10000,
  timeoutMs: 30000,
  userAgent:
    "TrashData-CA/1.0 (California Waste Data Visualization Project; educational/non-commercial use)",
  studyID: "104",
};

// ---------------------------------------------------------------------------
// Known endpoints (discovered via HAR + HTML analysis of the live site)
// ---------------------------------------------------------------------------

const ENDPOINTS = {
  mainPage: "/MaterialTypeStreams",
  jurisdictions: "/_LocalGovernmentsByCounty",
  exportMaterialTypes: "/_ExportToExcelMaterialTypeStreams",
  exportResidential: "/_ExportToExcelResidentialStreams",
  jsonMaterialTypes: "/_MaterialTypeStreamsGridData",
  jsonResidential: "/_ResidentialStreamsGridData",
};

// Column definitions for Excel export (field|header format, matching the Kendo grid)
const MATERIAL_TYPE_COLUMNS = [
  "MaterialTypeID| ",
  "MaterialTypeCategoryNameGroup|Material Category",
  "MaterialTypeName|Material Type",
  "TonsDisposed|Disposed Tons",
  "PercentDisposed|%",
  "TonsCurbsideRecycle|Curbside Recycle Tons",
  "PercentCurbsideRecycle|%",
  "TonsCurbsideOrganics|Curbside Organics Tons",
  "PercentCurbsideOrganics|%",
  "TonsOtherDiversion|Other Diversion Tons",
  "PercentOtherDiversion|%",
  "TonsTotalGeneration|Total Generation Tons",
  "PercentTotalGeneration|%",
].join(",");

// Column definitions confirmed via HAR capture from live site (studyID 103)
const RESIDENTIAL_COLUMNS = [
  "MaterialTypeCategoryNameGroup|Material Type Category Name Group",
  "MaterialTypeName|Material Type",
  "TonsSingleFamily|Single Family Tons",
  "FactorSingleFamily|Single Family Composition",
  "TonsMultiFamily|Multi Family Tons",
  "FactorMultiFamily|Multi Family Composition (statewide)",
  "TonsTotal|Total Residential Tons",
  "PercentTotal|Total Residential Composition",
].join(",");

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function slugify(name) {
  return name
    .toLowerCase()
    .replace(/[()]/g, "")
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-|-$/g, "");
}

function log(msg) {
  console.log(`[${new Date().toISOString().slice(11, 19)}] ${msg}`);
}

function logError(msg) {
  console.error(`[${new Date().toISOString().slice(11, 19)}] ERROR: ${msg}`);
}

function createClient() {
  return axios.create({
    baseURL: CONFIG.baseUrl,
    timeout: CONFIG.timeoutMs,
    headers: {
      "User-Agent": CONFIG.userAgent,
      Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
      "Accept-Language": "en-US,en;q=0.5",
    },
    maxRedirects: 5,
    validateStatus: (s) => s < 500,
  });
}

async function fetchRetry(client, url, opts = {}, attempt = 1) {
  try {
    const res = await client.get(url, opts);
    if (res.status >= 400) throw new Error(`HTTP ${res.status} for ${url}`);
    return res;
  } catch (err) {
    if (attempt < CONFIG.maxRetries) {
      const wait = CONFIG.retryDelayMs * attempt;
      logError(
        `Attempt ${attempt}/${CONFIG.maxRetries} failed: ${err.message}`,
      );
      log(`  Retrying in ${wait / 1000}s...`);
      await sleep(wait);
      return fetchRetry(client, url, opts, attempt + 1);
    }
    throw err;
  }
}

// ---------------------------------------------------------------------------
// Step 1: Discover counties
// ---------------------------------------------------------------------------

async function discoverCounties(client) {
  log("Fetching county list...");
  const res = await fetchRetry(client, ENDPOINTS.mainPage);
  const $ = cheerio.load(res.data);

  const counties = [];
  $("#CountyID option").each((_, el) => {
    const val = $(el).attr("value");
    const text = $(el).text().trim();
    if (val && text && !text.startsWith("--")) {
      counties.push({ id: val, name: text });
    }
  });

  // Filter out statewide study entries (real county IDs are 1-58)
  const real = counties.filter((c) => {
    const n = parseInt(c.id);
    return n >= 1 && n <= 58;
  });

  log(`  Found ${real.length} counties`);
  return real;
}

// ---------------------------------------------------------------------------
// Step 2: Discover jurisdictions within a county
// ---------------------------------------------------------------------------

async function discoverJurisdictions(client, countyId, countyName) {
  log(`Fetching jurisdictions for ${countyName}...`);

  // Primary: the AJAX endpoint used by the Kendo MultiSelect widget
  try {
    const res = await fetchRetry(
      client,
      `${ENDPOINTS.jurisdictions}?countyID=${countyId}`,
      {
        headers: {
          "X-Requested-With": "XMLHttpRequest",
          Accept: "application/json",
        },
      },
    );

    let data = typeof res.data === "string" ? JSON.parse(res.data) : res.data;

    if (Array.isArray(data) && data.length > 0) {
      const jurs = data.map((item) => ({
        id: String(item.ID || item.Id || item.id),
        name: (item.Name || item.name || "").trim(),
      }));
      log(`  Found ${jurs.length} jurisdictions`);
      return jurs;
    }
  } catch (err) {
    logError(`  AJAX jurisdiction fetch failed: ${err.message}`);
  }

  // Fallback: parse from the page HTML
  log("  Trying HTML fallback...");
  try {
    const res = await fetchRetry(
      client,
      `${ENDPOINTS.mainPage}?cy=${countyId}`,
    );
    const $ = cheerio.load(res.data);
    const jurs = [];
    $("#LocalGovernmentIDList option").each((_, el) => {
      const val = $(el).attr("value");
      const text = $(el).text().trim();
      if (val && text) jurs.push({ id: val, name: text });
    });
    if (jurs.length > 0) {
      log(`  Found ${jurs.length} jurisdictions via HTML`);
      return jurs;
    }
  } catch (err) {
    logError(`  HTML fallback failed: ${err.message}`);
  }

  return [];
}

// ---------------------------------------------------------------------------
// Step 3: Download data
// ---------------------------------------------------------------------------

async function downloadExcel(
  client,
  exportEndpoint,
  columnsParam,
  params,
  outputPath,
) {
  const qs = new URLSearchParams({
    sort: "MaterialTypeCategoryNameGroup-asc",
    filter: "~",
    group: "~",
    columns: columnsParam,
    studyID: CONFIG.studyID,
    countyID: params.countyId,
    localGovernmentIDList: params.jurisdictionId,
    localGovernmentIDListString: `[${params.jurisdictionId}]`,
  });

  try {
    const res = await client.get(`${exportEndpoint}?${qs.toString()}`, {
      responseType: "arraybuffer",
      validateStatus: (s) => s < 500,
      headers: {
        Accept:
          "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,*/*",
      },
    });

    if (res.status !== 200) {
      logError(`  HTTP ${res.status}`);
      return false;
    }

    const buf = Buffer.from(res.data);
    const isPK = buf.length > 4 && buf[0] === 0x50 && buf[1] === 0x4b;
    const ct = res.headers["content-type"] || "";

    if (
      !isPK &&
      !ct.includes("xlsx") &&
      !ct.includes("spreadsheet") &&
      !ct.includes("octet")
    ) {
      const preview = buf.toString("utf-8", 0, 200);
      if (preview.includes("<!DOCTYPE") || preview.includes("<html")) {
        logError("  Got HTML instead of Excel");
        return false;
      }
    }

    fs.mkdirSync(path.dirname(outputPath), { recursive: true });
    fs.writeFileSync(outputPath, buf);
    log(
      `  ✓ ${path.basename(outputPath)} (${(buf.length / 1024).toFixed(1)} KB)`,
    );
    return true;
  } catch (err) {
    logError(`  Download failed: ${err.message}`);
    return false;
  }
}

/**
 * Download a residential Excel export.
 * Parameters confirmed via HAR capture: studyID=103, distinct sort/group/columns from commercial.
 */
async function downloadResidentialExcel(client, params, outputPath) {
  const qs = new URLSearchParams({
    sort: "DisplayOrderMaterialTypeCategory-asc~DisplayOrderMaterialType-asc",
    filter: "~",
    group: "MaterialTypeCategoryNameGroup-asc",
    columns: RESIDENTIAL_COLUMNS,
    studyID: "103",
    countyID: params.countyId,
    localGovernmentIDList: params.jurisdictionId,
    localGovernmentIDListString: `[${params.jurisdictionId}]`,
  });

  try {
    const res = await client.get(
      `${ENDPOINTS.exportResidential}?${qs.toString()}`,
      {
        responseType: "arraybuffer",
        validateStatus: (s) => s < 500,
        headers: {
          Accept:
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,*/*",
        },
      },
    );

    if (res.status !== 200) {
      logError(`  HTTP ${res.status}`);
      return false;
    }

    const buf = Buffer.from(res.data);
    const isPK = buf.length > 4 && buf[0] === 0x50 && buf[1] === 0x4b;
    const ct = res.headers["content-type"] || "";

    if (
      !isPK &&
      !ct.includes("xlsx") &&
      !ct.includes("spreadsheet") &&
      !ct.includes("octet")
    ) {
      const preview = buf.toString("utf-8", 0, 200);
      if (preview.includes("<!DOCTYPE") || preview.includes("<html")) {
        logError("  Got HTML instead of Excel");
        return false;
      }
    }

    fs.mkdirSync(path.dirname(outputPath), { recursive: true });
    fs.writeFileSync(outputPath, buf);
    log(
      `  ✓ ${path.basename(outputPath)} (${(buf.length / 1024).toFixed(1)} KB)`,
    );
    return true;
  } catch (err) {
    logError(`  Download failed: ${err.message}`);
    return false;
  }
}

async function downloadJson(client, jsonEndpoint, params, outputPath) {
  const qs = new URLSearchParams({
    cy: params.countyId,
    lg: params.jurisdictionId,
  });

  try {
    const res = await fetchRetry(client, `${jsonEndpoint}?${qs.toString()}`, {
      headers: {
        Accept: "application/json",
        "X-Requested-With": "XMLHttpRequest",
      },
    });

    let data = typeof res.data === "string" ? JSON.parse(res.data) : res.data;
    const records = data.Data || data.data || data;

    if (!records || (Array.isArray(records) && records.length === 0)) {
      logError("  Empty JSON response");
      return false;
    }

    fs.mkdirSync(path.dirname(outputPath), { recursive: true });
    fs.writeFileSync(outputPath, JSON.stringify(records, null, 2));
    const count = Array.isArray(records) ? records.length : "?";
    log(`  ✓ ${path.basename(outputPath)} (${count} records)`);
    return true;
  } catch (err) {
    logError(`  JSON download failed: ${err.message}`);
    return false;
  }
}

// ---------------------------------------------------------------------------
// Step 4: Main orchestration
// ---------------------------------------------------------------------------

async function discoverMode(client) {
  console.log("\n=== CalRecycle — County Discovery ===\n");
  const counties = await discoverCounties(client);

  console.log(`  ID   | County`);
  console.log("  -----+---------------------------");
  for (const c of counties) console.log(`  ${c.id.padStart(4)} | ${c.name}`);
  console.log(`\nUsage: node fetch-waste-data.js "<county>"\n`);

  const out = path.join(CONFIG.outputDir, "counties.json");
  fs.mkdirSync(path.dirname(out), { recursive: true });
  fs.writeFileSync(out, JSON.stringify(counties, null, 2));
  log(`Saved to ${out}`);
}

async function downloadCounty(client, targetCounty, delayMs, format, only) {
  const downloading = only || "commercial + residential";
  console.log(`\n=== CalRecycle Waste Characterization Downloader ===`);
  console.log(
    `County: ${targetCounty} | Format: ${format} | Delay: ${delayMs / 1000}s | Data: ${downloading}\n`,
  );

  const counties = await discoverCounties(client);
  let county = counties.find(
    (c) => c.name.toLowerCase() === targetCounty.toLowerCase(),
  );

  if (!county) {
    const partial = counties.filter((c) =>
      c.name.toLowerCase().includes(targetCounty.toLowerCase()),
    );
    if (partial.length === 1) county = partial[0];
    else {
      logError(`County "${targetCounty}" not found.`);
      if (partial.length > 1)
        partial.forEach((m) => console.log(`  - ${m.name}`));
      else console.log("  Run with --discover to see all counties.");
      process.exit(1);
    }
  }

  log(`County: ${county.name} (ID: ${county.id})`);

  await sleep(delayMs);
  const jurisdictions = await discoverJurisdictions(
    client,
    county.id,
    county.name,
  );
  if (jurisdictions.length === 0) {
    logError("No jurisdictions found.");
    process.exit(1);
  }

  console.log(`\nJurisdictions (${jurisdictions.length}):`);
  jurisdictions.forEach((j, i) =>
    console.log(`  ${String(i + 1).padStart(3)}. ${j.name} (${j.id})`),
  );
  console.log();

  const countySlug = slugify(county.name);
  let success = 0,
    fail = 0;
  const download = format === "json" ? downloadJson : downloadExcel;

  const skipCommercial = only === "residential";
  const skipResidential = only === "commercial";

  for (let i = 0; i < jurisdictions.length; i++) {
    const j = jurisdictions[i];
    const jSlug = slugify(j.name);
    const tag = `[${i + 1}/${jurisdictions.length}]`;
    const params = { countyId: county.id, jurisdictionId: j.id };

    log(`${tag} ${j.name}`);

    // Commercial material types
    let commOk = true;
    if (!skipCommercial) {
      await sleep(delayMs);
      if (format === "json") {
        commOk = await downloadJson(
          client,
          ENDPOINTS.jsonMaterialTypes,
          params,
          path.join(
            CONFIG.outputDir,
            countySlug,
            "commercial",
            `${jSlug}_commercial.json`,
          ),
        );
      } else {
        commOk = await downloadExcel(
          client,
          ENDPOINTS.exportMaterialTypes,
          MATERIAL_TYPE_COLUMNS,
          params,
          path.join(
            CONFIG.outputDir,
            countySlug,
            "commercial",
            `${jSlug}_commercial.xlsx`,
          ),
        );
      }

      // If first download fails, try the other format before giving up
      if (!commOk && i === 0) {
        logError("First download failed — testing alternate format...");
        await sleep(2000);
        const altFormat = format === "json" ? "xlsx" : "json";
        let altOk;
        if (altFormat === "json") {
          altOk = await downloadJson(
            client,
            ENDPOINTS.jsonMaterialTypes,
            params,
            path.join(
              CONFIG.outputDir,
              countySlug,
              "commercial",
              `${jSlug}_commercial.json`,
            ),
          );
        } else {
          altOk = await downloadExcel(
            client,
            ENDPOINTS.exportMaterialTypes,
            MATERIAL_TYPE_COLUMNS,
            params,
            path.join(
              CONFIG.outputDir,
              countySlug,
              "commercial",
              `${jSlug}_commercial.xlsx`,
            ),
          );
        }
        if (altOk) {
          log(
            `  ${altFormat} format works! Re-run with: --format ${altFormat}`,
          );
        } else {
          logError(
            "Both formats failed. Check browser DevTools for current endpoint URLs.",
          );
        }
        process.exit(1);
      }
    }

    // Residential (uses dedicated function — different params than commercial)
    let resOk = true;
    if (!skipResidential) {
      await sleep(delayMs);
      if (format === "json") {
        resOk = await downloadJson(
          client,
          ENDPOINTS.jsonResidential,
          params,
          path.join(
            CONFIG.outputDir,
            countySlug,
            "residential",
            `${jSlug}_residential.json`,
          ),
        );
      } else {
        resOk = await downloadResidentialExcel(
          client,
          params,
          path.join(
            CONFIG.outputDir,
            countySlug,
            "residential",
            `${jSlug}_residential.xlsx`,
          ),
        );
      }
      if (!resOk) logError(`  Residential failed for ${j.name}`);
    }

    if (!skipCommercial && commOk) success++;
    if (!skipCommercial && !commOk) fail++;
    if (!skipResidential && !resOk) fail++;
    if (skipCommercial && resOk) success++;
    if (skipCommercial && !resOk) fail++;
  }

  console.log("\n" + "=".repeat(60));
  console.log(`Done! ${county.name}: ${success} ok, ${fail} failed`);
  console.log(`Output: ${path.join(CONFIG.outputDir, countySlug)}`);
  console.log("=".repeat(60) + "\n");
}

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

async function main() {
  const args = process.argv.slice(2);
  let target = null,
    delayMs = CONFIG.delayMs,
    discover = false,
    format = "xlsx";
  let only = null; // null = both, "commercial", or "residential"

  for (let i = 0; i < args.length; i++) {
    if (args[i] === "--discover") discover = true;
    else if (args[i] === "--residential") only = "residential";
    else if (args[i] === "--commercial") only = "commercial";
    else if (args[i] === "--delay" && args[i + 1]) {
      delayMs = parseInt(args[i + 1]);
      i++;
    } else if (args[i] === "--format" && args[i + 1]) {
      format = args[i + 1].toLowerCase();
      i++;
    } else if (args[i] === "--output" && args[i + 1]) {
      CONFIG.outputDir = args[i + 1];
      i++;
    } else if (!args[i].startsWith("--")) target = args[i];
  }

  if (!discover && !target) {
    console.log(`
CalRecycle Waste Characterization Data Downloader
=================================================
Usage:
  node fetch-waste-data.js "<county>"              Download a county
  node fetch-waste-data.js --discover              List all counties
Options:
  --residential        Download residential data only
  --commercial         Download commercial data only
  --format xlsx|json   Output format (default: xlsx)
  --delay <ms>         Delay between requests (default: ${CONFIG.delayMs}ms)
  --output <dir>       Output directory
Examples:
  node fetch-waste-data.js "Alameda"
  node fetch-waste-data.js "Alameda" --residential
  node fetch-waste-data.js "Alameda" --format json
  node fetch-waste-data.js "San Francisco" --delay 10000
`);
    process.exit(0);
  }

  const client = createClient();
  try {
    if (discover) await discoverMode(client);
    else await downloadCounty(client, target, delayMs, format, only);
  } catch (err) {
    logError(`Fatal: ${err.message}`);
    process.exit(1);
  }
}

main();
