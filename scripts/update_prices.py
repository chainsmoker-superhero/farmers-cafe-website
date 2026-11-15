import os, sys, json, re, time, requests
from collections import defaultdict
from datetime import datetime, timezone

API_KEY = os.environ.get("DATA_GOV_API_KEY")
if not API_KEY:
    sys.exit("Error: DATA_GOV_API_KEY environment variable is not set.")

RESOURCE_ID = "9ef84268-d588-465a-a308-a864a43d0070"
URL = f"https://api.data.gov.in/resource/{RESOURCE_ID}"
# Default to 250 instead of 1000: data.gov.in servers frequently time out on limit=1000
DEFAULT_LIMIT = 250
OUT_DIR = "data"

session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
})

def fetch_chunk(offset, limit=DEFAULT_LIMIT, tries=2, timeout_sec=12):
    """Fetch a single chunk. Drops hanging calls fast (12s instead of 90s) and restarts."""
    for attempt in range(1, tries + 1):
        try:
            print(f"Fetching offset {offset} (attempt {attempt}/{tries})...", end="", flush=True)
            start_t = time.time()
            r = session.get(URL, params={
                "api-key": API_KEY,
                "format": "json",
                "limit": limit,
                "offset": offset,
            }, timeout=(5, timeout_sec))
            r.raise_for_status()
            data = r.json()
            elapsed = time.time() - start_t
            if data.get("status") != "ok":
                raise ValueError(f"API status: {data.get('status')} {data.get('message')}")

            records = data.get("records", [])
            print(f" OK ({len(records)} records in {elapsed:.1f}s)")
            return data
        except requests.exceptions.Timeout:
            print(f" HUNG (no reply in {timeout_sec}s). Dropping connection and restarting fresh call...")
            time.sleep(1)
        except Exception as e:
            print(f" FAILED: {e}")
            if attempt < tries:
                time.sleep(2)
    return None

# 1. Fetch everything with fast-timeout & deferred queue for hanging calls
raw, offset = [], 0
total_reported = None
deferred_offsets = []

while True:
    data = fetch_chunk(offset, DEFAULT_LIMIT, tries=2, timeout_sec=12)
    if not data:
        # Don't stall the whole pipeline: defer this chunk and continue with the next
        print(f"  -> Offset {offset} was unresponsive. Deferring to end of queue and continuing...")
        deferred_offsets.append(offset)
        offset += DEFAULT_LIMIT
        if total_reported and offset >= total_reported:
            break
        continue

    batch = data.get("records", [])
    batch_total = int(data.get("total", 0))
    if batch_total > 0 and (total_reported is None or batch_total > total_reported):
        total_reported = batch_total

    raw.extend(batch)
    offset += len(batch)
    
    total_display = total_reported if total_reported is not None else "unknown"
    print(f"Progress: {len(raw)}/{total_display} records")

    # If the API returned no records for this batch, we reached the end of available stream
    if not batch:
        print(f"API returned empty batch at offset {offset}. Reached end of available stream.")
        break

    if total_reported and offset >= total_reported:
        print(f"Reached reported total of {total_reported} records.")
        break

    # Polite delay to prevent rate-limiting by data.gov.in WAF
    time.sleep(0.35)

# Process any deferred chunks that were deferred earlier
if deferred_offsets:
    print(f"\n--- Retrying {len(deferred_offsets)} deferred chunk(s) ---")
    for def_offset in deferred_offsets:
        data = fetch_chunk(def_offset, DEFAULT_LIMIT, tries=3, timeout_sec=20)
        if data:
            batch = data.get("records", [])
            raw.extend(batch)
            print(f"  -> Recovered deferred offset {def_offset} (+{len(batch)} records, total now: {len(raw)})")
        else:
            print(f"  -> Could not recover deferred offset {def_offset}. Skipping.")

# Fetch must have substantial data, otherwise keep old data
if len(raw) < 1000:
    sys.exit(f"Suspiciously incomplete fetch ({len(raw)} records). Keeping old data.")

# 2. Clean rows
def clean(r):
    try:
        return {
            "state": r["state"].strip(),
            "district": r["district"].strip(),
            "market": r["market"].strip(),
            "commodity": r["commodity"].strip(),
            "variety": (r.get("variety") or "").strip(),
            "grade": (r.get("grade") or "").strip(),
            "date": datetime.strptime(r["arrival_date"], "%d/%m/%Y").date().isoformat(),
            "min": float(r["min_price"]),
            "max": float(r["max_price"]),
            "modal": float(r["modal_price"]),
        }
    except (KeyError, ValueError, TypeError, AttributeError):
        return None

rows, skipped, seen = [], 0, set()
for r in raw:
    c = clean(r)
    if c is None:
        skipped += 1
        continue
    key = (c["state"], c["district"], c["market"], c["commodity"],
           c["variety"], c["grade"], c["date"])
    if key in seen:          # drop exact duplicates
        continue
    seen.add(key)
    rows.append(c)

print(f"Clean rows: {len(rows)}, skipped bad rows: {skipped}")

# 2.5 Enrich with Latitude & Longitude from persistent cache & geocoder
GEO_CACHE_FILE = f"{OUT_DIR}/mandi_coordinates_cache.json"
geo_cache = {}
if os.path.exists(GEO_CACHE_FILE):
    try:
        with open(GEO_CACHE_FILE, "r", encoding="utf-8") as f:
            geo_cache = json.load(f)
    except Exception as e:
        print(f"Warning: could not read {GEO_CACHE_FILE}: {e}")

def clean_town(market_name):
    s = re.sub(r"(?i)\bkrishi\s+upaj\s+mandi\s+samiti\b", "", market_name)
    s = re.sub(r"(?i)\bkrishi\s+upaj\s+mandi\b", "", s)
    s = re.sub(r"(?i)\bapmc\b", "", s)
    s = re.sub(r"\(.*?\)", "", s)
    s = re.sub(r"[^a-zA-Z\s]", " ", s)
    return s.strip()

def geocode_new_market(state, district, market):
    town = clean_town(market)
    queries = [
        f"{town} {district} {state} India",
        f"{town} {district} India",
        f"{town} {state} India",
        f"{district} {state} India",
    ]
    # Try Photon first (fast)
    for q in queries:
        try:
            url = "https://photon.komoot.io/api/?q=" + requests.utils.quote(q) + "&limit=1"
            req_headers = {"User-Agent": "FarmerGPT/1.0"}
            r = requests.get(url, headers=req_headers, timeout=4)
            if r.status_code == 200:
                features = r.json().get("features", [])
                if features:
                    c = features[0]["geometry"]["coordinates"]
                    return [round(c[1], 4), round(c[0], 4)]
        except Exception:
            pass
        time.sleep(0.1)

    # Fallback to Nominatim
    for q in queries:
        try:
            url = "https://nominatim.openstreetmap.org/search?q=" + requests.utils.quote(q) + "&format=json&limit=1"
            req_headers = {"User-Agent": "FarmerGPT-Crawler/1.0 (contact@farmerscafe.co.in)"}
            r = requests.get(url, headers=req_headers, timeout=5)
            if r.status_code == 200:
                data = r.json()
                if data and len(data) > 0:
                    return [round(float(data[0]["lat"]), 4), round(float(data[0]["lon"]), 4)]
        except Exception:
            pass
        time.sleep(1.0)
    return None

# Find any unmapped markets in current run
unmapped = set()
for c in rows:
    key = f"{c['state']}|{c['district']}|{c['market']}"
    if key not in geo_cache:
        unmapped.add((c['state'], c['district'], c['market']))

if unmapped:
    print(f"Resolving coordinates for {len(unmapped)} new market(s)...")
    cache_updated = False
    for state, district, market in sorted(list(unmapped))[:30]:  # limit to 30 per run so CI finishes fast
        coords = geocode_new_market(state, district, market)
        if coords:
            geo_cache[f"{state}|{district}|{market}"] = coords
            cache_updated = True
            print(f"  -> Geocoded {market} ({district}, {state}): {coords}")
        time.sleep(0.3)

    if cache_updated:
        tmp_cache = GEO_CACHE_FILE + ".tmp"
        with open(tmp_cache, "w", encoding="utf-8") as f:
            json.dump(geo_cache, f, indent=2)
        os.replace(tmp_cache, GEO_CACHE_FILE)

# Attach lat/lon to each row
enriched_count = 0
for c in rows:
    key = f"{c['state']}|{c['district']}|{c['market']}"
    coords = geo_cache.get(key)
    if coords:
        c["lat"] = coords[0]
        c["lon"] = coords[1]
        enriched_count += 1

print(f"Enriched {enriched_count}/{len(rows)} records with lat/long.")

# 3. Group by state
by_state = defaultdict(list)
for c in rows:
    by_state[c["state"]].append(c)

if len(rows) < 1000 or len(by_state) < 5:
    sys.exit(f"Suspiciously small result ({len(rows)} rows, {len(by_state)} states). Keeping old data.")

# 4. Write files
def slug(s):
    return re.sub(r"[^a-z0-9]+", "-", s.lower()).strip("-")

def write_json(path, obj):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, separators=(",", ":"))
    os.replace(tmp, path)   # atomic, never leaves a half-written file

now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
os.makedirs(f"{OUT_DIR}/states", exist_ok=True)

index = {"updated_at": now, "total_records": len(rows), "states": []}

for state in sorted(by_state):
    recs = sorted(by_state[state],
                  key=lambda x: (x["district"], x["market"], x["commodity"]))
    fname = f"{slug(state)}.json"
    write_json(f"{OUT_DIR}/states/{fname}", {
        "updated_at": now,
        "state": state,
        "count": len(recs),
        "unit_note": "Prices in INR per quintal; some fruit and vegetable markets report per kg",
        "records": recs,
    })
    index["states"].append({
        "state": state,
        "file": f"states/{fname}",
        "count": len(recs),
        "markets": len({r["market"] for r in recs}),
        "commodities": len({r["commodity"] for r in recs}),
        "latest_date": max(r["date"] for r in recs),
    })

write_json(f"{OUT_DIR}/index.json", index)
print(f"Done: {len(by_state)} states, {len(rows)} records written to {OUT_DIR}/")
