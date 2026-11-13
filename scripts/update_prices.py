import os, sys, json, re, time, requests
from collections import defaultdict
from datetime import datetime, timezone

API_KEY = os.environ["DATA_GOV_API_KEY"]
RESOURCE_ID = "9ef84268-d588-465a-a308-a864a43d0070"
URL = f"https://api.data.gov.in/resource/{RESOURCE_ID}"
LIMIT = 1000
OUT_DIR = "data"

def get_page(offset, tries=5):
    for i in range(tries):
        try:
            r = requests.get(URL, params={
                "api-key": API_KEY, "format": "json",
                "limit": LIMIT, "offset": offset,
            }, timeout=90)
            r.raise_for_status()
            data = r.json()
            if data.get("status") != "ok":
                raise ValueError(f"API status: {data.get('status')} {data.get('message')}")
            return data
        except Exception as e:
            print(f"offset {offset}, attempt {i+1} failed: {e}")
            time.sleep(5 * (i + 1))
    sys.exit("API failed after retries. Keeping old data.")

# 1. Fetch everything
raw, offset, total = [], 0, None
while True:
    data = get_page(offset)
    total = int(data.get("total", 0))
    batch = data.get("records", [])
    raw.extend(batch)
    offset += len(batch)
    print(f"Fetched {len(raw)}/{total}")
    if not batch or offset >= total:
        break

# Fetch must be complete, otherwise keep old data
if total == 0 or len(raw) < total * 0.98:
    sys.exit(f"Incomplete fetch ({len(raw)}/{total}). Keeping old data.")

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
print(f"Done: {len(by_state)} states, {len(rows)} records")
