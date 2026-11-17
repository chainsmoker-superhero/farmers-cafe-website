#!/usr/bin/env python3
"""
Price History Pipeline for Mandi Data.
Maintains rolling 14-day price history per commodity and market series.
Outputs to: data/history/<state-slug>/<commodity-slug>.json
and data/history/<state-slug>/index.json
"""

import os
import sys
import re
import json
import statistics
import subprocess
from datetime import datetime, timezone
from collections import defaultdict
from typing import List, Dict, Any, Tuple, Optional, Set

OUT_DIR = "data"
HISTORY_DIR = f"{OUT_DIR}/history"


def slug(s: str) -> str:
    """Standard lowercase slug with hyphens."""
    return re.sub(r"[^a-z0-9]+", "-", s.lower()).strip("-")


def commodity_to_slug(raw_commodity: str) -> str:
    """
    Generate commodity slug directly from RAW commodity name.
    Example: 'Kabuli Chana(Chickpeas-White)' -> 'kabuli-chana-chickpeas-white'
    """
    return re.sub(r"[^a-z0-9]+", "-", raw_commodity.strip().lower()).strip("-")


def make_market_key(district: str, market: str) -> str:
    """
    Generate market key matching ApmcDataMapper.resolveMandi in FarmerGPT Android app.
    Formula: mandi_${slug(district)}_${slug(marketClean)}
    """
    market_clean = re.sub(r"(?i)\bapmc\b", "", market).strip()
    dist_slug = re.sub(r"[^a-z0-9]+", "_", district.strip().lower()).strip("_")
    mkt_slug = re.sub(r"[^a-z0-9]+", "_", market_clean.lower()).strip("_")
    return f"mandi_{dist_slug}_{mkt_slug}".rstrip("_")


def make_variety_slug(variety: str) -> str:
    """Generate variety slug for series key."""
    clean = re.sub(r"[^a-z0-9]+", "_", (variety or "").strip().lower()).strip("_")
    return clean if clean else "default"


def make_series_key(market_key: str, variety: str) -> str:
    """
    Series key = market_key + variety.
    Example: mandi_dhar_dhamnod__dollar_gram
    """
    return f"{market_key}__{make_variety_slug(variety)}"


def collapse_variety_grades(rows: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    Within one variety and day, collapse grades:
    1. Prefer FAQ rows if any are present.
    2. If several FAQ rows, use median modal and min of mins / max of maxes.
    3. If no FAQ rows, use median modal and min of mins / max of maxes across all available rows.
    """
    if not rows:
        return None

    # Check for FAQ rows (case-insensitive)
    faq_rows = [r for r in rows if (r.get("grade") or "").strip().upper() == "FAQ"]
    target_rows = faq_rows if faq_rows else rows

    modals = [float(r["modal"]) for r in target_rows]
    mins = [float(r["min"]) for r in target_rows]
    maxs = [float(r["max"]) for r in target_rows]

    # Median modal rounded to 1 decimal place (or 2)
    median_modal = round(float(statistics.median(modals)), 2)
    min_price = min(mins)
    max_price = max(maxs)

    # Use the record's own date field
    date_val = target_rows[0]["date"]

    point: Dict[str, Any] = {
        "d": date_val,
        "modal": median_modal,
        "min": min_price,
        "max": max_price,
    }

    # Optional arrival if present in data
    arrivals = [float(r["arrival"]) for r in target_rows if "arrival" in r and r["arrival"] is not None]
    if arrivals:
        point["arrival"] = round(sum(arrivals), 2)

    return point


def detect_market_key_collisions(records: List[Dict[str, Any]]) -> Dict[str, Set[Tuple[str, str]]]:
    """
    Detect market_key collisions (same key produced from different district or market strings).
    Warns if collisions found; raises ValueError if > 3 collisions.
    """
    key_to_pairs: Dict[str, Set[Tuple[str, str]]] = {}
    for r in records:
        dist = r["district"].strip()
        mkt = r["market"].strip()
        k = make_market_key(dist, mkt)
        key_to_pairs.setdefault(k, set()).add((dist, mkt))

    collisions = {k: pairs for k, pairs in key_to_pairs.items() if len(pairs) > 1}
    if collisions:
        print(f"WARNING: Detected {len(collisions)} market_key collision(s):")
        for k, pairs in sorted(collisions.items()):
            print(f"  Key '{k}' mapped to multiple distinct (district, market) pairs: {sorted(pairs)}")
        if len(collisions) > 3:
            raise ValueError(f"Too many market_key collisions ({len(collisions)} > 3). Aborting to prevent data corruption.")
    return collisions


MAX_FILE_BYTES = 500 * 1024  # 500 KB safety limit


def write_json_atomic(path: str, obj: Any, max_bytes: int = MAX_FILE_BYTES) -> None:
    """Atomic file write using .tmp and os.replace. Fails if file exceeds max_bytes."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, separators=(",", ":"))
    file_size = os.path.getsize(tmp)
    if file_size > max_bytes:
        os.remove(tmp)
        raise ValueError(f"File {path} exceeds size limit: {file_size} bytes > {max_bytes} bytes (500 KB).")
    os.replace(tmp, path)


def load_json(path: str) -> Optional[Dict[str, Any]]:
    """Safely load JSON if file exists."""
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"Warning: could not read {path}: {e}")
        return None


def update_state_history(state: str, records: List[Dict[str, Any]], updated_at: Optional[str] = None) -> Dict[str, Any]:
    """
    Process records for a single state, updating per-commodity history files.
    Maintains last 14 days per series, keeping atomic writes and idempotency.
    Returns summary dict for state history index.
    """
    if not records:
        return {"state": state, "series_count": 0, "commodities": [], "newest_date": None}

    detect_market_key_collisions(records)

    state_slug = slug(state)
    state_hist_dir = f"{HISTORY_DIR}/{state_slug}"
    os.makedirs(state_hist_dir, exist_ok=True)

    if not updated_at:
        updated_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Group incoming records by: commodity -> (market_key, variety, date) -> list of rows
    # Each commodity file contains all its series
    by_commodity: Dict[str, Dict[Tuple[str, str, str, str, str], List[Dict[str, Any]]]] = defaultdict(lambda: defaultdict(list))

    for r in records:
        raw_comm = r["commodity"].strip()
        comm_slug = commodity_to_slug(raw_comm)
        district = r["district"].strip()
        market = r["market"].strip()
        variety = (r.get("variety") or "").strip()
        m_key = make_market_key(district, market)
        date_str = r["date"].strip()

        # Group key: (comm_slug, raw_comm, m_key, variety, district, market, date_str)
        by_commodity[comm_slug][(m_key, variety, district, market, date_str)].append(r)

    total_series_count = 0
    all_dates: Set[str] = set()

    for comm_slug, groups in sorted(by_commodity.items()):
        file_path = f"{state_hist_dir}/{comm_slug}.json"
        existing_data = load_json(file_path)

        raw_comm_name = next(iter(groups.values()))[0]["commodity"].strip()

        if existing_data and isinstance(existing_data.get("series"), dict):
            series_dict = existing_data["series"]
        else:
            series_dict = {}

        # Collapse today's incoming records
        for (m_key, variety, district, market, date_str), rows in groups.items():
            all_dates.add(date_str)
            s_key = make_series_key(m_key, variety)
            collapsed_point = collapse_variety_grades(rows)
            if not collapsed_point:
                continue

            if s_key not in series_dict:
                series_dict[s_key] = {
                    "market_key": m_key,
                    "market": market,
                    "district": district,
                    "variety": variety,
                    "points": [],
                }

            # Idempotent merge into points: update date if exists, else append
            existing_points = series_dict[s_key].get("points", [])
            points_by_date = {p["d"]: p for p in existing_points}
            points_by_date[collapsed_point["d"]] = collapsed_point

            # Sort chronological ascending (newest last) and keep last 14 entries
            sorted_points = sorted(points_by_date.values(), key=lambda x: x["d"])[-14:]
            series_dict[s_key]["points"] = sorted_points
            series_dict[s_key]["last_date"] = sorted_points[-1]["d"] if sorted_points else None
            series_dict[s_key]["n_points"] = len(sorted_points)

        # Ensure all series have up-to-date metadata
        for s_val in series_dict.values():
            pts = s_val.get("points", [])
            s_val["n_points"] = len(pts)
            s_val["last_date"] = pts[-1]["d"] if pts else None

        # Order series: cluster by market_key, and within each market sort by n_points descending (best series first), then variety
        sorted_keys = sorted(
            series_dict.keys(),
            key=lambda k: (series_dict[k].get("market_key", ""), -series_dict[k].get("n_points", 0), series_dict[k].get("variety", ""))
        )
        sorted_series = {k: series_dict[k] for k in sorted_keys}
        total_series_count += len(sorted_series)

        # Write commodity history file
        comm_payload = {
            "commodity": raw_comm_name,
            "commodity_slug": comm_slug,
            "state": state,
            "unit": "INR/quintal",
            "updated_at": updated_at,
            "series": sorted_series,
        }
        write_json_atomic(file_path, comm_payload)

    # Write state history index.json
    newest_date = max(all_dates) if all_dates else None
    state_index_payload = {
        "state": state,
        "state_slug": state_slug,
        "updated_at": updated_at,
        "commodities": sorted(list(by_commodity.keys())),
        "series_count": total_series_count,
        "newest_date": newest_date,
    }
    write_json_atomic(f"{state_hist_dir}/index.json", state_index_payload)

    return state_index_payload


def backfill_from_git(state_slug: str = "madhya-pradesh", min_records: int = 1000) -> List[str]:
    """
    Backfill history only from commits whose record count for this state is >= min_records.
    Skips thin/incomplete commits.
    Returns list of used commit SHAs.
    """
    target_file = f"data/states/{state_slug}.json"
    used_commits: List[str] = []

    # Get git revisions modifying target_file in chronological order (oldest first)
    try:
        cmd = ["git", "log", "--reverse", "--format=%h", "--", target_file]
        res = subprocess.run(cmd, capture_output=True, text=True, check=True)
        commits = [c.strip() for c in res.stdout.strip().splitlines() if c.strip()]
    except Exception as e:
        print(f"Error querying git log for backfill: {e}")
        return []

    print(f"\n--- Scanning {len(commits)} git commit(s) for backfill (min_records={min_records}) ---")

    for commit in commits:
        try:
            show_cmd = ["git", "show", f"{commit}:{target_file}"]
            show_res = subprocess.run(show_cmd, capture_output=True, text=True, check=True)
            state_data = json.loads(show_res.stdout)
            records = state_data.get("records", [])
            state_name = state_data.get("state", "Madhya Pradesh")
            rec_count = len(records)

            if rec_count < min_records:
                print(f"  Commit {commit}: {rec_count} records (< {min_records}) -> SKIPPED")
                continue

            print(f"  Commit {commit}: {rec_count} records (>= {min_records}) -> PROCESSING...")
            updated_at = state_data.get("updated_at")
            update_state_history(state_name, records, updated_at=updated_at)
            used_commits.append(commit)
        except Exception as err:
            print(f"  Commit {commit}: failed to inspect: {err}")

    print(f"Backfill complete. Commits used: {used_commits}\n")
    return used_commits


def print_commodity_slug_table(records: List[Dict[str, Any]]) -> None:
    """Print full list of raw commodity names -> slugs and flag any collisions."""
    commodities = sorted(set(r["commodity"].strip() for r in records))
    slug_map: Dict[str, str] = {}
    collisions: Dict[str, List[str]] = {}

    for c in commodities:
        s = commodity_to_slug(c)
        if s in slug_map:
            collisions.setdefault(s, [slug_map[s]]).append(c)
        else:
            slug_map[s] = c

    print(f"\n=== Commodity Slug Mapping ({len(commodities)} commodities) ===")
    for c in commodities:
        print(f"  {c} -> {commodity_to_slug(c)}")

    if collisions:
        print(f"\n[ALERT] Collisions detected ({len(collisions)}):")
        for s, raw_list in collisions.items():
            print(f"  Slug '{s}' from: {raw_list}")
    else:
        print("\n[OK] Zero commodity slug collisions detected.")


if __name__ == "__main__":
    mp_file = "data/states/madhya-pradesh.json"
    if os.path.exists(mp_file):
        with open(mp_file, "r", encoding="utf-8") as f:
            d = json.load(f)
        records = d.get("records", [])
        print_commodity_slug_table(records)
        backfill_from_git("madhya-pradesh", min_records=1000)
        # Also process current working copy records
        update_state_history("Madhya Pradesh", records)

