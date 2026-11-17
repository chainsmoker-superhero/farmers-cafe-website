import pytest
from scripts.build_history import (
    make_market_key,
    commodity_to_slug,
    make_series_key,
    collapse_variety_grades,
    detect_market_key_collisions,
    update_state_history,
    load_json,
)


def test_make_market_key():
    """Verify market key generation matches ApmcDataMapper.resolveMandi."""
    assert make_market_key("Sehore", "Sehore APMC") == "mandi_sehore_sehore"
    assert make_market_key("Agar Malwa", "Agar(F&V) APMC") == "mandi_agar_malwa_agar_f_v"
    assert make_market_key("Dhar", "Dhamnod APMC") == "mandi_dhar_dhamnod"
    assert make_market_key("Khargone", "Bhikangaon APMC") == "mandi_khargone_bhikangaon"
    assert make_market_key("Chhindwara", "Chaurai APMC") == "mandi_chhindwara_chaurai"
    assert make_market_key("Bhopal", "Bhopal APMC") == "mandi_bhopal_bhopal"


def test_commodity_to_slug():
    """Verify raw commodity name produces expected slug without alias map."""
    assert commodity_to_slug("Kabuli Chana(Chickpeas-White)") == "kabuli-chana-chickpeas-white"
    assert commodity_to_slug("Bengal Gram(Gram)(Whole)") == "bengal-gram-gram-whole"
    assert commodity_to_slug("Red Gram (Arhar/Tur)(Whole)") == "red-gram-arhar-tur-whole"
    assert commodity_to_slug("Wheat") == "wheat"
    assert commodity_to_slug("Soyabean") == "soyabean"


def test_collapse_variety_grades_prefers_faq():
    """Verify FAQ rows take precedence over Non-FAQ rows for the same variety and day."""
    rows = [
        {
            "date": "2026-11-13",
            "grade": "Non-FAQ",
            "min": 2200.0,
            "max": 2500.0,
            "modal": 2400.0,
        },
        {
            "date": "2026-11-13",
            "grade": "FAQ",
            "min": 2600.0,
            "max": 2900.0,
            "modal": 2800.0,
        },
    ]
    collapsed = collapse_variety_grades(rows)
    assert collapsed is not None
    assert collapsed["d"] == "2026-11-13"
    assert collapsed["modal"] == 2800.0
    assert collapsed["min"] == 2600.0
    assert collapsed["max"] == 2900.0


def test_collapse_variety_grades_median_and_extrema():
    """Verify median modal and extrema (min of mins, max of maxes) across multiple FAQ rows."""
    rows = [
        {"date": "2026-11-13", "grade": "FAQ", "min": 2400.0, "max": 2600.0, "modal": 2500.0},
        {"date": "2026-11-13", "grade": "FAQ", "min": 2350.0, "max": 2800.0, "modal": 2700.0},
        {"date": "2026-11-13", "grade": "FAQ", "min": 2500.0, "max": 2650.0, "modal": 2600.0},
    ]
    collapsed = collapse_variety_grades(rows)
    assert collapsed is not None
    # Median of [2500, 2600, 2700] is 2600.0
    assert collapsed["modal"] == 2600.0
    assert collapsed["min"] == 2350.0
    assert collapsed["max"] == 2800.0


def test_variety_kept_separate(tmp_path, monkeypatch):
    """Verify varieties in the same market are kept in distinct series and modals are NOT averaged."""
    monkeypatch.setattr("scripts.build_history.HISTORY_DIR", str(tmp_path))

    records = [
        {
            "state": "Madhya Pradesh",
            "district": "Sehore",
            "market": "Sehore APMC",
            "commodity": "Wheat",
            "variety": "Lokwan",
            "grade": "FAQ",
            "date": "2026-11-13",
            "min": 2600.0,
            "max": 2800.0,
            "modal": 2750.0,
        },
        {
            "state": "Madhya Pradesh",
            "district": "Sehore",
            "market": "Sehore APMC",
            "commodity": "Wheat",
            "variety": "Mill Quality",
            "grade": "FAQ",
            "date": "2026-11-13",
            "min": 2400.0,
            "max": 2550.0,
            "modal": 2500.0,
        },
    ]

    update_state_history("Madhya Pradesh", records)

    hist_file = tmp_path / "madhya-pradesh" / "wheat.json"
    data = load_json(str(hist_file))

    assert data is not None
    series = data["series"]
    assert "mandi_sehore_sehore__lokwan" in series
    assert "mandi_sehore_sehore__mill_quality" in series

    lokwan = series["mandi_sehore_sehore__lokwan"]
    mill = series["mandi_sehore_sehore__mill_quality"]

    assert lokwan["variety"] == "Lokwan"
    assert lokwan["points"][0]["modal"] == 2750.0

    assert mill["variety"] == "Mill Quality"
    assert mill["points"][0]["modal"] == 2500.0


def test_idempotent_rerun(tmp_path, monkeypatch):
    """Verify re-running with same date updates entry in-place rather than creating duplicate points."""
    monkeypatch.setattr("scripts.build_history.HISTORY_DIR", str(tmp_path))

    record = [{
        "state": "Madhya Pradesh",
        "district": "Dhar",
        "market": "Dhamnod APMC",
        "commodity": "Kabuli Chana(Chickpeas-White)",
        "variety": "Dollar Gram",
        "grade": "FAQ",
        "date": "2026-11-13",
        "min": 9500.0,
        "max": 10500.0,
        "modal": 10200.0,
    }]

    # First run
    update_state_history("Madhya Pradesh", record)
    hist_file = tmp_path / "madhya-pradesh" / "kabuli-chana-chickpeas-white.json"
    data1 = load_json(str(hist_file))
    points1 = data1["series"]["mandi_dhar_dhamnod__dollar_gram"]["points"]
    assert len(points1) == 1
    assert points1[0]["modal"] == 10200.0

    # Second run with updated price on same date
    record[0]["modal"] = 10400.0
    update_state_history("Madhya Pradesh", record)
    data2 = load_json(str(hist_file))
    points2 = data2["series"]["mandi_dhar_dhamnod__dollar_gram"]["points"]
    assert len(points2) == 1
    assert points2[0]["modal"] == 10400.0


def test_14_day_trim(tmp_path, monkeypatch):
    """Verify that only the most recent 14 points are retained in chronological order."""
    monkeypatch.setattr("scripts.build_history.HISTORY_DIR", str(tmp_path))

    records = []
    for day in range(1, 18):  # 17 days
        records.append({
            "state": "Madhya Pradesh",
            "district": "Khargone",
            "market": "Bhikangaon APMC",
            "commodity": "Bengal Gram(Gram)(Whole)",
            "variety": "Gram",
            "grade": "FAQ",
            "date": f"2026-11-{day:02d}",
            "min": 7000.0 + day,
            "max": 7500.0 + day,
            "modal": 7200.0 + day,
        })

    update_state_history("Madhya Pradesh", records)

    hist_file = tmp_path / "madhya-pradesh" / "bengal-gram-gram-whole.json"
    data = load_json(str(hist_file))
    points = data["series"]["mandi_khargone_bhikangaon__gram"]["points"]

    assert len(points) == 14
    # Earliest date should be day 4 (17 - 14 + 1 = 4)
    assert points[0]["d"] == "2026-11-04"
    assert points[-1]["d"] == "2026-11-17"


def test_detect_market_key_collisions():
    """Verify detection of market key collisions and error when exceeding threshold."""
    # 0 collisions
    records_clean = [
        {"district": "Sehore", "market": "Sehore APMC"},
        {"district": "Bhopal", "market": "Bhopal APMC"},
    ]
    assert len(detect_market_key_collisions(records_clean)) == 0

    # 4 distinct pairs producing colliding keys
    records_colliding = [
        {"district": "Dist A", "market": "Market-1 APMC"},
        {"district": "Dist-A", "market": "Market 1 APMC"},  # collision 1
        {"district": "Dist B", "market": "Market-2 APMC"},
        {"district": "Dist-B", "market": "Market 2 APMC"},  # collision 2
        {"district": "Dist C", "market": "Market-3 APMC"},
        {"district": "Dist-C", "market": "Market 3 APMC"},  # collision 3
        {"district": "Dist D", "market": "Market-4 APMC"},
        {"district": "Dist-D", "market": "Market 4 APMC"},  # collision 4
    ]
    with pytest.raises(ValueError, match="Too many market_key collisions"):
        detect_market_key_collisions(records_colliding)


def test_backfill_skips_thin_commits(monkeypatch):
    """Verify backfill skips commits with record count < 1000 and uses commits >= 1000."""
    from scripts import build_history

    fake_commits = ["commit_thin", "commit_full"]

    def mock_subprocess_run(cmd, *args, **kwargs):
        class MockResult:
            def __init__(self, stdout):
                self.stdout = stdout

        if cmd[1] == "log":
            return MockResult("commit_thin\ncommit_full\n")
        elif cmd[1] == "show":
            commit_ref = cmd[2].split(":")[0]
            if commit_ref == "commit_thin":
                return MockResult('{"state": "Madhya Pradesh", "records": [{"commodity": "Wheat", "district": "Sehore", "market": "Sehore APMC", "variety": "Lokwan", "grade": "FAQ", "date": "2026-11-15", "min": 2500, "max": 2700, "modal": 2600}] * 245}')
            else:
                records = [{
                    "commodity": "Wheat",
                    "district": "Sehore",
                    "market": "Sehore APMC",
                    "variety": "Lokwan",
                    "grade": "FAQ",
                    "date": "2026-11-13",
                    "min": 2500,
                    "max": 2700,
                    "modal": 2600
                }] * 1050
                import json
                return MockResult(json.dumps({"state": "Madhya Pradesh", "records": records}))
        raise ValueError(f"Unexpected cmd {cmd}")

    monkeypatch.setattr(build_history.subprocess, "run", mock_subprocess_run)
    monkeypatch.setattr(build_history, "update_state_history", lambda state, recs, updated_at=None: None)

    used = build_history.backfill_from_git("madhya-pradesh", min_records=1000)
    assert "commit_thin" not in used
    assert "commit_full" in used


def test_series_metadata_fields(tmp_path, monkeypatch):
    """Verify each series includes last_date and n_points fields."""
    monkeypatch.setattr("scripts.build_history.HISTORY_DIR", str(tmp_path))

    record = [{
        "state": "Madhya Pradesh",
        "district": "Dhar",
        "market": "Dhamnod APMC",
        "commodity": "Kabuli Chana(Chickpeas-White)",
        "variety": "Dollar Gram",
        "grade": "FAQ",
        "date": "2026-11-15",
        "min": 10500.0,
        "max": 10650.0,
        "modal": 10600.0,
    }]

    update_state_history("Madhya Pradesh", record)
    hist_file = tmp_path / "madhya-pradesh" / "kabuli-chana-chickpeas-white.json"
    data = load_json(str(hist_file))
    series = data["series"]["mandi_dhar_dhamnod__dollar_gram"]

    assert series["last_date"] == "2026-11-15"
    assert series["n_points"] == 1


def test_file_size_limit_check(tmp_path):
    """Verify write_json_atomic fails if file exceeds 500 KB limit."""
    from scripts.build_history import write_json_atomic

    target = tmp_path / "oversized.json"
    # Create an object that serializes to > 500 KB
    huge_data = {"data": "x" * (510 * 1024)}

    with pytest.raises(ValueError, match="exceeds size limit"):
        write_json_atomic(str(target), huge_data, max_bytes=500 * 1024)


