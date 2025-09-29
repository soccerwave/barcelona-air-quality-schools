from pathlib import Path
import sys
import pandas as pd

COVERAGE_MIN_PCT = 75.0

FILES = {
    "station_daily": "station_daily.parquet",
    "map_A": "schools_station_map.parquet",
    "out_csv": "school_exposure_daily.csv",
}

def assert_condition(cond: bool, msg: str, failures: list[str]) -> None:
    if not cond:
        failures.append(msg)

def main() -> None:
    project_root = Path(__file__).resolve().parents[1]
    pdir = project_root / "data" / "processed"
    pdir.mkdir(parents=True, exist_ok=True)

    #  Load station_daily
    station_daily_path = pdir / FILES["station_daily"]
    if not station_daily_path.exists():
        print(f"ERROR: Missing {station_daily_path}")
        sys.exit(2)

    sd = pd.read_parquet(station_daily_path)
    required_sd = {"station_id", "pollutant_name", "date", "value_mean", "valid_hours", "coverage_pct"}
    missing_sd = required_sd - set(sd.columns)
    if missing_sd:
        print("ERROR: station_daily missing columns:", missing_sd)
        sys.exit(2)

    # Clean station_daily
    sd["station_id"] = sd["station_id"].astype(str).str.strip()
    sd["date"] = pd.to_datetime(sd["date"]).dt.date
    sd = sd.loc[(sd["value_mean"].notna()) & (sd["value_mean"] >= 0)].copy()

    #  Load school->station mapping (Nearest)
    map_path = pdir / FILES["map_A"]
    if not map_path.exists():
        print(f"ERROR: Missing {map_path} (run Stage 6 Nearest).")
        sys.exit(2)

    m = pd.read_parquet(map_path)
    if "station_id" not in m.columns or "school_id" not in m.columns:
        print("ERROR: mapping file missing required columns: school_id, station_id")
        sys.exit(2)
    m["station_id"] = m["station_id"].astype(str).str.strip()
    m["school_id"] = m["school_id"].astype(str).str.strip()

    # schools per station
    spc = (
        m.groupby("station_id")["school_id"]
         .nunique()
         .reset_index(name="schools_per_station")
         .sort_values("schools_per_station", ascending=False)
    )
    print("\n===== Mapping diagnostic (Nearest) — schools per station (top 10) =====")
    print(spc.head(10).to_string(index=False))

    # Merging
    df = sd.merge(m[["school_id", "station_id"]], on="station_id", how="inner", validate="m:m")

    #  Weighted daily exposure per school × pollutant × date
    df["_w"] = df["valid_hours"].clip(lower=0)
    df["_wv"] = df["_w"] * df["value_mean"]

    group_keys = ["school_id", "pollutant_name", "date"]

    agg = (
        df.groupby(group_keys)
        .agg(
            station_count=("station_id", "nunique"),
            valid_hours=("valid_hours", "sum"),
            _w_sum=("_w", "sum"),
            _wv_sum=("_wv", "sum"),
        )
        .reset_index()
    )
    agg["value_agg"] = agg["_wv_sum"] / agg["_w_sum"]
    agg["coverage_pct"] = (agg["valid_hours"] / (24.0 * agg["station_count"])) * 100.0
    agg["method"] = "A"

    out = agg[["school_id", "pollutant_name", "date", "value_agg", "valid_hours",
               "coverage_pct", "station_count", "method"]].copy()

    #  Quality filters
    before = len(out)
    out = out.loc[(out["coverage_pct"] >= COVERAGE_MIN_PCT) & out["value_agg"].notna() & (out["value_agg"] >= 0)].copy()
    after = len(out)

    # Save
    out["date"] = pd.to_datetime(out["date"]).dt.strftime("%Y-%m-%d")
    out_path = pdir / FILES["out_csv"]
    out.to_csv(out_path, index=False)

    # Report
    print("\n===== Stage 7: School daily exposure summary =====")
    print("Method                 : A (Nearest)")
    print(f"Groups before filter   : {before}")
    print(f"Groups after filter    : {after}")
    print(f"Schools (unique)       : {out['school_id'].nunique()}")
    print(f"Pollutants (unique)    : {out['pollutant_name'].nunique()}")
    if not out.empty:
        print(f"Date range             : {out['date'].min()} -> {out['date'].max()}")
    print(f"Saved to               : {out_path}")

    # Acceptance
    failures: list[str] = []
    for c in ["school_id", "pollutant_name", "date", "value_agg", "valid_hours", "coverage_pct"]:
        assert_condition(c in out.columns, f"missing column: {c}", failures)
    if not out.empty:
        assert_condition((out["coverage_pct"] >= COVERAGE_MIN_PCT).all(), "rows with coverage < 75% present", failures)
        assert_condition((out["value_agg"] >= 0).all(), "negative value_agg present", failures)
        assert_condition((out["station_count"] == 1).all(), "station_count must be 1 for method A", failures)

    if failures:
        print("\n===== ACCEPTANCE FAILED (Stage 7) =====")
        for f in failures:
            print(f"- {f}")
        sys.exit(2)

    print("\n===== ACCEPTANCE PASSED (Stage 7) =====")

if __name__ == "__main__":
    main()
