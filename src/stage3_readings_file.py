from pathlib import Path
import sys
import pandas as pd
import numpy as np

RAW_FILENAME = "2024_05_Maig_qualitat_aire_BCN.csv"
OUT_FILENAME = "air_readings_long.csv"

ID_BASE = ["ANY", "MES", "DIA", "ESTACIO", "CODI_CONTAMINANT"]

def expected_hour_cols(prefix: str) -> list[str]:
    return [f"{prefix}{i:02d}" for i in range(1, 25)]

def robust_read_csv(path: Path) -> pd.DataFrame:
    seps = [",", ";"]
    encs = ["utf-8", "utf-8-sig", "latin1"]
    last_err = None
    for sep in seps:
        for enc in encs:
            try:
                df = pd.read_csv(path, sep=sep, encoding=enc, low_memory=False)
                return df
            except Exception as e:
                last_err = e
                continue
    raise RuntimeError(f"Failed to read {path.name}. Last error: {last_err}")

def normalize_validity(x) -> int:
    if pd.isna(x):
        return 0
    s = str(x).strip().casefold()
    if s in {"v", "1", "true", "ok", "valid"}:
        return 1
    try:
        return 1 if int(float(s)) == 1 else 0
    except Exception:
        return 0

def main() -> None:
    project_root = Path(__file__).resolve().parents[1]
    raw_dir = project_root / "data" / "raw"
    out_dir = project_root / "data" / "processed"
    out_dir.mkdir(parents=True, exist_ok=True)

    in_path = raw_dir / RAW_FILENAME
    if not in_path.exists():
        print(f"ERROR: Missing {in_path}")
        sys.exit(2)

    df = robust_read_csv(in_path)

    # Ensuring expected columns present
    H_cols = expected_hour_cols("H")
    V_cols = expected_hour_cols("V")

    missing_base = [c for c in ID_BASE if c not in df.columns]
    missing_h = [c for c in H_cols if c not in df.columns]
    missing_v = [c for c in V_cols if c not in df.columns]
    if missing_base or missing_h or missing_v:
        print("ERROR: Missing columns.")
        if missing_base:
            print("  Missing base:", missing_base)
        if missing_h:
            print("  Missing H:", missing_h)
        if missing_v:
            print("  Missing V:", missing_v)
        sys.exit(2)

    # Keeping only necessary columns
    df2 = df[ID_BASE + H_cols + V_cols].copy()

    long_val = df2.melt(
        id_vars=ID_BASE,
        value_vars=H_cols,
        var_name="hour_h",
        value_name="value",
    )
    # hour index: H01..H24 -> 0..23
    long_val["hour"] = long_val["hour_h"].str.slice(1, 3).astype(int) - 1
    long_val = long_val.drop(columns=["hour_h"])

    long_val_flag = df2.melt(
        id_vars=ID_BASE,
        value_vars=V_cols,
        var_name="hour_v",
        value_name="validity_raw",
    )
    long_val_flag["hour"] = long_val_flag["hour_v"].str.slice(1, 3).astype(int) - 1
    long_val_flag = long_val_flag.drop(columns=["hour_v"])

    # merging value + validity on keys
    keys = ID_BASE + ["hour"]
    long = pd.merge(long_val, long_val_flag, on=keys, how="left")

    long["station_id"] = long["ESTACIO"].astype(str).str.strip()
    long["pollutant_code"] = long["CODI_CONTAMINANT"].astype(str).str.strip()

    # normalizing validity to {0,1}
    long["validity"] = long["validity_raw"].map(normalize_validity).astype("Int8")
    long = long.drop(columns=["validity_raw"])

    # value to numeric
    long["value"] = pd.to_numeric(long["value"], errors="coerce")

    # datetime with Europe/Madrid tz
    dt = pd.to_datetime(
        dict(year=long["ANY"], month=long["MES"], day=long["DIA"], hour=long["hour"]),
        errors="coerce",
        utc=False,
    )
    # Localize to Europe/Madrid
    long["datetime"] = (
        dt.dt.tz_localize("Europe/Madrid", nonexistent="shift_forward", ambiguous="infer")
    )

    # Filter invalid and negative
    mask_keep = (long["validity"] == 1) & long["value"].notna() & (long["value"] >= 0) & long["datetime"].notna()
    before = len(long)
    long = long.loc[mask_keep].copy()
    after_filter = len(long)

    # Ensuring uniqueness
    long["date"] = long["datetime"].dt.date
    dedup_subset = ["station_id", "pollutant_code", "date", "hour"]
    dup_count = long.duplicated(subset=dedup_subset).sum()
    if dup_count > 0:
        long = long.drop_duplicates(subset=dedup_subset, keep="first").copy()

    # Ordering and selecting final columns
    long = long.sort_values(["station_id", "pollutant_code", "datetime"]).copy()
    out_cols = ["station_id", "pollutant_code", "datetime", "value", "validity"]
    out_path = out_dir / OUT_FILENAME
    long.to_csv(out_path, index=False)

    # Report
    print("\n===== Stage 3: Readings long summary =====")
    print(f"Input rows (wide)      : {len(df)}")
    print(f"Rows after filters     : {after_filter}")
    print(f"Duplicates removed     : {dup_count}")
    print(f"Datetime range         : {long['datetime'].min()}  ->  {long['datetime'].max()}")
    print(f"Stations (unique)      : {long['station_id'].nunique()}")
    print(f"Pollutants (unique)    : {long['pollutant_code'].nunique()}")
    print(f"Saved to               : {out_path}")

    # Acceptance checks
    failures = []
    for c in ["station_id", "pollutant_code", "datetime", "value", "validity"]:
        if c not in long.columns:
            failures.append(f"missing column: {c}")

    # Check uniqueness per station-pollutant-date-hour
    if long.duplicated(subset=dedup_subset).any():
        failures.append("duplicates remain at (station_id, pollutant_code, date, hour)")

    # Checking invalid rows
    if (long["value"] < 0).any():
        failures.append("negative values present after filtering")
    if (long["validity"] != 1).any():
        failures.append("rows with validity != 1 remain")

    if failures:
        print("\n===== ACCEPTANCE FAILED (Stage 3) =====")
        for f in failures:
            print(f"- {f}")
        sys.exit(2)

    print("\n===== ACCEPTANCE PASSED (Stage 3) =====")

if __name__ == "__main__":
    main()
