from pathlib import Path
import sys
import pandas as pd

IN_FILENAME = "air_readings_long.csv"
BACKUP_FILENAME = "air_readings_long.pre_filter.backup.csv"

POLLUTANT_MAP = {
    "1": "so2",         # Sulfur dioxide
    "6": "co",          # Carbon monoxide
    "7": "no",          # Nitric oxide
    "8": "no2",         # Nitrogen dioxide
    "9": "o3",          # Ozone
    "10": "pm10",       # PM10
    "38": "pm25",       # PM2.5
    "39": "pm1",        # PM1 (if present)
    "12": "benzene",
    "14": "toluene",
    "20": "xylene",
}

# Keep only these pollutant names in downstream
WHITELIST = {"pm10","pm25","pm1","no","no2","o3","so2","co","benzene","toluene","xylene"}

def normalize_code_series(s: pd.Series) -> pd.Series:
    out = s.astype(str).str.strip()
    out = out.str.replace(r"\.0$", "", regex=True)
    return out

def assert_condition(cond: bool, msg: str, failures: list[str]) -> None:
    if not cond:
        failures.append(msg)

def main() -> None:
    project_root = Path(__file__).resolve().parents[1]
    pdir = project_root / "data" / "processed"
    in_path = pdir / IN_FILENAME
    backup_path = pdir / BACKUP_FILENAME

    if not in_path.exists():
        print(f"ERROR: Missing {in_path}")
        sys.exit(2)

    df = pd.read_csv(in_path, low_memory=False, parse_dates=["datetime"])

    # Backup before editing
    if not backup_path.exists():
        df.to_csv(backup_path, index=False)

    # pollutant codes
    df["pollutant_code"] = normalize_code_series(df["pollutant_code"])
    df["pollutant_name"] = df["pollutant_code"].map(POLLUTANT_MAP).fillna(df["pollutant_code"]).str.lower()

    o3_codes = sorted(df.loc[df["pollutant_name"]=="o3","pollutant_code"].dropna().unique().tolist())

    # whitelist
    before = len(df)
    kept = df[df["pollutant_name"].isin(WHITELIST)].copy()
    after = len(kept)
    removed = before - after

    # overwrite
    kept.to_csv(in_path, index=False)

    # Acceptance
    failures: list[str] = []
    unexpected_o3 = set(o3_codes) - {"9"}
    assert_condition(len(unexpected_o3) == 0, f"O3 mapped from unexpected codes: {sorted(unexpected_o3)}", failures)
    remaining = set(kept["pollutant_name"].unique()) - WHITELIST
    assert_condition(len(remaining) == 0, f"Non-whitelisted pollutant names remained: {sorted(remaining)}", failures)
    for pol in ["no2","pm10","o3"]:
        assert_condition(pol in set(kept["pollutant_name"].unique()), f"Missing key pollutant after filtering: {pol}", failures)

    print("\n===== Stage 4b: Filter+Verify summary =====")
    print(f"Rows before : {before:,}")
    print(f"Rows after  : {after:,}")
    print(f"Removed     : {removed:,}")
    print(f"Saved to    : {in_path}")
    print(f"Backup      : {backup_path}")

    if failures:
        print("\n===== ACCEPTANCE FAILED (Stage 4b) =====")
        for f in failures:
            print(f"- {f}")
        sys.exit(2)

    print("\n===== ACCEPTANCE PASSED (Stage 4b) =====")

if __name__ == "__main__":
    main()
