from pathlib import Path
import sys
import pandas as pd

IN_FILENAME = "air_readings_long.csv"
BACKUP_FILENAME = "air_readings_long.backup.csv"

POLLUTANT_MAP = {
    "1": "so2",        # Sulfur dioxide
    "6": "co",         # Carbon monoxide
    "7": "no",         # Nitric oxide
    "8": "no2",        # Nitrogen dioxide
    "9": "o3",         # Ozone
    "10": "pm10",      # PM10
    "38": "pm25",      # PM2.5
    "39": "pm1",       # PM1 (if present)
    "12": "benzene",
    "14": "toluene",
    "20": "xylene",
    # add/adjust as needed for your dataset
}

def normalize_code(s: pd.Series) -> pd.Series:
    out = s.astype(str).str.strip()
    # Remove trailing ".0"
    out = out.str.replace(r"\.0$", "", regex=True)
    return out

def assert_condition(cond: bool, message: str, failures: list[str]) -> None:
    if not cond:
        failures.append(message)

def main() -> None:
    project_root = Path(__file__).resolve().parents[1]
    processed_dir = project_root / "data" / "processed"
    in_path = processed_dir / IN_FILENAME
    backup_path = processed_dir / BACKUP_FILENAME

    if not in_path.exists():
        print(f"ERROR: Missing {in_path}")
        sys.exit(2)

    df = pd.read_csv(in_path, low_memory=False)

    # presence checks
    failures: list[str] = []
    for c in ["station_id", "pollutant_code", "datetime", "value", "validity"]:
        assert_condition(c in df.columns, f"missing required column: {c}", failures)

    # Backup before editing
    if not backup_path.exists():
        df.to_csv(backup_path, index=False)

    # Normalizing pollutant_code
    df["pollutant_code"] = normalize_code(df["pollutant_code"])

    # change names
    df["pollutant_name"] = df["pollutant_code"].map(POLLUTANT_MAP).fillna(df["pollutant_code"])

    # Save to same CSV
    out_path = in_path
    df.to_csv(out_path, index=False)

    # Acceptance checks
    assert_condition("pollutant_name" in df.columns, "pollutant_name column not created", failures)
    assert_condition(not df["pollutant_name"].isna().any(), "NaN found in pollutant_name", failures)
    # Spot-check: if common codes exist, they should map to expected names
    if (df["pollutant_code"] == "10").any():
        assert_condition((df.loc[df["pollutant_code"] == "10", "pollutant_name"] == "pm10").all(), "code 10 not mapped to pm10", failures)
    if (df["pollutant_code"] == "38").any():
        assert_condition((df.loc[df["pollutant_code"] == "38", "pollutant_name"] == "pm25").all(), "code 38 not mapped to pm25", failures)
    if (df["pollutant_code"] == "8").any():
        assert_condition((df.loc[df["pollutant_code"] == "8", "pollutant_name"] == "no2").all(), "code 8 not mapped to no2", failures)

    if failures:
        print("\n===== ACCEPTANCE FAILED (Stage 4) =====")
        for f in failures:
            print(f"- {f}")
        sys.exit(2)

    print("\n===== ACCEPTANCE PASSED (Stage 4) =====")
    print(f"Saved to: {out_path}")
    print(f"Backup  : {backup_path}")

if __name__ == "__main__":
    main()
