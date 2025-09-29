from pathlib import Path
import sys
import pandas as pd

IN_CSV = "school_exposure_daily.csv"
BACKUP_CSV = "school_exposure_daily.raw.before_qc.csv"
REPORT_TXT = "school_exposure_daily_qc_report.txt"

# Logical bounds per pollutant_name
POLLUTANT_BOUNDS = {
    "pm25":   (0.0, 250.0),
    "pm10":   (0.0, 400.0),
    "pm1":    (0.0, 200.0),
    "no2":    (0.0, 400.0),
    "no":     (0.0, 500.0),
    "o3":     (0.0, 300.0),
    "so2":    (0.0, 500.0),
    "co":     (0.0, 1_000_000.0),   # دامنه باز؛ چون واحد CO متفاوت است
    "benzene":(0.0, 200.0),
    "toluene":(0.0, 1_000.0),
    "xylene": (0.0, 1_000.0),
}

REQUIRED_COLS = {
    "school_id", "pollutant_name", "date", "value_agg",
    "valid_hours", "coverage_pct", "station_count", "method"
}

def assert_condition(cond: bool, msg: str, failures: list[str]) -> None:
    if not cond:
        failures.append(msg)

def main() -> None:
    project_root = Path(__file__).resolve().parents[1]
    pdir = project_root / "data" / "processed"
    in_path = pdir / IN_CSV
    backup_path = pdir / BACKUP_CSV
    report_path = pdir / REPORT_TXT

    if not in_path.exists():
        print(f"ERROR: Missing {in_path}")
        sys.exit(2)

    df = pd.read_csv(in_path, low_memory=False)

    # --- Basic checks
    missing = REQUIRED_COLS - set(df.columns)
    if missing:
        print("ERROR: Missing required columns:", missing)
        sys.exit(2)

    # Backup
    if not backup_path.exists():
        df.to_csv(backup_path, index=False)

    # Normalize pollutant_name
    df["pollutant_name"] = df["pollutant_name"].astype(str).str.strip().str.casefold()

    # Attach bounds
    default_bounds = (0.0, 1_000_000.0)
    bounds = df["pollutant_name"].map(lambda n: POLLUTANT_BOUNDS.get(n, default_bounds))
    df["_min"] = bounds.map(lambda x: x[0])
    df["_max"] = bounds.map(lambda x: x[1])

    # QC flags
    df["_neg"] = df["value_agg"] < 0
    df["_gtmax"] = df["value_agg"] > df["_max"]
    df["_ltmin"] = df["value_agg"] < df["_min"]

    def reason(row) -> str:
        if row["_neg"]:
            return "negative"
        if row["_ltmin"]:
            return "below_min"
        if row["_gtmax"]:
            return "above_max"
        return "ok"

    df["qc_flag"] = df.apply(reason, axis=1)

    # Report counts
    total_rows = len(df)
    flagged = df.loc[df["qc_flag"] != "ok"].copy()
    kept = df.loc[df["qc_flag"] == "ok"].copy()

    # Save cleaned
    cleaned = kept.drop(columns=["_min", "_max", "_neg", "_gtmax", "_ltmin"])
    cleaned.to_csv(in_path, index=False)  # overwrite original CSV

    # Post-clean checks
    post_failures: list[str] = []
    if not cleaned.empty:
        if (cleaned["coverage_pct"] < 75).any():
            post_failures.append("rows with coverage_pct < 75 remain after QC")
        if (cleaned["value_agg"] < 0).any():
            post_failures.append("negative value_agg present after QC")
        if (cleaned["station_count"] != 1).any():
            post_failures.append("station_count != 1 in some rows (should all be 1 for nearest)")

    # Write report
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("===== Stage 8 QC Report =====\n")
        f.write(f"Input rows           : {total_rows}\n")
        f.write(f"Flagged (to drop)    : {len(flagged)}\n")
        f.write(f"Kept (clean)         : {len(cleaned)}\n\n")

        if post_failures:
            f.write("---- Post-clean checks ----\n")
            for msg in post_failures:
                f.write(f"FAIL: {msg}\n")
        else:
            f.write("PASS: Clean file satisfies constraints (coverage>=75, non-negative, station_count=1).\n")

        if not cleaned.empty:
            v = cleaned["value_agg"].describe(percentiles=[0.5, 0.9, 0.95]).to_string()
            f.write("\n---- value_agg stats (cleaned) ----\n")
            f.write(v + "\n")

    # Console summary
    print("\n===== Stage 8: QC summary =====")
    print(f"Input rows           : {total_rows}")
    print(f"Flagged (to drop)    : {len(flagged)}")
    print(f"Kept (clean)         : {len(cleaned)}")
    print(f"Report saved to      : {report_path}")
    print(f"Clean CSV overwritten: {in_path}")

    if post_failures:
        print("\n===== ACCEPTANCE FAILED (Stage 8) =====")
        for m in post_failures:
            print(f"- {m}")
        sys.exit(2)

    print("\n===== ACCEPTANCE PASSED (Stage 8) =====")

if __name__ == "__main__":
    main()
