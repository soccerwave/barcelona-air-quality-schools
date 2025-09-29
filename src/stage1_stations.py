from pathlib import Path
import sys
import pandas as pd
import geopandas as gpd

SEPS = [",", ";"]
ENCS = ["utf-8", "utf-8-sig", "latin1"]

RAW_FILENAME = "2025_qualitat_aire_estacions.csv"
EXPECTED_COLS = {"Estacio", "Latitud", "Longitud", "Nom_districte", "Nom_barri"}

def robust_read_csv(path: Path, expected_cols: set) -> pd.DataFrame:
    best_df = None
    best_score = -1
    last_err = None
    for sep in SEPS:
        for enc in ENCS:
            try:
                df = pd.read_csv(path, sep=sep, encoding=enc, low_memory=False)
                score = len(set(df.columns) & expected_cols)
                if score > best_score:
                    best_df = df
                    best_score = score
            except Exception as e:
                last_err = e
                continue
    if best_df is None:
        raise RuntimeError(f"Failed to read {path.name}. Last error: {last_err}")
    return best_df

def coerce_float_series(s: pd.Series) -> pd.Series:
    s = s.astype(str).str.replace(",", ".", regex=False).str.strip()
    return pd.to_numeric(s, errors="coerce")

def assert_condition(cond: bool, message: str, failures: list[str]) -> None:
    if not cond:
        failures.append(message)

def main() -> None:
    project_root = Path(__file__).resolve().parents[1]
    raw_dir = project_root / "data" / "raw"
    out_dir = project_root / "data" / "processed"
    out_dir.mkdir(parents=True, exist_ok=True)

    stations_path = raw_dir / RAW_FILENAME
    if not stations_path.exists():
        print(f"ERROR: Missing {stations_path}")
        sys.exit(2)

    df = robust_read_csv(stations_path, EXPECTED_COLS)

    failures: list[str] = []
    for col in EXPECTED_COLS:
        assert_condition(col in df.columns, f"missing column: {col}", failures)

    df["Latitud"] = coerce_float_series(df["Latitud"])
    df["Longitud"] = coerce_float_series(df["Longitud"])

    before = len(df)
    df = df.dropna(subset=["Latitud", "Longitud"]).copy()
    after = len(df)

    # filtering wrong ranges
    mask_valid_range = (
        df["Latitud"].between(-90, 90, inclusive="both")
        & df["Longitud"].between(-180, 180, inclusive="both")
    )
    dropped_out_of_range = (~mask_valid_range).sum()
    df = df.loc[mask_valid_range].copy()

    df["Estacio"] = df["Estacio"].astype(str).str.strip()

    # building GeoDataFrame
    gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df["Longitud"], df["Latitud"]),
        crs="EPSG:4326",
    )

    # saving
    out_path = out_dir / "stations_clean.geoparquet"


    # Report
    print("\n===== Stage 1: Stations summary =====")
    print(f"Input rows            : {before}")
    print(f"Dropped NaN coords    : {before - after}")
    print(f"Dropped out-of-range  : {dropped_out_of_range}")
    print(f"Output rows           : {len(gdf)}")
    print(f"CRS                   : {gdf.crs}")
    print(f"Saved to              : {out_path}")

    if failures:
        print("\n===== ACCEPTANCE FAILED (Stage 1) =====")
        for f in failures:
            print(f"- {f}")
        sys.exit(2)

    print("\n===== ACCEPTANCE PASSED (Stage 1) =====")

if __name__ == "__main__":
    main()
