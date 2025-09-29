from pathlib import Path
import sys
import pandas as pd
import geopandas as gpd

READINGS_CSV = "air_readings_long.csv"
STATIONS_GPQ = "stations_clean.geoparquet"
OUT_GPQ = "air_readings_long_with_coords.geoparquet"

def assert_condition(cond: bool, message: str, failures: list[str]) -> None:
    if not cond:
        failures.append(message)

def main() -> None:
    project_root = Path(__file__).resolve().parents[1]
    processed_dir = project_root / "data" / "processed"
    processed_dir.mkdir(parents=True, exist_ok=True)

    readings_path = processed_dir / READINGS_CSV
    stations_path = processed_dir / STATIONS_GPQ
    out_path = processed_dir / OUT_GPQ

    if not readings_path.exists():
        print(f"ERROR: Missing {readings_path}")
        sys.exit(2)
    if not stations_path.exists():
        print(f"ERROR: Missing {stations_path}")
        sys.exit(2)

    # Load data
    readings = pd.read_csv(readings_path, low_memory=False, parse_dates=["datetime"])
    stations = gpd.read_parquet(stations_path)

    # standardization
    readings["station_id"] = readings["station_id"].astype(str).str.strip()
    stations["Estacio"] = stations["Estacio"].astype(str).str.strip()

    # Deduplicate stations at station level
    stations_unique = (
        stations[["Estacio", "Latitud", "Longitud", "geometry"]]
        .drop_duplicates(subset=["Estacio"])
        .copy()
    )
    stations_unique = stations_unique.rename(
        columns={"Estacio": "station_id", "Latitud": "lat", "Longitud": "lon"}
    )

    # Join readings
    df = readings.merge(
        stations_unique,
        on="station_id",
        how="left",
        validate="m:1"  # each station_id should map to exactly one station row
    )

    # Check missing coords
    missing = df["lat"].isna().sum() + df["lon"].isna().sum()
    if missing > 0:
        # Report a quick diagnostic sample
        missing_ids = df.loc[df["lat"].isna() | df["lon"].isna(), "station_id"].drop_duplicates().tolist()
        print("ERROR: Some readings have no station match. Example station_ids:", missing_ids[:10])
        print("Hint: ensure both sources use the same zero-padding and types (string vs int).")
        sys.exit(2)

    # Build GeoDataFrame
    gdf = gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")

    # Save
    try:
        gdf.to_parquet(out_path, index=False)
    except Exception as e:
        print("ERROR: Failed to write GeoParquet. Ensure 'pyarrow' is installed and up-to-date.")
        print("Details:", e)
        sys.exit(2)

    # Report
    print("\n===== Stage 5: Spatial join summary =====")
    print(f"Readings rows          : {len(readings)}")
    print(f"Stations (unique)      : {len(stations_unique)}")
    print(f"Joined rows            : {len(gdf)}")
    print(f"CRS                    : {gdf.crs}")
    print(f"Saved to               : {out_path}")

    # Acceptance
    failures = []
    assert_condition(gdf.crs.to_string() == "EPSG:4326", f"CRS is not EPSG:4326 (found {gdf.crs})", failures)
    assert_condition(not gdf["lat"].isna().any(), "Some rows missing lat", failures)
    assert_condition(not gdf["lon"].isna().any(), "Some rows missing lon", failures)

    if failures:
        print("\n===== ACCEPTANCE FAILED (Stage 5) =====")
        for f in failures:
            print(f"- {f}")
        sys.exit(2)

    print("\n===== ACCEPTANCE PASSED (Stage 5) =====")

if __name__ == "__main__":
    main()
