from pathlib import Path
import sys
import pandas as pd
import geopandas as gpd

FILES = {
    "readings_with_coords": "air_readings_long_with_coords.geoparquet",
    "stations": "stations_clean.geoparquet",
    "schools": "schools_bcn.geoparquet",
    "station_daily": "station_daily.parquet",
    "schools_station_map": "schools_station_map.parquet",
}

def assert_condition(cond: bool, msg: str, failures: list[str]) -> None:
    if not cond:
        failures.append(msg)

def build_station_daily(readings_gdf: gpd.GeoDataFrame) -> pd.DataFrame:
    df = readings_gdf.copy()
    for c in ["station_id", "pollutant_name", "datetime", "value"]:
        if c not in df.columns:
            raise RuntimeError(f"Missing column in readings: {c}")

    df["date"] = pd.to_datetime(df["datetime"]).dt.date
    gp = df.groupby(["station_id", "pollutant_name", "date"], as_index=False).agg(
        valid_hours=("value", "count"),
        value_mean=("value", "mean"),
    )
    gp["coverage_pct"] = (gp["valid_hours"] / 24.0) * 100.0
    return gp

def nearest_station_map(schools_gdf: gpd.GeoDataFrame, stations_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    schools_m = schools_gdf.to_crs("EPSG:25831")
    stations_m = stations_gdf.to_crs("EPSG:25831")
    joined = gpd.sjoin_nearest(
        schools_m[["school_id", "geometry"]],
        stations_m[["Estacio", "geometry"]],
        how="left",
        distance_col="distance_m",
    )
    out = joined.rename(columns={"Estacio": "station_id"})[["school_id", "station_id", "distance_m", "geometry"]].copy()
    out = out.to_crs("EPSG:4326")
    out = out.drop_duplicates(subset=["school_id"])
    return out

def main() -> None:
    project_root = Path(__file__).resolve().parents[1]
    pdir = project_root / "data" / "processed"

    readings_path = pdir / FILES["readings_with_coords"]
    stations_path = pdir / FILES["stations"]
    schools_path  = pdir / FILES["schools"]

    if not readings_path.exists() or not stations_path.exists() or not schools_path.exists():
        print("ERROR: Missing one or more processed inputs.")
        print("Expecting:", readings_path, stations_path, schools_path, sep="\n")
        sys.exit(2)

    readings_gdf = gpd.read_parquet(readings_path)
    stations_gdf = gpd.read_parquet(stations_path)
    schools_gdf  = gpd.read_parquet(schools_path)

    # checks
    failures: list[str] = []
    for c in ["station_id", "pollutant_code", "datetime", "value"]:
        assert_condition(c in readings_gdf.columns, f"readings missing column: {c}", failures)
    for c in ["Estacio", "geometry"]:
        assert_condition(c in stations_gdf.columns, f"stations missing column: {c}", failures)
    for c in ["school_id", "geometry"]:
        assert_condition(c in schools_gdf.columns, f"schools missing column: {c}", failures)

    # Ensuring pollutant_name
    if "pollutant_name" not in readings_gdf.columns:
        readings_gdf["pollutant_name"] = readings_gdf["pollutant_code"].astype(str).str.strip()

    # Build station daily aggregates
    station_daily = build_station_daily(readings_gdf)
    station_daily_path = pdir / FILES["station_daily"]
    try:
        station_daily.to_parquet(station_daily_path, index=False)
    except Exception as e:
        print("ERROR writing station_daily.parquet (install/upgrade pyarrow). Details:", e)
        sys.exit(2)

    # Nearest station per school
    mapping_gdf = nearest_station_map(schools_gdf, stations_gdf)
    map_path = pdir / FILES["schools_station_map"]
    try:
        mapping_gdf.to_parquet(map_path, index=False)
    except Exception as e:
        print("ERROR writing schools_station_map.parquet. Details:", e)
        sys.exit(2)

    # Quick feasibility check
    tmp = station_daily.merge(mapping_gdf[["school_id", "station_id"]], on="station_id", how="inner")
    coverage_check = (
        tmp.groupby(["school_id", "pollutant_name"], as_index=False)["date"].nunique()
        .rename(columns={"date": "days_with_data"})
    )
    print("\n===== Stage 6 (Nearest) coverage sample =====")
    print(coverage_check.head(10).to_string(index=False))

    # Report
    print("\n===== Stage 6: Exposure logic summary =====")
    print(f"Schools mapped (nearest) : {mapping_gdf['school_id'].nunique()}")
    print(f"Unique stations          : {mapping_gdf['station_id'].nunique()}")
    print(f"station_daily rows       : {len(station_daily)}")
    print(f"Saved mapping            : {map_path}")
    print(f"Saved station_daily      : {station_daily_path}")

    # Acceptance
    assert_condition(mapping_gdf["school_id"].nunique() == len(schools_gdf), "Not all schools mapped", failures)
    assert_condition(mapping_gdf.crs is None or mapping_gdf.crs.to_string() == "EPSG:4326", "Mapping CRS not EPSG:4326", failures)
    assert_condition({"station_id", "pollutant_name", "date", "value_mean", "valid_hours", "coverage_pct"}.issubset(set(station_daily.columns)),
                     "station_daily missing required columns", failures)

    if failures:
        print("\n===== ACCEPTANCE FAILED (Stage 6) =====")
        for f in failures:
            print(f"- {f}")
        sys.exit(2)

    print("\n===== ACCEPTANCE PASSED (Stage 6) =====")

if __name__ == "__main__":
    main()
