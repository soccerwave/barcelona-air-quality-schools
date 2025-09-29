from pathlib import Path
import sys
import pandas as pd
import geopandas as gpd

RAW_FILENAME = "Directori_de_centres_docents._Fins_curs_2019-2020_20250925.csv"

EXPECTED_COLS = {
    "name": "Denominació completa",
    "address": "Adreça",
    "municipality": "Nom municipi",
    "utm_x": "Coordenades UTM X",
    "utm_y": "Coordenades UTM Y",
    "geo_x": "Coordenades GEO X",
    "geo_y": "Coordenades GEO Y",
    "code": "Codi centre",
}

SEPS = [",", ";"]
ENCS = ["utf-8", "utf-8-sig", "latin1"]

def robust_read_csv(path: Path) -> pd.DataFrame:
    last_err = None
    for sep in SEPS:
        for enc in ENCS:
            try:
                return pd.read_csv(path, sep=sep, encoding=enc, low_memory=False)
            except Exception as e:
                last_err = e
                continue
    raise RuntimeError(f"Failed to read {path.name}. Last error: {last_err}")

def coerce_num(s: pd.Series) -> pd.Series:
    # Converting strings with comma decimal to float
    return pd.to_numeric(s.astype(str).str.replace(",", ".", regex=False).str.strip(), errors="coerce")

def build_gdf_from_utm(df: pd.DataFrame, x_col: str, y_col: str) -> gpd.GeoDataFrame:
    x = coerce_num(df[x_col])
    y = coerce_num(df[y_col])
    valid = x.between(200000, 800000, inclusive="both") & y.between(4400000, 4800000, inclusive="both")
    df2 = df.loc[valid].copy()
    gdf = gpd.GeoDataFrame(
        df2,
        geometry=gpd.points_from_xy(x.loc[valid], y.loc[valid]),
        crs="EPSG:25831",
    ).to_crs("EPSG:4326")
    return gdf

def build_gdf_from_geo(df: pd.DataFrame, lon_col: str, lat_col: str) -> gpd.GeoDataFrame:
    lon = coerce_num(df[lon_col])
    lat = coerce_num(df[lat_col])

    # Detecting swapped columns
    cond_lon = lon.between(-10, 10, inclusive="both")
    cond_lat = lat.between(39, 45, inclusive="both")
    ok = cond_lon & cond_lat

    if ok.mean() < 0.5:
        # swapping interpretation
        lon2 = coerce_num(df[lat_col])
        lat2 = coerce_num(df[lon_col])
        cond_lon2 = lon2.between(-10, 10, inclusive="both")
        cond_lat2 = lat2.between(39, 45, inclusive="both")
        ok2 = cond_lon2 & cond_lat2
        df2 = df.loc[ok2].copy()
        gdf = gpd.GeoDataFrame(
            df2,
            geometry=gpd.points_from_xy(lon2.loc[ok2], lat2.loc[ok2]),
            crs="EPSG:4326",
        )
        source = "geo_swapped"
    else:
        df2 = df.loc[ok].copy()
        gdf = gpd.GeoDataFrame(
            df2,
            geometry=gpd.points_from_xy(lon.loc[ok], lat.loc[ok]),
            crs="EPSG:4326",
        )
        source = "geo_direct"

    gdf["_geo_source"] = source
    return gdf

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

    # column_checks
    missing = [c for c in EXPECTED_COLS.values() if c not in df.columns]
    if missing:
        print("ERROR: Missing required columns:", missing)
        sys.exit(2)

    # Trying UTM
    use_mode = None
    gdf_candidate = None
    if EXPECTED_COLS["utm_x"] in df.columns and EXPECTED_COLS["utm_y"] in df.columns:
        gdf_utm = build_gdf_from_utm(df, EXPECTED_COLS["utm_x"], EXPECTED_COLS["utm_y"])
        if len(gdf_utm) > 10000:  # enough plausible UTM points
            gdf_candidate = gdf_utm
            use_mode = "utm_25831"
        else:
            gdf_candidate = None

    if gdf_candidate is None:
        gdf_geo = build_gdf_from_geo(df, EXPECTED_COLS["geo_x"], EXPECTED_COLS["geo_y"])
        gdf_candidate = gdf_geo
        if "_geo_source" in gdf_candidate.columns:
            use_mode = gdf_candidate["_geo_source"].iloc[0]
        else:
            use_mode = "geo_direct"

    # Keeping essential columns
    gdf = gdf_candidate.copy()
    gdf["school_id"] = gdf[EXPECTED_COLS["code"]].astype(str).str.strip()
    gdf["school_name"] = gdf[EXPECTED_COLS["name"]].astype(str).str.strip()
    gdf["address"] = gdf[EXPECTED_COLS["address"]].astype(str).str.strip()
    gdf["municipality"] = gdf[EXPECTED_COLS["municipality"]].astype(str).str.strip()

    # Filtering municipality
    mask_bcn = gdf["municipality"].str.strip().str.casefold() == "barcelona"
    gdf_bcn = gdf.loc[mask_bcn].copy()

    # Drop empty geometries and duplicates
    gdf_bcn = gdf_bcn[~gdf_bcn.geometry.is_empty & gdf_bcn.geometry.notna()].copy()
    gdf_bcn = gdf_bcn.drop_duplicates(subset=["school_id"]).copy()

    # Ensure CRS = EPSG:4326
    try:
        if gdf_bcn.crs is None:
            # If missing, assume EPSG:4326 (should not happen here)
            gdf_bcn = gdf_bcn.set_crs("EPSG:4326")
        elif gdf_bcn.crs.to_string() != "EPSG:4326":
            gdf_bcn = gdf_bcn.to_crs("EPSG:4326")
    except Exception as e:
        print("ERROR: CRS issue. Details:", e)
        sys.exit(2)

    out_path = out_dir / "schools_bcn.geoparquet"
    try:
        gdf_bcn[["school_id", "school_name", "address", "municipality", "geometry"]].to_parquet(out_path, index=False)
    except Exception as e:
        print("ERROR: Failed to write GeoParquet. Ensure 'pyarrow' is installed.")
        print("Details:", e)
        sys.exit(2)

    # Report
    print("\n===== Stage 2: Schools summary =====")
    print(f"Input rows (all Catalunya) : {len(df)}")
    print(f"Mode used                  : {use_mode}")
    print(f"Barcelona rows (pre-clean) : {mask_bcn.sum()}")
    print(f"Output rows (Barcelona)    : {len(gdf_bcn)}")
    print(f"CRS                        : {gdf_bcn.crs}")
    print(f"Saved to                   : {out_path}")

    # Acceptance checks
    failures = []
    if gdf_bcn.crs.to_string() != "EPSG:4326":
        failures.append(f"CRS is not EPSG:4326 (found {gdf_bcn.crs})")
    if len(gdf_bcn) <= 300:
        failures.append(f"Barcelona schools <= 300 (found {len(gdf_bcn)})")
    if gdf_bcn.geometry.is_empty.any() or gdf_bcn.geometry.isna().any():
        failures.append("Empty geometries present")

    if failures:
        print("\n===== ACCEPTANCE FAILED (Stage 2) =====")
        for f in failures:
            print(f"- {f}")
        sys.exit(2)

    print("\n===== ACCEPTANCE PASSED (Stage 2) =====")

if __name__ == "__main__":
    main()
