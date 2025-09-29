from pathlib import Path
import sys
import pandas as pd
import geopandas as gpd
import folium
from folium.plugins import MarkerCluster

POLLUTANT = "no2"
WEEK_SELECT = "latest"
SCHOOL_LAYER_NAME = "Schools (weekly mean)"
STATION_LAYER_NAME = "Stations"
OUT_HTML = "school_exposure_map.html"

FILES = {
    "exposure_csv": "school_exposure_daily.csv",
    "schools_gpq": "schools_bcn.geoparquet",
    "stations_gpq": "stations_clean.geoparquet",
}


def assert_condition(cond: bool, msg: str, failures: list[str]) -> None:
    if not cond:
        failures.append(msg)

def compute_weekly(df: pd.DataFrame, pollutant: str, week_select: str):
    df = df.copy()
    df["pollutant_name"] = df["pollutant_name"].astype(str).str.strip().str.casefold()

    available = sorted(df["pollutant_name"].dropna().unique().tolist())
    if pollutant not in set(available):
        raise ValueError(f"pollutant '{pollutant}' not found. Available: {available}")

    d = pd.to_datetime(df["date"], errors="coerce")
    if d.isna().all():
        raise ValueError("invalid or empty 'date' column")
    iso = d.dt.isocalendar()
    df["iso_year"] = iso["year"].astype(int)
    df["iso_week"] = iso["week"].astype(int)

    cols = ["school_id", "pollutant_name", "value_agg", "valid_hours", "station_count", "iso_year", "iso_week"]
    dfp = df.loc[df["pollutant_name"] == pollutant, cols].copy()

    # weight = valid_hours
    dfp["_w"] = dfp["valid_hours"].clip(lower=0)
    dfp["_wv"] = dfp["_w"] * dfp["value_agg"]

    grp = ["school_id", "pollutant_name", "iso_year", "iso_week"]
    agg = dfp.groupby(grp, as_index=False).agg(
        w_sum=("_w", "sum"),
        wv_sum=("_wv", "sum"),
        station_count=("station_count", "max"),
    )
    agg["value_week"] = agg["wv_sum"] / agg["w_sum"]

    # Pick week
    if week_select == "latest":
        tmp = agg.dropna(subset=["value_week"]).sort_values(["iso_year", "iso_week"])
        if tmp.empty:
            raise ValueError("no weekly values available for the selected pollutant")
        wy, ww = int(tmp.iloc[-1]["iso_year"]), int(tmp.iloc[-1]["iso_week"])
    else:
        try:
            wy_str, ww_str = week_select.split("-", 1)
            wy, ww = int(wy_str), int(ww_str)
        except Exception:
            raise ValueError("WEEK_SELECT must be 'latest' or 'YYYY-WW' (e.g., '2024-22').")

    sel = agg.loc[(agg["iso_year"] == wy) & (agg["iso_week"] == ww)].copy()
    if sel.empty:
        weeks = sorted({(int(y), int(w)) for y, w in zip(agg["iso_year"], agg["iso_week"])})
        raise ValueError(f"No rows for ISO {wy}-W{str(ww).zfill(2)}. Available weeks: {weeks[:20]}{' ...' if len(weeks)>20 else ''}")

    sel["year_week"] = sel["iso_year"].astype(str) + "-W" + sel["iso_week"].astype(str).str.zfill(2)
    return sel, wy, ww


def add_school_layer(m, gdf_sch_week, pollutant_label: str):
    # color scaling
    if gdf_sch_week.empty:
        raise ValueError("No rows to map after weekly aggregation.")
    v = gdf_sch_week["value_week"]
    vmin = float(v.quantile(0.05))
    vmax = float(v.quantile(0.95))
    if vmin == vmax:
        vmin = float(v.min())
        vmax = float(v.max())
    if vmin == vmax:
        vmin, vmax = 0.0, float(v.max() if v.max() > 0 else 1.0)

    #  linear mapper
    def color_fn(val: float) -> str:
        # Normalize 0..1
        x = 0.0 if vmax == vmin else (val - vmin) / (vmax - vmin)
        # Build a blue->red gradient
        # 0 -> blue, 0.5 -> orange, 1 -> red
        import math
        r = int(255 * x)
        g = int(128 + 127 * (1 - abs(2 * x - 1)))  # peak near mid
        b = int(255 * (1 - x))
        return f"#{r:02x}{g:02x}{b:02x}"

    fg = folium.FeatureGroup(name=f"{SCHOOL_LAYER_NAME}: {pollutant_label}", show=True)
    for _, r in gdf_sch_week.iterrows():
        val = float(r["value_week"])
        lat = float(r.geometry.y)
        lon = float(r.geometry.x)
        col = color_fn(val)
        radius = 4 + 8 * (0 if vmax == vmin else (val - vmin) / (vmax - vmin))
        popup = folium.Popup(
            html=f"<b>School:</b> {r['school_id']}<br>"
                 f"<b>Pollutant:</b> {pollutant_label}<br>"
                 f"<b>Weekly mean:</b> {val:.2f}<br>"
                 f"<b>Year-Week:</b> {r['year_week']}",
            max_width=300
        )
        folium.CircleMarker(
            location=[lat, lon],
            radius=float(max(3.0, min(12.0, radius))),
            color="#333333",
            weight=0.5,
            fill=True,
            fill_opacity=0.8,
            fill_color=col,
            popup=popup,
        ).add_to(fg)
    fg.add_to(m)

def add_station_layer(m, stations_gdf):
    mc = MarkerCluster(name=STATION_LAYER_NAME, show=True)
    for _, r in stations_gdf.iterrows():
        lat = float(r.geometry.y)
        lon = float(r.geometry.x)
        name = r.get("nom_cabina", "") or r.get("Nom_barri", "")
        sid = r.get("Estacio", "")
        popup = folium.Popup(
            html=f"<b>Station:</b> {sid}<br>"
                 f"<b>Name:</b> {name}",
            max_width=250
        )
        folium.Marker(location=[lat, lon], popup=popup).add_to(mc)
    mc.add_to(m)

def main() -> None:
    project_root = Path(__file__).resolve().parents[1]
    pdir = project_root / "data" / "processed"
    maps_dir = project_root / "maps"
    maps_dir.mkdir(parents=True, exist_ok=True)

    exposure_path = pdir / FILES["exposure_csv"]
    schools_path = pdir / FILES["schools_gpq"]
    stations_path = pdir / FILES["stations_gpq"]
    out_html = project_root / "maps" / OUT_HTML

    #  inputs
    if not exposure_path.exists() or not schools_path.exists() or not stations_path.exists():
        print("ERROR: Missing one or more inputs:")
        print(exposure_path, schools_path, stations_path, sep="\n")
        sys.exit(2)

    exp = pd.read_csv(exposure_path, low_memory=False)
    # Weekly aggregation
    weekly, wy, ww = compute_weekly(exp, POLLUTANT, WEEK_SELECT)

    # schools GeoDataFrame
    g_sch = gpd.read_parquet(schools_path)
    if g_sch.crs is None or g_sch.crs.to_string() != "EPSG:4326":
        g_sch = g_sch.set_crs("EPSG:4326", allow_override=True)

    # Join weekly mean to school geometries
    weekly["school_id"] = weekly["school_id"].astype(str).str.strip()
    g_sch["school_id"] = g_sch["school_id"].astype(str).str.strip()
    g_sch_week = g_sch.merge(weekly[["school_id", "value_week", "year_week"]], on="school_id", how="inner", validate="1:1")

    # Load stations
    g_sta = gpd.read_parquet(stations_path)
    if g_sta.crs is None or g_sta.crs.to_string() != "EPSG:4326":
        g_sta = g_sta.set_crs("EPSG:4326", allow_override=True)

    # Build map centered on Barcelona
    m = folium.Map(location=[41.3851, 2.1734], zoom_start=12, control_scale=True, tiles="cartodbpositron")

    # Add layers
    pollutant_label = POLLUTANT
    add_school_layer(m, g_sch_week, pollutant_label=pollutant_label)
    add_station_layer(m, g_sta)

    # Title + subtitle
    title_html = f"""
         <h3 style="position: fixed; top: 10px; left: 50px; z-index: 9999;
         background-color: rgba(255,255,255,0.85); padding: 8px 12px; border-radius: 8px;">
         School exposure — weekly mean ({pollutant_label}) — ISO {wy}-W{str(ww).zfill(2)}
         </h3>
     """
    m.get_root().html.add_child(folium.Element(title_html))

    folium.LayerControl(collapsed=False).add_to(m)
    m.save(out_html)

    print("\n===== Stage 9: Map summary =====")
    print(f"Pollutant            : {pollutant_label}")
    print(f"ISO Year-Week        : {wy}-W{str(ww).zfill(2)}")
    print(f"Schools on map       : {len(g_sch_week)}")
    print(f"Stations on map      : {len(g_sta)}")
    print(f"CRS schools/stations : {g_sch.crs} / {g_sta.crs}")
    print(f"Saved to             : {out_html}")

    # Acceptance
    failures: list[str] = []
    assert_condition(out_html.exists(), "HTML map file not written", failures)
    try:
        minx, miny, maxx, maxy = g_sch_week.total_bounds
        in_barcelona = (minx >= -1) and (maxx <= 3.5) and (miny >= 40) and (maxy <= 42.5)
        assert_condition(in_barcelona, "schools extent not in expected Barcelona bounds", failures)
    except Exception:
        failures.append("failed to validate extent bounds")
    assert_condition(g_sch.crs.to_string() == "EPSG:4326", "schools CRS not EPSG:4326", failures)
    assert_condition(g_sta.crs.to_string() == "EPSG:4326", "stations CRS not EPSG:4326", failures)

    if failures:
        print("\n===== ACCEPTANCE FAILED (Stage 9) =====")
        for f in failures:
            print(f"- {f}")
        sys.exit(2)

    print("\n===== ACCEPTANCE PASSED (Stage 9) =====")

if __name__ == "__main__":
    main()
