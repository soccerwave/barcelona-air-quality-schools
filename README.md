# Barcelona Schools Air Quality Exposure Pipeline

This project builds a data pipeline to link **Barcelona schools** with **nearest air quality monitoring stations** and compute daily/weekly exposure to pollutants.  
The output includes clean datasets and an interactive HTML map.

---

## Project Structure

```
project_root/
  data/
    raw/                  # input CSV files (stations, readings, schools)
    processed/            # cleaned outputs (CSV/Parquet/Reports)
  maps/
    school_exposure_map.html   # final interactive Folium map
    school_exposure_map_preview.png
    ample_school_popup.png
    sample_station_popup.png
    pipeline_acceptance.png
  src/
    stage0_check.py
    stage1_stations.py
    stage2_schools.py
    stage3_readings_long.py
    stage4_pollutant_map.py
    stage4b_filter_and_verify.py
    stage5_spatial_join.py
    stage6_exposure_logic.py
    stage7_daily_agg.py
    stage8_qc.py
    stage9_map.py
    stage_check_postfilter.py
  README.md
  requirements.txt
```

- Time zone: **Europe/Madrid**
- Final CRS: **EPSG:4326 (WGS84)**; metric operations in **EPSG:25831**

---

## Quickstart (Windows, PowerShell)

```powershell
python -m venv .venv
.venv\Scripts\activate

pip install -r requirements.txt

# Run the pipeline step by step
python src\stage0_check.py
python src\stage1_stations.py
python src\stage2_schools.py
python src\stage3_readings_long.py
python src\stage4_pollutant_map.py
python src\stage4b_filter_and_verify.py
python src\stage5_spatial_join.py
python src\stage6_exposure_logic.py
python src\stage7_daily_agg.py
python src\stage8_qc.py
python src\stage9_map.py

# Final validation
python src\stage_check_postfilter.py
```

If `pyogrio` fails to install on Windows, install GDAL first or use **fiona** instead.

---

## Key Outputs

- `data/processed/air_readings_long.csv` — hourly readings in long format with `station_id, pollutant_code, pollutant_name, datetime, value, validity`.  
- `data/processed/stations_clean.geoparquet` — monitoring stations GeoDataFrame (EPSG:4326).  
- `data/processed/schools_bcn.geoparquet` — Barcelona schools GeoDataFrame (EPSG:4326).  
- `data/processed/station_daily.parquet` — daily mean per station × pollutant.  
- `data/processed/schools_station_map.parquet` — school → nearest station mapping.  
- `data/processed/school_exposure_daily.csv` — daily exposure per school × pollutant (coverage ≥ 75%).  
- `maps/school_exposure_map.html` — Folium map (weekly averages, default pollutant NO₂, last available week).  
- Reports:  
  - `data/processed/school_exposure_daily_qc_report.txt` — Stage 8 QC.  
  - `data/processed/postfilter_checks_report.txt` — Post-filter validation.

---

## Screenshots

Screenshots stored in `maps/` can be used to illustrate the README:

![Map preview](maps/school_exposure_map_preview.png)  
*Overview of schools and stations (weekly averages).*

![School popup](maps/ample_school_popup.png)  
*Example school popup with school_id, pollutant averages, ISO week.*

![Station popup](maps/sample_station_popup.png)  
*Example station popup with Estacio and pollutants.*

![Pipeline acceptance](maps/pipeline_acceptance.png)  
*Console output showing successful acceptance checks.*

---

## Pollutant Mapping & Filters

- **Whitelist pollutants:** `pm10, pm25, pm1, no, no2, o3, so2, co, benzene, toluene, xylene`.  
- **O3 must only come from code '9'** (validated against the official network codebook).  
- Other unknown/auxiliary codes (e.g., `22, 901, 996–999`) are removed.  

---

## Validation Results (example run)

- Weeks available: **ISO 2024-W18 .. W22 (May 2024)**  
- Schools mapped: **1000**; Stations: **8**  
- Distance school → station: median ≈ **1.3 km**, p95 ≈ **3.54 km**, max ≈ **6.25 km**  
- Post-filter report: **OVERALL: PASS**

---

## Limitations

- Nearest-station mapping is coarse; spatial smoothing or buffer-based methods could improve accuracy.  
- Units for benzene/toluene/CO should be cross-checked with the official catalog.  
- All metric calculations must use CRS **EPSG:25831** before converting back to EPSG:4326.

---

## Dependencies

Pinned versions in `requirements.txt` ensure stable installation on Windows with GeoPandas and PyArrow.
