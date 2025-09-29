# SchoolAir: Mapping Air Quality Exposure for Schools

This project demonstrates an end-to-end **data engineering and analytics pipeline** for environmental data, using Barcelona school exposure to air pollutants as a case study.

## Features

- **ETL Pipeline**: Clean and transform raw air quality readings into structured datasets.  
- **Geospatial Analysis**: Match schools with their nearest monitoring station using geospatial joins.  
- **Exposure Metrics**: Compute daily pollutant exposure levels per school with quality control.  
- **Interactive Visualization**: Explore weekly mean pollutant levels via an interactive map (`school_exposure_map.html`).  

## Repository Structure

```
├── data/
│   └── processed/         # Processed parquet/CSV datasets
├── outputs/
│   └── figures/           # Generated plots and charts
├── school_exposure_map.html   # Interactive Leaflet map (open in browser)
├── src/                   # ETL and analysis scripts
│   ├── stage_1_ingest.py
│   ├── stage_4_mapping.py
│   ├── stage_6_exposure.py
│   ├── stage_7_daily.py
│   └── stage_8_qc.py
└── README.md
```

## Tech Stack

- **Python**: pandas, geopandas, folium, pyarrow  
- **Geospatial**: spatial joins, buffering, coordinate transforms (EPSG:4326, EPSG:25831)  
- **Visualization**: Matplotlib, Folium (Leaflet.js)  
- **Data Formats**: CSV, Parquet, GeoParquet  

## Quick Start

1. Clone this repository.  
2. Install dependencies:  
   ```bash
   pip install -r requirements.txt
   ```
3. Run scripts in order (`src/` directory).  
4. Open the interactive map in your browser:  
   ```
   school_exposure_map.html
   ```

## Demo

Here’s a preview of the interactive map included in the repo:

![Preview](outputs/figures/data_quality.png)

## Use Cases

- Portfolio project for **data analytics and geospatial data engineering**.  
- Demonstrates **ETL workflows, quality checks, and geospatial joins**.  
- Recruiters and collaborators can directly explore results with the provided interactive map.

---

**Author:** HmD  
**Location:** Barcelona, Spain  
