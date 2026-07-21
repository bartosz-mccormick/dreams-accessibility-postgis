# dreams-accessibility-postgis
Quickly create a PostGIS database ready for accessibility analyses. Import OSM data via `osm2pgsql` , then clean, de-duplicate, and classify into analysis-ready categories.

Developed as part of the [DUT 15-Minute City DREAMS Project](https://www.dreams15mc.eu/)

## Quick start

1. Create a `.env` file by making a copying `.env.example`, adjust the variables as desired
2. Create / start the database: `docker compose up -d db`
    - Note: if you'd like to delete your database: `docker volume rm tum-accessibility-postgis_pgdata`
3. Download `.pbf`file and import it into the database using `osm2pgsql`: `docker compose --profile import up`


- clone the repository
- replace `data/admin.gpkg` with the administrative boundaries of the city (`name` field is required). `admin.gpkg` can contain a single polygon of the boundary or of individual neighborhoods 
- save a copy of `.env.example` as `.env`
  - set `PBF_URL` to a `.pbf` extract of OSM data that will be downloaded to extract POIs (e.g., from [Geofabrik](https://download.geofabrik.de/): `PBF_URL=https://download.geofabrik.de/europe/germany/bayern/oberbayern-latest.osm.pbf`)
  - set `POP_URL` to a `.tif` of population estimates from [WorldPop](https://data.worldpop.org/GIS/Population/) (e.g., `POP_URL=https://data.worldpop.org/GIS/Population/Global_2015_2030/R2025A/2025/DEU/v1/100m/constrained/deu_pop_2025_CN_100m_R2025A_v1.tif`)
  - set `TARGET_SRID` to a projected CRS that covers the city
  - (OPTIONAL) set `ADMIN_BUFFER` to the distance (meters) `admin.gpkg` should be buffered by. The network and POIs within the buffered distance will be loaded to avoid edge effects. 
  - (OPTIONAL) set `DEFAULT_POINT_IF_SMALL_AREA`  to the area (m^2) for converting smaller polygons to points when "point_if_small" polyon policy is set 
- create  `/sql/class_a_config.csv` by copying a template from `/sql/templates/`.
  - `class_a` is the name of the most detailed POI representation in the database
  - `sql` provides the definition through:
    - in-line sql
    - (path to) dedicated sql file (leaving blank will look for corresponding file in `/sql/class_a_defs/default`)
  - `polygon_policy` specifies how to process polygon objects:
    - "area": keep polygon geometries as-is (default)
    - "point_all": convert all polygons to points
    - "point_if_small" : convert polygons smaller than threshold to points (`DEFAULT_POINT_IF_SMALL_AREA` or `min_area_m2`)    
- create `/sql/class_b_config.csv` by copying a template from `/sql/templates/` . These aggregate class_a POIs into functionally equivalent groups.
- make sure docker is running on your computer
- run all docker services (this will initialize the database and run the importer)
  - `docker compose up -d db`
  - if you want to delete the database: `docker volume rm tum-accessibility-postgis_pgdata` 
  - `docker compose --profile import up`
- run `post_import/execute_main.sh` (db must be running. If it fails, trying running it again - known bug)
- run `post_import/pop_raster_import.sh`
