DROP MATERIALIZED VIEW IF EXISTS staging.pop_raster CASCADE;

CREATE MATERIALIZED VIEW staging.pop_raster AS
WITH pa AS (
  SELECT ST_Transform(geom, 4326) AS geom_4326
  FROM admin.planning_area
)
SELECT
  row_number() OVER ()::bigint AS rid,
  ST_Clip(r.rast, 1, pa.geom_4326, true) AS rast
FROM public.raw_pop_raster r
CROSS JOIN pa
WHERE ST_Intersects(r.rast, pa.geom_4326);

CREATE UNIQUE INDEX pop_raster_rid_idx ON staging.pop_raster (rid);
CREATE INDEX pop_raster_rast_gix ON staging.pop_raster USING GIST (ST_ConvexHull(rast));


-- Drop + recreate the (materialized) view that aggregates pop per H3 cell
DROP VIEW IF EXISTS staging.pop_by_h3 CASCADE;

CREATE VIEW staging.pop_by_h3 AS
WITH px AS (
  -- explode raster into pixel centroids + values
  SELECT
    (pc).geom AS geom,
    (pc).val  AS pop
  FROM (
    SELECT ST_PixelAsCentroids(rast, 1) AS pc
    FROM staging.pop_raster
  ) s
  WHERE (pc).val IS NOT NULL
),
agg AS (
  SELECT
    h3_lat_lng_to_cell(
        point(ST_X(geom), ST_Y(geom)),  -- << matches your working pattern
        10
    )::text AS h3_cell,
    SUM(pop)::double precision AS pop
  FROM px
  GROUP BY 1
)
SELECT h3_cell, pop
FROM agg;

-- 1) Add a column on the grid if it doesn't exist
ALTER TABLE admin.grid
ADD COLUMN IF NOT EXISTS pop double precision;
UPDATE admin.grid SET pop = 0;


-- 2) Update from the aggregated pop view (left join behaviour via COALESCE)
UPDATE admin.grid g
SET pop = p.pop
FROM staging.pop_by_h3 p
WHERE p.h3_cell = g.h3_cell;