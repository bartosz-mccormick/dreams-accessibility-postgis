
-- =========================================================
-- 0) Extensions & Schemas
-- =========================================================
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE SCHEMA IF NOT EXISTS staging;

-- =========================================================
-- 1) Unified features as MATERIALIZED VIEW (fast & indexable)
-- =========================================================
DROP MATERIALIZED VIEW IF EXISTS staging.unified_features_mat CASCADE;
CREATE MATERIALIZED VIEW staging.unified_features_mat AS
SELECT
  *,
  'point' AS geom_type
FROM public.raw_points_of_interest
UNION ALL
SELECT
  *,
  'area' AS geom_type
FROM public.raw_areas_of_interest;

-- Helpful indexes
CREATE INDEX IF NOT EXISTS unified_mat_tags ON staging.unified_features_mat (amenity, shop, leisure);
CREATE INDEX IF NOT EXISTS unified_mat_geom_gix ON staging.unified_features_mat USING GIST (geom);
CREATE INDEX IF NOT EXISTS unified_mat_osm_geomtype ON staging.unified_features_mat (osm_id, geom_type);

-- =========================================================
-- 2) Config landing & load CSV
--    (/sql/class_a_config.csv with columns: class_a,sql,polygon_policy,min_area_m2 - last two can be empty)
-- =========================================================
DROP TABLE IF EXISTS staging.class_a_config CASCADE;

CREATE TABLE staging.class_a_config (
  class_a      text PRIMARY KEY,
  sql          text,
  polygon_policy  text DEFAULT 'area',          -- 'area' | 'point_all' | 'point_if_small'
  min_area_m2  double precision DEFAULT 1000 -- m2 threshold for point_if_small
);

\copy staging.class_a_config (class_a, sql,polygon_policy,min_area_m2) FROM '/sql/class_a_config.csv' WITH (FORMAT csv, HEADER true, NULL '');

-- trim leading and trailing whitespace
UPDATE staging.class_a_config
SET sql = btrim(sql);

-- Change all keys with : to _ (healthcare:speciality to healthcare_speciality)
UPDATE staging.class_a_config
SET sql = replace(btrim(sql), ':', '_');

-- Validation of polygon_policy values
ALTER TABLE staging.class_a_config
  ADD CONSTRAINT polygon_policy_chk
  CHECK (polygon_policy IS NULL OR polygon_policy IN ('area','point_all','point_if_small'));
  
-- =========================================================
-- 3) Classification
--    - Read each snippet file for a class
--    - Support WHERE-only snippets (wrapped) and FULL-SQL (SELECT/WITH)
--    - Write to staging.rule_hits (osm_id, class_a, name, geom)
-- =========================================================
DROP TABLE IF EXISTS staging.rule_hits;
CREATE TABLE staging.rule_hits (
  osm_id   bigint,
  class_a  text,
  name text,
  geom     geometry
);

DO $$
DECLARE
  rec RECORD;
  resolved_path text;
  snippet       text;
  first_kw      text;
  sql_text      text;
BEGIN
  FOR rec IN
    SELECT *
    FROM staging.class_a_config
    ORDER BY class_a
  LOOP
    sql_text := NULL;
    IF lower(ltrim(rec.sql)) LIKE 'where%' THEN
        -- handle WHERE case here
        sql_text := format($fmt$
            INSERT INTO staging.rule_hits (osm_id, class_a, name, geom)
            SELECT osm_id, %L AS class_a, name, geom
            FROM staging.unified_features_mat 
            %s;
        $fmt$, rec.class_a, rec.sql);  
    ELSIF rec.sql ~ '^/sql/class_a_defs/.*\.sql$' THEN
        resolved_path := rec.sql;
    ELSIF rec.sql IS NULL THEN
        -- Default to /sql/class_a_defs/default/<class_a>.sql if sql is empty
        resolved_path := '/sql/class_a_defs/default/' || rec.class_a || '.sql';
    ELSE
        RAISE EXCEPTION 'Invalid sql value for class %: % (must start with WHERE, be /sql/class_a_defs/*.sql, or be empty)', rec.class_a, rec.sql;
    END IF;
    -- handle full SELECT/WITH/etc. case here
    IF sql_text IS NULL THEN
        -- NOTE: pg_read_file requires superuser (true for default POSTGRES_USER)
        snippet := pg_read_file(resolved_path);

        -- Detect if snippet starts with SELECT/WITH
        first_kw := lower(regexp_replace(snippet, '^\s*', ''));
        first_kw := substring(first_kw from '^[a-z]+');

        IF first_kw IN ('select','with') THEN
        -- FULL-SQL mode: snippet must yield (osm_id,name, geom)
            sql_text := format($fmt$
                INSERT INTO staging.rule_hits (osm_id, class_a, name, geom)
                SELECT osm_id, %L AS class_a,name, geom
                FROM (
                %s
                ) AS subq;
            $fmt$, rec.class_a, snippet);
        ELSE
            RAISE EXCEPTION 'Unrecognized snippet for class % at % (expected SELECT/WITH)', rec.class_a, resolved_path;
        END IF;
    END IF;
    
    EXECUTE sql_text;
  END LOOP;
END$$;

CREATE INDEX IF NOT EXISTS rule_hits_class ON staging.rule_hits (class_a);
CREATE INDEX IF NOT EXISTS rule_hits_geom_gix ON staging.rule_hits USING GIST (geom);

-- =========================================================
-- 4) Geometry normalization per-class policy
--      (area | point_all | point_if_small [< min_area_m2])
-- =========================================================
DROP MATERIALIZED VIEW IF EXISTS staging.rule_hits_normalized CASCADE;
CREATE MATERIALIZED VIEW staging.rule_hits_normalized AS
SELECT
  r.osm_id,
  r.class_a,
  r.name,
  CASE
    WHEN COALESCE(c.polygon_policy, 'area') = 'area' THEN r.geom

    WHEN COALESCE(c.polygon_policy, 'area') = 'point_all'
         AND GeometryType(r.geom) IN ('POLYGON','MULTIPOLYGON')
      THEN ST_PointOnSurface(r.geom)
    WHEN COALESCE(c.polygon_policy, 'area') = 'point_all'
      THEN r.geom  -- don't touch non-polygons

    WHEN COALESCE(c.polygon_policy, 'area') = 'point_if_small'
         AND GeometryType(r.geom) IN ('POLYGON','MULTIPOLYGON')
         AND ST_Area(ST_Transform(r.geom, 4326)::geography) < COALESCE(c.min_area_m2, 1000)
      THEN ST_PointOnSurface(r.geom)
    WHEN COALESCE(c.polygon_policy, 'area') = 'point_if_small'
      THEN r.geom  -- no threshold or non-polygon -> no change

    ELSE r.geom    -- unknown policy -> no change
  END AS geom
FROM staging.rule_hits r
JOIN staging.class_a_config c USING (class_a);

-- -- =========================================================
-- -- 5) Fix invalid geometries
-- -- =========================================================
DROP MATERIALIZED VIEW IF EXISTS staging.classified_valid CASCADE;
CREATE MATERIALIZED VIEW staging.classified_valid AS
SELECT
  osm_id,
  class_a,
  name,
  CASE
    WHEN NOT ST_IsValid(geom) AND GeometryType(geom) IN ('POLYGON','MULTIPOLYGON') -- only check polygons - points and lines are unlikely to have invalid geoms
      THEN ST_CollectionExtract(ST_MakeValid(geom), 3)  -- 3 = POLYGON/MULTIPOLYGON
    ELSE geom
  END AS geom
FROM staging.rule_hits_normalized
WHERE NOT ST_IsEmpty(geom); -- removing empty geometries

CREATE INDEX IF NOT EXISTS classified_valid_class 
  ON staging.classified_valid (class_a);

CREATE INDEX IF NOT EXISTS classified_valid_geom_gix 
  ON staging.classified_valid USING GIST (geom);

-- =========================================================
-- 6) Drop points of same class that INTERSECT polygons of same class
--    - Keep all polygons
--    - Keep only those points that do NOT intersect a polygon of the same class
-- =========================================================
CREATE SCHEMA IF NOT EXISTS model;

DROP MATERIALIZED VIEW IF EXISTS model.amenities_polygons CASCADE;
CREATE MATERIALIZED VIEW model.amenities_polygons AS
SELECT *
FROM staging.classified_valid
WHERE GeometryType(geom) IN ('POLYGON','MULTIPOLYGON');

CREATE INDEX IF NOT EXISTS amenities_polygons_geom_gix
  ON model.amenities_polygons USING GIST (geom);
CREATE INDEX IF NOT EXISTS amenities_polygons_class
  ON model.amenities_polygons (class_a);

DROP MATERIALIZED VIEW IF EXISTS model.amenities_points CASCADE;
CREATE MATERIALIZED VIEW model.amenities_points AS
WITH pts AS (
  SELECT *
  FROM staging.classified_valid
  WHERE GeometryType(geom) IN ('POINT','MULTIPOINT')
),
filtered AS (
  SELECT p.*
  FROM pts p
  WHERE NOT EXISTS (
    SELECT 1
    FROM model.amenities_polygons a
    WHERE a.class_a = p.class_a
      AND ST_Intersects(a.geom, p.geom)
  )
)
SELECT * FROM filtered;

CREATE INDEX IF NOT EXISTS amenities_points_geom_gix
  ON model.amenities_points USING GIST (geom);
CREATE INDEX IF NOT EXISTS amenities_points_class
  ON model.amenities_points (class_a);

-- (Optional convenience view combining both, if you want a single read endpoint)
CREATE OR REPLACE VIEW model.amenities AS
SELECT class_a, geom FROM model.amenities_points
UNION ALL
SELECT class_a, geom FROM model.amenities_polygons;

-- =========================================================
-- 7) Roads & Entrances + parent tags from unified_features_mat
-- =========================================================

-- Roads (only features having highway)
DROP MATERIALIZED VIEW IF EXISTS model.roads CASCADE;
CREATE MATERIALIZED VIEW model.roads AS
SELECT
  osm_id AS road_osm_id,
  highway,
  name,
  geom
FROM public.raw_lines_roads
WHERE highway IS NOT NULL AND highway <> '';

CREATE INDEX IF NOT EXISTS roads_geom_gix ON model.roads USING GIST (geom);
CREATE INDEX IF NOT EXISTS roads_highway  ON model.roads (highway);

-- Entrances with parent tags
DROP MATERIALIZED VIEW IF EXISTS model.entrances CASCADE;
CREATE MATERIALIZED VIEW model.entrances AS
WITH polys AS (
  SELECT
    p.osm_id AS parent_osm_id,
    p.class_a,
    p.name,
    p.geom
  FROM model.amenities_polygons p
),
ix AS (
  -- intersections of polygon boundary with roads
  SELECT
    p.parent_osm_id,
    p.class_a,
    p.name,
    r.road_osm_id,
    r.highway,  -- road highway
    ST_Intersection(ST_Boundary(p.geom), r.geom) AS gi
  FROM polys p
  JOIN model.roads r
    ON ST_Intersects(r.geom, ST_Boundary(p.geom))
),
ix_pts AS (
  -- extract point intersections
  SELECT
    parent_osm_id,
    class_a,
    name,
    road_osm_id,
    highway,
    (dp).geom::geometry(Point, 3857) AS geom,
    'road_intersection'::text        AS source
  FROM ix
  CROSS JOIN LATERAL ST_Dump(ST_CollectionExtract(gi, 1)) AS dp
),
ix_line_mid AS (
  -- overlapping linear intersections -> midpoint
  SELECT
    parent_osm_id,
    class_a,
    name,
    road_osm_id,
    highway,
    ST_LineInterpolatePoint((dl).geom, 0.5)::geometry(Point, 3857) AS geom,
    'road_intersection_line'::text                                  AS source
  FROM ix
  CROSS JOIN LATERAL ST_Dump(ST_CollectionExtract(gi, 2)) AS dl
),
hits AS (
  SELECT * FROM ix_pts
  UNION ALL
  SELECT * FROM ix_line_mid
),
nohit_polys AS (
  -- polygons with no boundary intersection -> centroid as entrance
  SELECT
    p.osm_id AS parent_osm_id,
    p.class_a,
    p.name,
    NULL::bigint                    AS road_osm_id,
    NULL::text                      AS highway,
    ST_Centroid(p.geom)::geometry(Point, 3857) AS geom,
    'centroid'::text                AS source
  FROM model.amenities_polygons p
  WHERE NOT EXISTS (
    SELECT 1
    FROM model.roads r
    WHERE ST_Intersects(r.geom, ST_Boundary(p.geom))
  )
),
all_points AS (
  -- include all point amenities as entrances
  SELECT
    osm_id       AS parent_osm_id,
    class_a,
    name,
	NULL::bigint AS road_osm_id,
    NULL::text   AS highway,
    geom::geometry(Point, 3857) AS geom,
    'point'::text               AS source
FROM model.amenities_points
),
u AS (
  SELECT * FROM hits
  UNION ALL
  SELECT * FROM nohit_polys
  UNION ALL
  SELECT * FROM all_points
)
SELECT
  u.parent_osm_id,
  u.class_a,
  u.name,
  u.geom,
  u.source,

  -- parent tags from unified_features_mat (prefix parent_)
  uf.amenity                 AS parent_amenity,
  uf.shop                    AS parent_shop,
  uf.leisure                 AS parent_leisure,
  uf.office                  AS parent_office,
  uf.tourism                 AS parent_tourism,
  uf.healthcare              AS parent_healthcare,
  uf.place                   AS parent_place,
  uf.natural                 AS parent_natural,
  uf.landuse                 AS parent_landuse,
  uf.boundary                AS parent_boundary,
  uf.post_office             AS parent_post_office,
  uf.building                AS parent_building,
  uf.man_made                AS parent_man_made,
  uf.sport                   AS parent_sport,
  uf.railway                 AS parent_railway,
  uf.public_transport        AS parent_public_transport,
  uf.highway                 AS parent_highway,
  uf.bus                     AS parent_bus,
  uf.tram                    AS parent_tram,
  uf.train                   AS parent_train,
  uf.origin                  AS parent_origin,
  uf.isced_level             AS parent_isced_level,
  uf.healthcare_speciality   AS parent_healthcare_speciality
FROM u
LEFT JOIN staging.unified_features_mat uf
  ON uf.osm_id = u.parent_osm_id;

CREATE INDEX IF NOT EXISTS entrances_geom_gix   ON model.entrances USING GIST (geom);
CREATE INDEX IF NOT EXISTS entrances_parent_idx ON model.entrances (parent_osm_id);
CREATE INDEX IF NOT EXISTS entrances_class_idx  ON model.entrances (class_a);
CREATE INDEX IF NOT EXISTS entrances_source_idx ON model.entrances (source);

-- -- Done