--[[

Purpose
  Define import schema for three raw tables used later in SQL pipeline:
      - public.raw_points_of_interest (POINT)
      - public.raw_areas_of_interest  (MULTIPOLYGON)
      - public.raw_lines_roads        (LINESTRING)
  Keep only OSM features that have at least one of the primary classification keys.
  Preserve a small set of refinement keys for downstream analysis.

Key conventions
  SRID is configured via srid (default 3857).
  We store both osm_id (int64) and a human‑readable osm_uid (text: node_/way_/relation_ prefix).
  Colon in OSM keys is converted to underscore so SQL columns are valid identifiers.

Tables created
  raw_points_of_interest(osm_id, osm_uid, name, <tags>, geom Point)
  raw_areas_of_interest (osm_id, osm_uid, name, <tags>, geom MultiPolygon)
  raw_lines_roads       (osm_id, osm_uid, name, <tags>, geom LineString) - only when highway tag exists

--]]

-- ========================================================
-- 1) Classification keys
--    LIST A decides if a feature is kept at all (must have a non-empty value).
--    LIST B is copied if present but doesn’t affect inclusion.
-- ========================================================

-- LIST A (primary classification keys)
local KEYS_CLASS_MAIN = {
    'shop','amenity','office','tourism','healthcare','place','natural','landuse',
    'leisure','boundary','post_office','building','man_made','sport',
    'railway','public_transport','highway'
}

-- LIST B (refinement keys) – not used to decide, but stored in tags
local KEYS_CLASS_REFINE = {
    'bus','tram','train','origin','isced:level','healthcare:speciality'
}

-- Turn LIST A into a fast lookup table for inclusion checks
local KEEP_KEYS = {}
for _, k in ipairs(KEYS_CLASS_MAIN) do
    KEEP_KEYS[k] = true
end

-- Return true if any LIST A key exists and has a value
-- for faster checks can precompile a function via osm2pgsql.make_check_values_func.
local function keep_feature(tags)
    for k, _ in pairs(KEEP_KEYS) do
        if tags[k] and tags[k] ~= '' then
            return true
        end
    end
    return false
end

-- ========================================================
-- 2) Target projection
-- ========================================================
local srid = 3857

-- Container for table handles returned by osm2pgsql.define_*_table
local tables = {}

-- ========================================================
-- 3) Helpers
-- ========================================================

-- Replace ':' with '_' so tag keys become valid SQL column names
local function sanitize_key(key)
    return string.gsub(key, ":", "_")
end

-- Build common column list for point features (duplicated for areas/lines with geom swapped)
local columns_points = {}

-- Add all classification + refinement keys as text columns
for _, k in ipairs(KEYS_CLASS_MAIN) do
    table.insert(columns_points, { column = sanitize_key(k), type = 'text' })
end
for _, k in ipairs(KEYS_CLASS_REFINE) do
    table.insert(columns_points, { column = sanitize_key(k), type = 'text' })
end

-- Geometry + identifiers + common name
table.insert(columns_points, { column = 'geom', type = 'point',       projection = srid, not_null = true })
table.insert(columns_points, { column = 'osm_id',  type = 'int8'  })   -- original numeric OSM id
table.insert(columns_points, { column = 'osm_uid', type = 'text'  })   -- prefixed id: node_/way_/relation_
table.insert(columns_points, { column = 'name',    type = 'text'  })

-- For areas: copy and swap geometry type to multipolygon
local columns_poly = {}
for _, col in ipairs(columns_points) do
    if col.column == 'geom' then
        table.insert(columns_poly, { column = 'geom', type = 'multipolygon', projection = srid, not_null = true })
    else
        table.insert(columns_poly, col)
    end
end

-- For lines (roads): keep only what's needed downstream
local columns_lines = {
    { column = 'highway', type = 'text' },
    { column = 'name',    type = 'text' },
    { column = 'geom',    type = 'linestring', projection = srid, not_null = true },
    { column = 'osm_id',  type = 'int8' },
    { column = 'osm_uid', type = 'text' }
}

-- Define output tables (one per geometry type)
tables.raw_points_of_interest = osm2pgsql.define_node_table('raw_points_of_interest', columns_points)
tables.raw_areas_of_interest  = osm2pgsql.define_area_table('raw_areas_of_interest',  columns_poly)
tables.raw_lines_roads        = osm2pgsql.define_way_table ('raw_lines_roads',        columns_lines)

-- ========================================================
-- 4) Area detection
--    Decide whether a closed way/relation is an area based on tags.
--    Mirrors osm2pgsql standard heuristics with a curated set of keys.
-- ========================================================
local function has_area_tags(tags)
    if tags.area == 'yes' then
        return true
    end
    if tags.area == 'no' then
        return false
    end

    -- Heuristic: these tags typically imply area semantics
    return tags.aeroway
        or tags.amenity
        or tags.boundary
        or tags.building
        or tags.harbour
        or tags.historic
        or tags.landuse
        or tags.leisure
        or tags.man_made
        or tags.military
        or tags.natural
        or tags.office
        or tags.place
        or tags.power
        or tags.public_transport
        or tags.shop
        or tags.sport
        or tags.tourism
        or tags.water
        or tags.waterway
        or tags.wetland
        or tags['abandoned:aeroway']
        or tags['abandoned:amenity']
        or tags['abandoned:building']
        or tags['abandoned:landuse']
        or tags['abandoned:power']
        or tags['area:highway']
        or tags['building:part']
end

-- Extract the tag subset we keep as columns for a given object
local function extract_tag_fields(tags)
    local row = {}
    for _, k in ipairs(KEYS_CLASS_MAIN) do
        row[sanitize_key(k)] = tags[k]
    end
    for _, k in ipairs(KEYS_CLASS_REFINE) do
        row[sanitize_key(k)] = tags[k]
    end
    row['name'] = tags['name']
    return row
end

-- ========================================================
-- 5) Ingest callbacks
--    Called by osm2pgsql for each OSM element type. Early‑exit unless keep_feature(tags) is true.
-- ========================================================

function osm2pgsql.process_node(object)
    if not keep_feature(object.tags) then return end

    local row = extract_tag_fields(object.tags)
    row.geom    = object:as_point()
    row.osm_id  = object.id
    row.osm_uid = 'node_' .. object.id
    tables.raw_points_of_interest:insert(row)
end

function osm2pgsql.process_way(object)
    if not keep_feature(object.tags) then return end

    if object.is_closed and has_area_tags(object.tags) then
        -- Closed way with area semantics -> MULTIPOLYGON
        local row = extract_tag_fields(object.tags)
        row.geom    = object:as_polygon()
        row.osm_id  = object.id
        row.osm_uid = 'way_' .. object.id
        tables.raw_areas_of_interest:insert(row)
    else
        -- Open way: keep only if it’s a road/highway (feeds model.roads later)
        if object.tags.highway and object.tags.highway ~= '' then
            local row = extract_tag_fields(object.tags)
            row.geom    = object:as_linestring()
            row.osm_id  = object.id
            row.osm_uid = 'way_' .. object.id
            tables.raw_lines_roads:insert(row)
        else
            -- Other open ways are ignored to keep DB lean
            return
        end
        return
    end
end

function osm2pgsql.process_relation(object)
    if not keep_feature(object.tags) then return end

    local relation_type = object:grab_tag('type')
    if (relation_type == 'multipolygon' or relation_type == 'boundary') and has_area_tags(object.tags) then
        -- Multipolygon/boundary relations -> MULTIPOLYGON
        local mp  = object:as_multipolygon()
        local row = extract_tag_fields(object.tags)
        row.geom    = mp
        row.osm_id  = object.id
        row.osm_uid = 'relation_' .. object.id
        tables.raw_areas_of_interest:insert(row)
    else
        -- Other relations are ignored
        return
    end
end
