DROP INDEX IF EXISTS osm_point_addresses_city_idx, 
osm_point_addresses_housenumber_idx, osm_point_addresses_street_idx, 
osm_polygon_addresses_city_idx, osm_polygon_addresses_housenumber_idx,
osm_polygon_addresses_street_idx;

CREATE INDEX osm_point_addresses_city_idx ON osm_point_addresses ("addr:housenumber") WITH (FILLFACTOR=99);
CREATE INDEX osm_point_addresses_housenumber_idx ON osm_point_addresses ("addr:street") WITH (FILLFACTOR=99);
CREATE INDEX osm_point_addresses_street_idx ON osm_point_addresses ("addr:city") WITH (FILLFACTOR=99);
CREATE INDEX osm_polygon_addresses_city_idx ON osm_polygon_addresses ("addr:city") WITH (FILLFACTOR=99);
CREATE INDEX osm_polygon_addresses_housenumber_idx ON osm_polygon_addresses ("addr:housenumber") WITH (FILLFACTOR=99);
CREATE INDEX osm_polygon_addresses_street_idx ON osm_polygon_addresses ("addr:street") WITH (FILLFACTOR=99);
VACUUM ANALYZE;
