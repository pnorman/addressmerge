#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Basic modules
import logging as l
l.basicConfig(level=l.DEBUG)
from collections import deque
import copy

# Database modules
import psycopg2
import psycopg2.extras

# .osm modules
# This imports the single-threaded XML parser from imposm.
# This is more reliable with strangely formatted but valid XML
# and the speed differences don't matter for small files
from imposm.parser.xml.parser import XMLParser as OSMParser
from lxml import etree

class OSMSource(object):
    def __init__(self, database, user, password, host, port, wkt, strippable, changes, buffer):
        l.debug('Connecting to postgresql')
        self._conn=psycopg2.connect(database=database, user=user, 
                                    password=password, host=host, 
                                    port=str(port))
        self._conn.set_session(readonly=False, autocommit=False)
        psycopg2.extras.register_hstore(self._conn, unicode=True)
        self.wkt = wkt
        self.strippable = strippable
        self.buffer = buffer
        self.validate_wkt()
        self.create_tables()
        if changes:
            self.create_change_tables()

    def validate_wkt(self):
        '''
        This function checks that self.wkt is a valid WKT string. It will also fail if
        the DB does not have PostGIS enabled or it could not connect.
        '''
        l.debug('Validating WKT')
        curs = None
        try:
            curs = self._conn.cursor()
            curs.execute('''SELECT ST_GeomFromText(%s,4326);''', (self.wkt,))
            curs.connection.rollback()
        except BaseException:
            if curs is not None:
                curs.connection.rollback()
            raise
        finally:
            if curs is not None:
                curs.close()

    def create_tables(self):
        l.debug('Creating tables')
        curs = None
        try:
            curs = self._conn.cursor()
            curs.execute('''CREATE TEMPORARY TABLE import_addresses
                            (import_id integer PRIMARY KEY,
                            geom geometry,
                            tags hstore,
                            pending_delete boolean DEFAULT FALSE);''')

            curs.execute('''CREATE TEMPORARY VIEW local_nodes AS
                            SELECT id, tags, geom FROM nodes
                            WHERE ST_Intersects(geom, ST_GeomFromText(%s,4326));''',
                            (self.wkt,))
            curs.execute('''CREATE TEMPORARY VIEW local_ways AS
                            SELECT id, tags, linestring AS geom FROM ways
                            WHERE ST_Intersects(linestring,ST_GeomFromText(%s,4326));''',
                            (self.wkt,))
            curs.execute('''CREATE TEMPORARY VIEW local_mps AS
                            SELECT relation_id AS id,
                                relation_tags AS tags,
                                ST_MakeLine(relation_ways.linestring) AS geom
                            FROM (SELECT relations.id AS relation_id,
                                relations.tags AS relation_tags, geom AS linestring
                            FROM local_ways JOIN relation_members
                            ON (local_ways.id = relation_members.member_id
                            AND relation_members.member_type='W')
                            JOIN relations
                            ON (relation_members.relation_id = relations.id)
                            WHERE relations.tags @> hstore('type','multipolygon'))
                            AS relation_ways
                            GROUP BY relation_id, relation_tags;''')

            curs.execute('''CREATE TEMPORARY TABLE local_all
                            (id, type, geom, tags)
                            AS (SELECT id, type, geom, tags FROM (SELECT local_nodes.id, 'N'::character(1) AS type,
                                local_nodes.tags, local_nodes.geom FROM local_nodes
                            UNION ALL SELECT local_ways.id, 'W'::character(1) AS type,
                                local_ways.tags, local_ways.geom FROM local_ways
                            UNION ALL SELECT local_mps.id, 'M'::character(1) AS type,
                                local_mps.tags, local_mps.geom FROM local_mps) AS everything);''')

            l.debug('Indexing and analyzing tables')
            curs.execute('''ALTER TABLE local_all ADD PRIMARY KEY (type, id) WITH (FILLFACTOR=100);''')
            curs.execute('''CREATE INDEX ON local_all USING gist (geom) WITH (FILLFACTOR=100);''')
            curs.execute('''CREATE INDEX local_all_addr_idx ON local_all USING btree
                            ((local_all.tags -> 'addr:housenumber'),
                            (local_all.tags -> 'addr:street'),
                            (local_all.tags -> 'addr:city'))
                             WITH (FILLFACTOR=100)''')

            curs.execute('''ANALYZE local_all;''')

            l.debug('Committing transaction')
            curs.connection.commit()

        except BaseException:
            if curs is not None:
                curs.connection.rollback()
            raise
        finally:
            if curs is not None:
                curs.close()

    def create_change_tables(self):
        curs = None
        try:
            curs = self._conn.cursor()
            curs.execute('''CREATE TEMPORARY TABLE changed_nodes
                            (id bigint PRIMARY KEY CHECK (id > 0),
                            version integer CHECK (version > 1),
                            tags hstore,
                            geom geometry);''')
            curs.execute('''CREATE TEMPORARY TABLE changed_ways
                            (id bigint PRIMARY KEY CHECK (id > 0),
                            version integer CHECK (version > 1),
                            tags hstore,
                            nodes bigint[]);''')
            curs.execute('''CREATE TEMPORARY TABLE changed_relations
                            (id bigint PRIMARY KEY CHECK (id > 0),
                            version integer CHECK (version > 1),
                            tags hstore,
                            types character(1)[],
                            ids bigint[],
                            roles text[]);''')
        except BaseException:
            if curs is not None:
                curs.connection.rollback()
            raise
        finally:
            if curs is not None:
                curs.close()

    def load_addresses(self, addresses):
        l.debug('Loading addresses')
        curs = None
        try:
            curs = self._conn.cursor()
            for (id, tags, (x, y)) in addresses:
                curs.execute('''INSERT INTO import_addresses
                                (import_id, geom, tags)
                                VALUES (%s, ST_SetSRID(ST_MakePoint(%s, %s), 4326), %s);''',
                                (id, x, y, tags))
            l.debug('Indexing and analyzing tables')
            curs.execute('''CREATE INDEX import_addresses_addr_idx ON import_addresses USING btree
                            ((import_addresses.tags -> 'addr:housenumber'),
                            (import_addresses.tags -> 'addr:street'),
                            (import_addresses.tags -> 'addr:city'))
                            WITH (FILLFACTOR=100);''')
            curs.execute('''ANALYZE import_addresses;''')
            curs.connection.commit()
        except BaseException:
            if curs is not None:
                curs.connection.rollback()
            raise
        finally:
            if curs is not None:
                curs.close()

    def find_duplicates(self):
        l.debug('Finding duplicates')
        curs = None
        try:
            curs = self._conn.cursor()
            curs.execute('''DELETE FROM import_addresses USING local_all
                            WHERE local_all.tags -> 'addr:housenumber' = import_addresses.tags -> 'addr:housenumber'
                            AND (local_all.tags -> 'addr:housenumber') IS NOT NULL
                            AND local_all.tags -> 'addr:street' = import_addresses.tags -> 'addr:street'
                            AND (local_all.tags -> 'addr:housenumber') IS NOT NULL
                            AND local_all.tags -> 'addr:city' = import_addresses.tags -> 'addr:city'
                            AND (local_all.tags -> 'addr:housenumber') IS NOT NULL
                            RETURNING import_addresses.import_id;''')
            deleted = set(id[0] for id in curs.fetchall())
            curs.execute('''ALTER TABLE import_addresses
                            ADD COLUMN buffered_geom geometry;''')
            curs.execute('''UPDATE import_addresses
                            SET buffered_geom = geometry(ST_Buffer(geography(geom),%s));''',
                            (self.buffer,))
            curs.execute('''CREATE INDEX ON import_addresses
                            USING gist (buffered_geom)
                            WITH (FILLFACTOR=100);''')
            curs.execute('''COMMIT;''')
            curs.execute('''VACUUM ANALYZE import_addresses;''')
            return deleted
        except BaseException:
            if curs is not None:
                curs.connection.rollback()
            raise
        finally:
            if curs is not None:
                curs.close()

    def generate_changes(self, nocity=None, building=None):
        deleted = set()
        curs = None
        try:
            curs = self._conn.cursor()
            if nocity is not None:
                curs.execute('''WITH to_delete AS
                                (UPDATE import_addresses
                                SET pending_delete = TRUE
                                FROM local_all
                                WHERE import_addresses.tags -> 'addr:housenumber' = local_all.tags -> 'addr:housenumber'
                                AND (import_addresses.tags -> 'addr:housenumber') IS NOT NULL
                                AND import_addresses.tags -> 'addr:street' = local_all.tags -> 'addr:street'
                                AND (import_addresses.tags -> 'addr:housenumber') IS NOT NULL
                                AND local_all.type='N'
                                AND ST_Intersects(ST_Buffer(geography(import_addresses.geom),%s)::geometry,local_all.geom)
                                RETURNING local_all.id AS id,
                                (import_addresses.tags || local_all.tags) AS merged_tags)
                                INSERT INTO changed_nodes (id, version, tags, geom)
                                SELECT nodes.id, (nodes.version+1), to_delete.merged_tags, nodes.geom
                                FROM to_delete JOIN nodes ON to_delete.id=nodes.id;''', (nocity,))
                curs.execute('''WITH to_delete AS
                                (UPDATE import_addresses
                                SET pending_delete = TRUE
                                FROM local_all
                                WHERE import_addresses.tags -> 'addr:housenumber' = local_all.tags -> 'addr:housenumber'
                                AND (import_addresses.tags -> 'addr:housenumber') IS NOT NULL
                                AND import_addresses.tags -> 'addr:street' = local_all.tags -> 'addr:street'
                                AND (import_addresses.tags -> 'addr:housenumber') IS NOT NULL
                                AND local_all.type='W'
                                AND ST_Intersects(ST_Buffer(geography(import_addresses.geom),%s)::geometry,local_all.geom)
                                RETURNING local_all.id AS id,
                                (import_addresses.tags || local_all.tags) AS merged_tags)
                                INSERT INTO changed_ways (id, version, tags, nodes)
                                SELECT ways.id, (ways.version+1), to_delete.merged_tags, ways.nodes
                                FROM to_delete JOIN ways ON to_delete.id=ways.id;''', (nocity,))
                # no one likes relations, but we need to support them
                curs.execute('''WITH to_delete AS
                                (UPDATE import_addresses
                                SET pending_delete = TRUE
                                FROM local_all
                                WHERE import_addresses.tags -> 'addr:housenumber' = local_all.tags -> 'addr:housenumber'
                                AND (import_addresses.tags -> 'addr:housenumber') IS NOT NULL
                                AND import_addresses.tags -> 'addr:street' = local_all.tags -> 'addr:street'
                                AND (import_addresses.tags -> 'addr:housenumber') IS NOT NULL
                                AND local_all.type='M'
                                AND ST_Intersects(ST_Buffer(geography(import_addresses.geom),%s)::geometry,local_all.geom)
                                RETURNING local_all.id AS id,
                                (import_addresses.tags || local_all.tags) AS merged_tags)
                                INSERT INTO changed_relations
                                (id, version, tags, types, ids, roles)
                                SELECT id, version, tags,
                                array_agg(member_type) AS types,
                                array_agg(member_id) AS ids,
                                array_agg(member_role) AS roles
                                FROM (SELECT relations.id, (relations.version + 1) AS version,
                                merged_tags AS tags,
                                member_type, member_id, member_role
                                FROM to_delete JOIN relations
                                ON to_delete.id=relations.id
                                JOIN relation_members
                                ON relations.id = relation_members.relation_id
                                ORDER BY sequence_id ASC) AS combined_relations
                                GROUP BY id,version,tags;''', (nocity,))

            if building is not None:
                curs.execute('''CREATE TEMPORARY VIEW building_matches AS -- create this as a view since we'll be using it multiple times, and it's complex
                                  SELECT possible_matches.import_id,
                                    possible_matches.merged_tags,
                                    possible_matches.id, possible_matches.type,
                                    possible_matches.building_geom,
                                    other_import_addresses.import_id AS other_id
                                    FROM (
                                      SELECT
                                        import_id,
                                        (import_addresses.tags || local_all.tags) AS merged_tags,
                                        import_addresses.buffered_geom,
                                        id, type,
                                        ST_MakePolygon(local_all.geom) AS building_geom
                                      FROM import_addresses JOIN local_all
                                      ON import_addresses.buffered_geom && local_all.geom -- buildings aren't polygons yet so we can't use ST_Intersects, but this filter drastically brings down the matches that we need to MakePolygon on
                                      WHERE local_all.tags ? 'building' -- well-formed buildings without addresses
                                        AND (local_all.tags -> 'addr:housenumber') IS NULL
                                        AND ST_IsClosed(local_all.geom)
                                      OFFSET 0 --force the subquery to run without optimizing it out to avoid calling ST_IsValid on a MakePolygon of a non-closed linestring
                                    ) AS possible_matches -- buildings that might match
                                    LEFT JOIN import_addresses AS other_import_addresses -- we want to filter out buildings that would match multiple import addrs
                                      ON possible_matches.import_id != other_import_addresses.import_id -- different point
                                      AND ST_Intersects(possible_matches.building_geom, other_import_addresses.buffered_geom) -- but still in the building.
                                      -- the st_intersects produces a geometry_gist_joinsel notice
                                    LEFT JOIN local_all AS other_osm_addresses -- now we need to filter out cases with nearby OSM addresses
                                      ON ST_DWithin(geography(possible_matches.building_geom), geography(other_osm_addresses.geom), %s, FALSE) -- With the number of matches at this point this is faster than an intersects + buffer
                                      AND (other_osm_addresses.tags -> 'addr:housenumber') IS NOT NULL -- this also enforces the two IDs don't match since the inner select has the inverse of this
                                    WHERE
                                      ST_IsValid(possible_matches.buffered_geom) -- get rid of self-intersecting, etc
                                      AND ST_Intersects(possible_matches.buffered_geom, building_geom) -- needs to be actually within, not just bbox overlap
                                      AND other_import_addresses.import_id IS NULL -- find the ones that don't match to other import addrs
                                      AND other_osm_addresses.type IS NULL; -- or to existing addrs''', (building,))
                curs.execute('''WITH to_delete AS (
                                  UPDATE import_addresses
                                    SET pending_delete = TRUE
                                  FROM building_matches
                                    WHERE import_addresses.import_id = building_matches.import_id
                                      AND building_matches.type = 'W'
                                    RETURNING building_matches.id AS id, building_matches.merged_tags )
                                INSERT INTO changed_ways (id, version, tags, nodes)
                                  SELECT ways.id, (ways.version+1), to_delete.merged_tags, ways.nodes
                                    FROM to_delete JOIN ways ON to_delete.id=ways.id;''')
            curs.execute('''DELETE FROM import_addresses
                            WHERE pending_delete
                            RETURNING import_id;''')
            deleted |= set(id[0] for id in curs.fetchall())
            curs.execute('''ANALYZE import_addresses;''')
            curs.connection.commit()

            return deleted
        except BaseException:
            if curs is not None:
                curs.connection.rollback()
            raise
        finally:
            if curs is not None:
                curs.close()

    def get_changed_nodes(self):
        curs = None
        try:
            curs = self._conn.cursor()
            curs.execute('''SELECT id, version, tags-%s, ST_X(geom) AS x, ST_Y(geom) AS y FROM changed_nodes;''',(self.strippable,))
            curs.connection.rollback()
            return (curs.fetchall())
        except BaseException:
            if curs is not None:
                curs.connection.rollback()
            raise
        finally:
            if curs is not None:
                curs.close()
    def get_changed_ways(self):
        curs = None
        try:
            curs = self._conn.cursor()
            curs.execute('''SELECT id, version, tags-%s, nodes FROM changed_ways;''',(self.strippable,))
            curs.connection.rollback()
            return (curs.fetchall())
        except BaseException:
            if curs is not None:
                curs.connection.rollback()
            raise
        finally:
            if curs is not None:
                curs.close()
    def get_changed_relations(self):
        curs = None
        try:
            curs = self._conn.cursor()
            curs.execute('''SELECT id, version, tags-%s, types, ids, roles FROM changed_relations;''',(self.strippable,))
            curs.connection.rollback()
            return (curs.fetchall())
        except BaseException:
            if curs is not None:
                curs.connection.rollback()
            raise
        finally:
            if curs is not None:
                curs.close()

class ImportDocument(object):
    def __init__(self, input):
        self._parser = OSMParser(nodes_callback=self._parse_nodes)
        self._nodes = deque()
        l.debug('Parsing %s', input)
        self._parser.parse(input)

    def _parse_nodes(self, nodes):
        for node in nodes:
            self._nodes.append(node)

    def _serialize_node(self, f, node):
        xmlnode = etree.Element('node', {'visible':'true', 'id':str(node[0]), 'lon':str(node[2][0]), 'lat':str(node[2][1])})
        for (k,v) in node[1].items():
            tag = etree.Element('tag',  {'k':k, 'v':v})
            xmlnode.append(tag)

        f.write(etree.tostring(xmlnode, pretty_print=True))

    def _serialize_modify_node(self, f, node):
        # A node is a tuple of (id, version, tags, x, y)
        xmlnode = etree.Element('node', {'id':str(node[0]), 'version':str(node[1]),  'lon':str(node[3]), 'lat':str(node[4])})
        for (k,v) in node[2].items():
            tag = etree.Element('tag',  {'k':k, 'v':v})
            xmlnode.append(tag)

        f.write(etree.tostring(xmlnode, pretty_print=True))

    def _serialize_modify_way(self, f, way):
        # A way is a tuple of (id, version, tags, nodes)
        xmlway = etree.Element('way', {'id':str(way[0]), 'version':str(way[1])})
        for ref in way[3]:
            nd = etree.Element('nd', {'ref':str(ref)})
            xmlway.append(nd)
        for (k,v) in way[2].items():
            tag = etree.Element('tag',  {'k':k, 'v':v})
            xmlway.append(tag)

        f.write(etree.tostring(xmlway, pretty_print=True))

    def _serialize_modify_relation(self, f, relation):
        # A relation is a tuple of (id, version, tags, types, ids, roles)
        xmlrelation = etree.Element('relation', {'id':str(relation[0]), 'version':str(relation[1])})
        typelookup = {'N':'node', 'W':'way', 'R':'relation'}
        for i in xrange(0, len(relation[3])):
            member = etree.Element('member', {'type':typelookup[str(relation[3][i])], 'ref':str(relation[4][i]), 'role':str(relation[5][i])})
            xmlrelation.append(member)
        for (k,v) in relation[2].items():
            tag = etree.Element('tag',  {'k':k, 'v':v})
            xmlrelation.append(tag)

        f.write(etree.tostring(xmlrelation, pretty_print=True))

    def remove_existing(self, existing):
        existing.load_addresses(self._nodes)
        duplicates = existing.find_duplicates()
        l.debug('Removing duplicates')
        self._nodes = filter(lambda node: node[0] not in duplicates, self._nodes)
        l.debug('%d duplicates removed', len(duplicates))

    def remove_changed(self, existing, **kwargs):
        duplicates = existing.generate_changes(**kwargs)
        l.debug('Removing changed')
        self._nodes = filter(lambda node: node[0] not in duplicates, self._nodes)
        l.debug('%d changed removed', len(duplicates))

    def output_osm(self, f):
        f.write('<?xml version="1.0"?>\n<osm version="0.6" upload="false" generator="addressmerge">\n')
        for node in self._nodes:
            self._serialize_node(f, node)

        f.write('</osm>\n')

    def output_osc(self, existing, f):
        f.write('<?xml version="1.0"?>\n<osmChange version="0.6" upload="false" generator="addressmerge">\n')
        f.write('<modify>\n')
        for node in existing.get_changed_nodes():
            self._serialize_modify_node(f, node)

        for way in existing.get_changed_ways():
            self._serialize_modify_way(f, way)

        for relation in existing.get_changed_relations():
            self._serialize_modify_relation(f, relation)
        f.write('</modify>\n')
        f.write('</osmChange>\n')

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Conflate an address file with existing OSM data')

    # Basic options
    verbosity = parser.add_mutually_exclusive_group()
    verbosity.add_argument("-v", "--verbose", action="store_true")
    verbosity.add_argument("-q", "--quiet", action="store_true")

    database_group = parser.add_argument_group('Database options', 'Options that effect the database connection')
    database_group.add_argument('-d', '--dbname', default='osm', help='Database to connect to. Defaults to osm.')
    database_group.add_argument('-U', '--username', default='osm', help='Username for database. Defaults to osm.')
    database_group.add_argument('--host', default='localhost', help='Hostname for database. Defaults to localhost.')
    database_group.add_argument('-p', '--port', default=5432, type=int, help='Port for database. Defaults to 5432.')
    database_group.add_argument('-P', '--password', default='osm',  help='Password for database. Defaults to osm.')

    file_group = parser.add_argument_group('File options', 'Options that effect the input and output files')
    file_group.add_argument('input', help='Input OSM file')
    file_group.add_argument('output', type=argparse.FileType('w'), help='Output OSM file')
    file_group.add_argument('--osc', type=argparse.FileType('w'), default=None, help='Output OSC file')
    file_group.add_argument('-w', '--wkt', type=argparse.FileType('r'), help='Well-known text (WKT) file with a POLYGON or other area type to search for addresses in', required=True)
    file_group.add_argument('-r', '--remove-tags', type=argparse.FileType('r'), default=None, help='File with list of tags to remove from any modified objects')

    matching_group = parser.add_argument_group('Matching options', 'Options that effect the .osc results. Output OSC file required')
    matching_group.add_argument('--nocity', type=float, default=None, help='Distance to detect matches without a city')
    matching_group.add_argument('--building', type=float, default=None, help='Distance to search around buildings for existing OSM addresses')

    other_group = parser.add_argument_group('Other options')
    other_group = other_group.add_argument('--buffer', type=float, default=0.5, help='Buffer distance in meters around existing addresses')

    args = parser.parse_args()

    if args.osc is None:
        if args.nocity is not None:
            raise argparse.ArgumentTypeError('--osc is required if diff generating options are used')

    if args.remove_tags is None:
        striplist = set(['created_by', 'odbl', 'odbl:note'])
    else:
        striplist = set(line.strip() for line in args.remove_tags.readlines()).union(set(['created_by', 'odbl', 'odbl:note']))

    existing = OSMSource( database=args.dbname, user=args.username,
                          password=args.password, host=args.host,
                          port=str(args.port),
                          wkt=args.wkt.read(),
                          strippable=list(striplist),
                          changes=args.osc!=None,
                          buffer=args.buffer)


    source = ImportDocument(args.input)

    source.remove_existing(existing)
    source.remove_changed(existing, nocity=args.nocity, building=args.building)
    source.output_osm(args.output)
    source.output_osc(existing, args.osc)
