#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Basic modules
import logging as l
l.basicConfig(level=l.DEBUG)
from collections import deque
import copy

# Database modules
import psycopg2

# .osm modules
from imposm.parser import OSMParser
from lxml import etree

class OSMSource(object):
    def __init__(self, database, user, password, host, port):
        l.debug('Connecting to postgresql')
        self._conn=psycopg2.connect(database=database, user=user, 
                                    password=password, host=host, 
                                    port=str(port))
        self._conn.set_session(readonly=False, autocommit=False)
        self._table_created = False

    def create_table(self):
        curs = None
        try:
            curs = self._conn.cursor()
            curs.execute('''CREATE TEMPORARY TABLE import_addresses (
                            import_id integer,
                            "addr:housenumber" varchar(255),
                            "addr:street" varchar(255),
                            "addr:city" varchar(255));''')
            self._table_created = True
            curs.connection.commit()
        except BaseException:
            if curs is not None:
                curs.connection.rollback()
            raise
        finally:
            if curs is not None:
                curs.close()

    def load_addresses(self, addresses):
        self.create_table()
        curs = None
        try:
            curs = self._conn.cursor()
            for (id, tags, _) in addresses:
                curs.execute('''INSERT INTO import_addresses
                                (import_id, "addr:housenumber", "addr:street", "addr:city")
                                VALUES (%s, %s, %s, %s);''',
                                (id, tags['addr:housenumber'], tags['addr:street'], tags['addr:city']))
            curs.execute('''ANALYZE;''')
            curs.connection.commit()
        except BaseException:
            if curs is not None:
                curs.connection.rollback()
            raise
        finally:
            if curs is not None:
                curs.close()

    def find_duplicates(self):
        curs = None
        try:
            curs = self._conn.cursor()
            curs.execute('''SELECT DISTINCT ON (import_id) import_id FROM osm_addresses JOIN import_addresses USING ("addr:housenumber", "addr:street", "addr:city");''')
            return set(id[0] for id in curs.fetchall())
        except BaseException:
            if curs is not None:
                curs.connection.rollback()
            raise
        finally:
            if curs is not None:
                curs.close()

class ImportDocument(object):
    newNodes = []
    def __init__(self):
        self._parser = OSMParser(nodes_callback=self._parse_nodes)
        self._nodes = deque()

    def parse_osm(self, filename):
        l.debug('Parsing %s', filename)
        self._parser.parse(filename)

    def _parse_nodes(self, nodes):
        for node in nodes:
            self._nodes.append(node)

    def _serialize_node(self, f, node):
        xmlnode = etree.Element('node', {'visible':'true', 'id':str(node[0]), 'lon':str(node[2][0]), 'lat':str(node[2][1])})
        for (k,v) in node[1].items():
            tag = etree.Element('tag',  {'k':k, 'v':v})
            xmlnode.append(tag)

        f.write(etree.tostring(xmlnode))
        f.write('\n')

    def remove_existing(self, source):
        ''' This function will remove addresses that already exist in OSM'''
        l.debug('Loading addresses')
        source.load_addresses(self._nodes)
        l.debug('Finding duplicates')
        duplicates = source.find_duplicates()
        l.debug('Removing duplicates')
        self._nodes = filter(lambda node: node[0] not in duplicates, self._nodes)
        l.debug('%d duplicates removed', len(duplicates))

    def output_osm(self, f):
        f.write('<?xml version="1.0"?>\n<osm version="0.6" upload="false" generator="addressmerge">\n')
        for node in self._nodes:
            self._serialize_node(f, node)
        f.write('</osm>')

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Conflate an address file with existing OSM data')

    # Basic options
    verbosity = parser.add_mutually_exclusive_group()
    verbosity.add_argument("-v", "--verbose", action="store_true")
    verbosity.add_argument("-q", "--quiet", action="store_true")

    # Database options
    parser.add_argument('-d', '--dbname', default='osm', help='Database to connect to. Defaults to osm.')
    parser.add_argument('-U', '--username', default='osm', help='Username for database. Defaults to osm.')
    parser.add_argument('--host', default='localhost', help='Hostname for database. Defaults to localhost.')
    parser.add_argument('-p', '--port', default=5432, type=int, help='Port for database. Defaults to 5432.')
    parser.add_argument('-P', '--password', default='osm',  help='Password for database. Defaults to osm.')

    # .osm parser options
    parser.add_argument('--threads', default=None, type=int, help='Threads to use when parsing the input OSM file')
    parser.add_argument('input', help='Input OSM file')
    parser.add_argument('output', type=argparse.FileType('w'), help='Output OSM file')

    args = parser.parse_args()

    existing = OSMSource( database=args.dbname, user=args.username,
                          password=args.password, host=args.host,
                          port=str(args.port))

    source = ImportDocument()

    source.parse_osm(args.input)
    source.remove_existing(existing)
    source.output_osm(args.output)
