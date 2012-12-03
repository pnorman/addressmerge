#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Basic modules
import argparse
import logging as l
l.basicConfig(level=l.DEBUG)

# Database modules
import psycopg2

# .osm modules
from imposm.parser import OSMParser

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
parser.add_argument('--threads', default=1, type=int,  help='Threads to use when parsing the input OSM file')
parser.add_argument('input', help='Input OSM file')

args = parser.parse_args()

class OSMSource(object):
    def __init__(self, database, user, password, host, port)
        l.debug('Connecting to postgresql')
        self._conn=psycopg2.connect(database=database, user=username, password=password, host=host, port=str(port))
        self._conn.set_session(readonly=True, autocommit=True)
        self._curs = self._conn.cursor()

class ImportDocument(object):
    newNodes = []
    def __init__(self)
        pass

    def tag_filter(self, tags):
        for key in tags.keys():
            if key not in ('addr:housenumber', 'addr:street', 'addr:city'):
                del tags[key]

    def nodes(self, nodes):
        '''
        Callback method for nodes
        '''
        for osmid, tags, refs in nodes:
            if ('addr:housenumber' in tags) and ('addr:street' in tags):
                pass

class Node(object):
    pass

existing = OSMSource(database=args.dbname, user=args.username, password=args.password, host=args.host, port=str(args.port))
