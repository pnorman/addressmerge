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

class OSMSource(object):
    def __init__(self, database, user, password, host, port):
        l.debug('Connecting to postgresql')
        self._conn=psycopg2.connect(database=database, user=user, 
                                    password=password, host=host, 
                                    port=str(port))
        self._conn.set_session(readonly=True, autocommit=True)


class ImportDocument(object):
    newNodes = []
    def __init__(self, chunk=1):
        self.chunk = chunk
        self._parser = OSMParser(nodes_callback=self._parse_nodes)
        self._nodes = deque()

    def parse_osm(self, filename):
        l.debug('Parsing %s', filename)
        self._parser.parse(filename)

    def _parse_nodes(self, nodes):
        for node in nodes:
            self._nodes.append(node)

    def remove_existing(self, source):
        ''' This function will remove addresses that already exist in OSM'''
        # Make a local copy that we'll delete from as we process
        self._localnodes = copy.copy(self._nodes)
        while len(self._localnodes) > 0:
            self._process_chunk(source)

    def _process_chunk(self, source):
        ''' Pops a deque of up to self.chunk nodes off and parses them '''
        request_list = deque()
        cur = self.
        try:
            for _i in xrange(0,self.chunk):
                request_list.append(self._localnodes.pop())
        except IndexError:
            pass
        # len(request_list) is now 0 to N
        
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
    parser.add_argument('--chunk', default=13, type=int, help='Number of records requested at once')

    # .osm parser options
    parser.add_argument('--threads', default=1, type=int, help='Threads to use when parsing the input OSM file')
    parser.add_argument('input', help='Input OSM file')
    parser.add_argument('output', help='Output OSM file')

    args = parser.parse_args()

    existing = OSMSource( database=args.dbname, user=args.username,
                          password=args.password, host=args.host,
                          port=str(args.port))

    source = ImportDocument(chunk=args.chunk)

    source.parse_osm(args.input)
    source.remove_existing(existing)
