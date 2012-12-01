#!/usr/bin/env python
# -*- coding: utf-8 -*-

import psycopg2
import argparse
import logging as l
l.basicConfig(level=l.DEBUG, format="%(message)s")

parser = argparse.ArgumentParser(description='Conflate an address file with existing OSM data')

# Database options
parser.add_argument('-d', '--dbname', default='osm', help='Database to connect to. Defaults to osm.')
parser.add_argument('-U', '--username', default='osm', help='Username for database. Defaults to osm.')
parser.add_argument('--host', default='localhost', help='Hostname for database. Defaults to localhost.')
parser.add_argument('-p', '--port', default=5432, type=int, help='Port for database. Defaults to 5432.')
parser.add_argument('-P', '--password', default='osm',  help='Password for database. Defaults to osm.')

args = parser.parse_args()
