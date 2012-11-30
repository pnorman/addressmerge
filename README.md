# addressmerge #

A tool for merging address data with OSM data

## Installation ##

addressmerge requires psycopg2. It also requires access to a postgresql database with OSM data for the area imported with imposm. This database will normally be on the same machine as addressmerge although it does not have to be.

To install the minimal requirements on Ubuntu

```
sudo apt-get install python-psycopg2
```

To install everything needed to run addressmerge on one machine

```
sudo apt-get install build-essential python-dev  \
                     protobuf-compiler libprotobuf-dev libtokyocabinet-dev
                     python-psycopg2 python-pip python-virtualenv \
                     postgresql-9.1 postgresql-contrib-9.1 postgis \
virtualenv venv
venv/bin/pip install imposm
```
