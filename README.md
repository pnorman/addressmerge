# addressmerge #

A tool for merging address data with OSM data

## Installation ##

addressmerge requires psycopg2 and imposm.parser. It also requires access to a postgresql 9.1 or later database with OSM data for the area imported with osmosis using a pgsnapshot schema. This database will normally be on the same machine as addressmerge although it does not have to be.

To install the minimal requirements for addressmerge and imposm on Ubuntu, using virtualenv

```
sudo apt-get install build-essential python-devel protobuf-compiler \
					 libprotobuf-dev python-psycopg2 python-pip
```

You can then install imposm for everyone or in a virtual environment

To install for everyone use ```sudo pip install imposm```

To use a virtual environment
```
sudo apt-get install python-virtualenv
virtualenv ~/venv
~/venv/bin/pip install imposm
```
If running with a virtual environment you need to do ```source venv/bin/activate``` before running commands.

It is possible to install ```imposm.parser``` with [less dependencies](http://dev.omniscale.net/imposm.parser/index.html#document-install) than ```imposm```.

Installation of osmosis and PostgreSQL+PostGIS is beyond the scope of this readme.

## Importing data ##
It is possible to use an existing pgsnapshot database kept up to date with minutely diffs. This is beyond the scope of this readme.

To import the data with imposm from ```dump.osm.pbf``` into the database ```osm``` with postgis and hstore already set up on the database use the commands

```
psql -d osm -f <path-to-osmosis>/script/pgsnapshot_schema_0.6.sql
osmosis --read-pbf dump.osm.pbf --write-pgsql host=localhost database=osm user=osm
```
