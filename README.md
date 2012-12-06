# addressmerge #

A tool for merging address data with OSM data

## Installation ##

addressmerge requires psycopg2 and imposm.parser. It also requires access to a postgresql database with OSM data for the area imported with imposm. This database will normally be on the same machine as addressmerge although it does not have to be.

To install the minimal requirements for addressmerge and imposm on Ubuntu, using virtualenv

```
sudo apt-get install build-essential python-devel protobuf-compiler \
					 libprotobuf-dev python-psycopg2 python-pip \
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

Installation of postgresql+postgis is beyond the scope of this readme.

## Importing data ##
To import the data with imposm from ```dump.osm.pbf``` into the database ```osm``` use the command

```
imposm -d osm -m addressmapping.py --overwrite-cache --read dump.osm.pbf --write --deploy-production-tables --remove-backup-tables
psql -d osm -f indexes.sql
```
