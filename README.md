# addressmerge #

A tool for merging address data with OSM data

## Installation ##

addressmerge requires psycopg2 and imposm.parser. It also requires access to a postgresql 9.1 or later database with OSM data for the area imported with osmosis using a pgsnapshot schema. This database will normally be on the same machine as addressmerge although it does not have to be.

To install the minimal requirements for addressmerge and imposm on Ubuntu, using virtualenv

```
sudo apt-get install build-essential python-dev protobuf-compiler \
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

To import the data with osmosis from ```dump.osm.pbf``` into the database ```osm``` with postgis and hstore already set up on the database use the commands

```
psql -d osm -f <path-to-osmosis>/script/pgsnapshot_schema_0.6.sql
osmosis --read-pbf dump.osm.pbf --write-pgsql host=localhost database=osm user=osm
```

## Usage ##

The syntax for addressmerge is

```
./addressmerge.py [-h] [DATABASE OPTIONS] input.osm output.osm -w bounds.wkt [--osc output.osc [OSC OPTIONS]]
```
For a full listing of options see ```./addressmerge.py -h```

Database options are

```
  Options that effect the database connection

  -d DBNAME, --dbname DBNAME
                        Database to connect to. Defaults to osm.
  -U USERNAME, --username USERNAME
                        Username for database. Defaults to osm.
  --host HOST           Hostname for database. Defaults to localhost.
  -p PORT, --port PORT  Port for database. Defaults to 5432.
  -P PASSWORD, --password PASSWORD
                        Password for database. Defaults to osm.
```

addressmerge will take the address data in ```input.osm```, connect to the specified pgsnapshot database, filter out any exact address matches and output the new set of addresses to ```output.osm```. It can also produce various changes to the existing OSM data, filtering more addresses from ```output.osm```

## OSC (diff) generation ##

addressmerge has the ability to generate an osmChange (.osc) file based on a series of filters. Filters are run in two stages. The first stage is for filters that match against existing addresses but are not an exact match. The second stage adds address information to features that did not have it before.

All distances are in meters.

### buffer ###

```--buffer``` specifies a distance around each address for matching to features where there are small misalignments between the features and the address nodes. It essentially turns each address point into a small circle for matching. Used by ```--building```

### nocity ###

```--nocity``` will find addresses within the specified distance of the import node with the same housenumber and street but missing the city and will add the city. Care should be used in regions with nearby addresses which are duplicates except for the city. Such addresses are also problematic for E911

### building ###

```--building N``` will attempt to match up addresses to buildings. It will not match to buildings with multiple addr nodes in the import or existing data within N meters of the building or to buildings where there is another building within N meters of the matched address.
