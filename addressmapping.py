# This file is based on geocodemapping.py of imposm.geocoder.
#
# geocodemapping.py Copyright 2012 Omniscale (http://omniscale.com)
#
# addressmapping.py Copyright 2012 Paul Norman
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from imposm.mapping import (
    Options,
    Points, LineStrings, Polygons,
    String, Bool, Integer, OneOfInt,
    set_default_name_type, LocalizedName,
    WayZOrder, ZOrder, Direction,
    GeneralizedTable, UnionView,
    PseudoArea, meter_to_mapunit, sqr_meter_to_mapunit,
)

db_conf = Options(
    # db='osm',
    host='localhost',
    port=5432,
    user='osm',
    password='osm',
    sslmode='allow',
    prefix='osm_new_',
    proj='epsg:4326',
)

point_addresses = Points(
    name = 'point_addresses',
    with_type_field = False,
    fields = (
        ('addr:housenumber', String()),
        ('addr:street', String()),
        ('addr:city', String()),
        ('addr:postcode', String()),
        ('addr:country', String()),
    ),
    mapping = {
        'addr:housenumber': (
            '__any__',
        ),
    }
)

polygon_addresses = Polygons(
    name = 'polygon_addresses',
    with_type_field = False,
    fields = (
        ('addr:housenumber', String()),
        ('addr:street', String()),
        ('addr:city', String()),
        ('addr:postcode', String()),
        ('addr:country', String()),
    ),
    mapping = {
        'addr:housenumber': (
            '__any__',
        ),
    }
)
addresses = UnionView(
    name = 'addresses',
    fields = (
        ('addr:housenumber', String()),
        ('addr:street', String()),
        ('addr:city', String()),
        ('addr:postcode', String()),
        ('addr:country', String()),
    ),
    mappings = [point_addresses, polygon_addresses],
)
