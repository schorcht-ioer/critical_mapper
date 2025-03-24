#!/usr/bin/env python3
# encoding: utf-8
#

import os

## NOTE: set number of parallel processes
NUM_PROC = 8
    
## NOTE: change connection settings
CLUSTER_POSTGRES = {
    'user': os.environ['POSTGRES_USER'],
    'pw': os.environ['POSTGRES_PASSWORD'],
    'db': os.environ['POSTGRES_DB'],
    'host': 'localhost',
    'port': '5432',
}
CLUSTER_DATABASE_URI = 'postgresql://%(user)s:%(pw)s@%(host)s:%(port)s/%(db)s' % CLUSTER_POSTGRES

## NOTE: set paths for client tools
raster2pgsql_PATH =  r'raster2pgsql'
shp2pgsql_PATH =  r'shp2pgsql'
psql_PATH = r'psql'
pypath = r'python3.9'

## NOTE: set gdal paths (needed for reprojections and raster import and export)
OGR2OGR_PATH = r'ogr2ogr'
GDALWARP_PATH =  r'gdalwarp'
GDALADDO_PATH =  r'gdaladdo'
GDALINFO_PATH =  r'gdalinfo'
GDALTRANSLATE_PATH = r'gdal_translate'

# NOTE: set proj lib
proj_lib = r"/usr/share/proj"

## NOTE: set config pathes (for postgres configuration)
# set path to postgres data folder
PG_DATA_PATH = r'/var/lib/postgresql/data' 
# set path to postgres postgresql.conf
PG_CONF_PATH = PG_DATA_PATH + r'/postgresql.conf' 
# set path to postgres pg_ctl for restarting 
# (for windows set empty str, like: PG_CTL_PATH = '')
PG_CTL_PATH = r'/usr/lib/postgresql/15/bin/pg_ctl'

## NOTE: set paths to temp, input and output files
# set paths of input tiff rasters
RASTER_INPUT_PATH = r'/cluster/input/raster' 
# set paths of temp tiff rasters (for projected and tiled tiffs)
RASTER_INPUT_TEMP_PATH = r'/cluster/input/temp' 
# set path of output raster
RASTER_OUTPUT_PATH = r'/cluster/output/raster' 
# set paths of input geojson
JSON_INPUT_PATH = r'/cluster/input/geojson' 
# set path of output geojson
JSON_OUTPUT_PATH = r'/cluster/output/geojson' 
# set paths of input borders
BORDERS_INPUT_PATH = r'/cluster/input/borders' 
# set path of output borders
BORDERS_OUTPUT_PATH = r'/cluster/output/borders' 


## NOTE: set name of permanent tables
# this does not really need to be adapted on a native environment

# name of input table
input_table = 'cluster_inputs' 
# name of extent table
extent_table = 'cluster_extents'
# results table
results_table = 'cluster_results' 
# info about type of imported files ('tiff' or 'geojson')
input_type = 'input_type'  
# name of borders table
borders_table = 'borders_inputs'
# name of borders results table
crit_dist_table = 'critical_distances_results'




