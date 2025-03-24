import subprocess
import datetime
import os
import sys

import config, pg_engine

# name of borders inupt table
border_table = config.borders_table

# path of import folder
input_path = config.BORDERS_INPUT_PATH

def import_borders():    

    # list of geojsons to import 
    jsons = [f for f in os.listdir(input_path) if f.endswith(".json") or f.endswith(".geojson")] 
    if len(jsons) == 0:
        print(f'No *.json or *.geojson file found! There should be one geojson as input in {input_path}!')
        sys.exit()  
    elif len(jsons) > 1:    
        print(f'More than one json file found! There should be only one geojson as input in {input_path}!')
        sys.exit()     

    json_name =  jsons[0]

    print(f"Start importing {json_name} ...")  

    file_path =  input_path + '/' + json_name
    
    # delete old border table
    pg_engine.run_sql(f"""drop table if exists {border_table}""")
    
    # import border geojson
    cmd = config.OGR2OGR_PATH + ' -f "PostgreSQL" PG:"dbname=%(db)s user=%(user)s password=%(pw)s" ' % config.CLUSTER_POSTGRES + f'-t_srs EPSG:4326 {file_path} -nln {border_table} -nlt GEOMETRY -lco GEOMETRY_NAME=geom -makevalid' 
    subprocess.call(cmd, shell=True, stdout=open(os.devnull, 'wb'))

    # create spatial index
    pg_engine.run_sql(f"""CREATE INDEX idx_{border_table}_geom_gist ON {border_table} USING gist (geom); """)

    # rename id
    pg_engine.run_sql(f"""ALTER TABLE {border_table} RENAME ogc_fid TO critical_id; """)

    print(f"Finished importing {json_name}")

