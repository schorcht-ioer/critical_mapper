import subprocess
import os


import config

input_path = config.BORDERS_INPUT_PATH
output_path = config.BORDERS_OUTPUT_PATH
border_results = config.crit_dist_table

def export_borders():
    try:
        # get file name of borders
        json_name = [f for f in os.listdir(input_path) if f.endswith(".json") or f.endswith(".geojson")][0] 

        file_path =  output_path + '/' + json_name

        # delete old file
        if os.path.exists(file_path):
            os.remove(file_path)

        # create folder if not exists
        os.makedirs(output_path, exist_ok=True)    

        # export geojson
        cmd = config.OGR2OGR_PATH + f' -f "GeoJSON" {file_path} ' + 'PG:"dbname=%(db)s user=%(user)s password=%(pw)s" ' % config.CLUSTER_POSTGRES + f' -nln {border_results} -sql "select * from {border_results}"'
        subprocess.call(cmd, shell=True, stdout=open(os.devnull, 'wb'))

        # list of geojsons for showing progeress
        print(f"Finished exporting {json_name}")
        
    except Exception as e:
        print(e)   