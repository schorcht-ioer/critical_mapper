from multiprocessing import Pool
import subprocess
import datetime
import os
import sys

import config, pg_engine, parallel_runner as pr

os.environ['PGPASSWORD'] = "%(pw)s" % config.CLUSTER_POSTGRES 

# results table
results_table = config.results_table

# output path
output_path = config.JSON_OUTPUT_PATH

class OutputFile:
    def __init__(self, args):
        self.file_name, self.file_names, self.file_id = args
        self.file_path = output_path + '/' + self.file_name
        # run export
        self.export_geojson()

    def export_geojson(self):
        try:
            # delete old file
            if os.path.exists(self.file_path):
                os.remove(self.file_path)

            # export geojson
            cmd = config.OGR2OGR_PATH + f' -f "GeoJSON" {self.file_path} ' + 'PG:"dbname=%(db)s user=%(user)s password=%(pw)s" ' % config.CLUSTER_POSTGRES + f' -nln cluster_results -sql "select * from {results_table} where file_id = {self.file_id}"'
            subprocess.call(cmd, shell=True, stdout=open(os.devnull, 'wb'))

            # list of geojsons for showing progeress
            total_count = len(self.file_names)
            this_count = self.file_names.index(self.file_name) + 1
            print(f"Finished exporting {self.file_name} [{str(this_count)}/{str(total_count)}]")
            
        except Exception as e:
            print(e)   

def geojson_exporter():

    # start time
    start_time = datetime.datetime.now()

    # get list of geojons
    jsons = os.listdir(config.JSON_INPUT_PATH) 

    # get list of args as tuple   
    output_files = [(json,jsons,jsons.index(json)) for json in jsons]

    # create output folder if not exists
    os.makedirs(output_path, exist_ok=True)    

    # run export parallel
    pr.run_parallel(OutputFile, output_files)

    # total run time
    total_time = (datetime.datetime.now() - start_time).seconds/60        

    print(f"----------------------------------------------")
    print(f"  Complete export of geojsons finished in:")
    print(f"    {str(round(total_time,2))} minutes")
    print(f"----------------------------------------------")
    

# make it also runable as standalone script
if __name__ == '__main__':

    geojson_exporter()