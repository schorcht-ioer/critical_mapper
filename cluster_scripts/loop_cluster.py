
import os, sys, shutil, datetime

import check_args, config, pg_engine, create_cluster, export_tiffs, export_geojsons

def cluster_looper(distances, print_step = 1, export = False, delete = False):    

    # start time
    start_time = datetime.datetime.now()

    # get type of imported files
    itype = pg_engine.get_sql(f"""select itype from {config.input_type}""")[0][0]

    for d in distances:

        # get clusters
        create_cluster.clusterize(d, print_step)

        # export in case of tiff
        if export and itype == 'tiff':
            export_tiffs.tiff_exporter(d, print_step)

    # export in case of geojson
    if export and itype == 'geojson':
        export_geojsons.geojson_exporter()

    # delete temp data
    if delete:

        # delete temp tiffs
        if os.path.isdir(config.RASTER_INPUT_TEMP_PATH):
            shutil.rmtree(config.RASTER_INPUT_TEMP_PATH) 

        # delete db-data
        for table in [config.input_table, config.extent_table, config.results_table, config.input_type]:
            pg_engine.run_sql(f"""drop table if exists {table};""")


    total_time = (datetime.datetime.now() - start_time).seconds/60
    print(f"##############################################")
    print(f"  Finished complete clustering of many distances :")
    print(f"    {str(round(total_time,2))} minutes")
    print(f"##############################################")

if __name__ == '__main__':
    
    # get tilesize from args
    distances, distances_range, print_step, export, delete = check_args.get_loop_cluster_args(sys.argv[1:])

    if distances_range:
        distances = distances_range

    print("start clustering for distances: " + str(distances))    

    # run raster tiler
    cluster_looper(distances, print_step, export, delete)
