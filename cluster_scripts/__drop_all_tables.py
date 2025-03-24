import datetime
import sys

import check_args, config, pg_engine



# PERMANENT TABLE
extent_table = config.extent_table

## TEMP TABLES
# name of input tile cluster table 
tile_cluster_table = 'tile_cluster_'
# name of intersection cluster table with gcid
intersection_cluster_gcid_table = 'intersected_gcid_'
# name of non intersected isolated clusters
isolated_cluster_table = 'isolated_cluster_'
# name of non intersected isolated clusters with gcid
isolated_cluster_gcid_table = 'isolated_cluster_gcid_'
# name of results table for raster
results_table = 'cluster_raster_results_'

    
def drop_all_tables(tile_ids,d):

    # singular tables
    pg_engine.run_sql(f"""   
        DROP TABLE IF EXISTS tmp_tile_intersections{str(d)};
        DROP TABLE IF EXISTS global_clusters_ids_d{str(d)};
        DROP TABLE IF EXISTS global_clusters_temp_d{str(d)};
        DROP TABLE IF EXISTS tmp_global_ids_unnest;
        DROP TABLE IF EXISTS tmp_cluster_gcid;
        """)     
    
    # many tables
    for tile_id in tile_ids:
        temp_buffer_table = 'tmp_buffer_' + tile_id
        center_cluster_table = tile_cluster_table + tile_id
        unnest_tile = 'tmp_unnest_tile_' + tile_id
        tmp_cluster_gcid = 'tmp_tile_cluster_gcid_' + tile_id
        intersection_cluster_gcid_table_tile = intersection_cluster_gcid_table + tile_id
        isolated_cluster_gcid_table_tile = isolated_cluster_gcid_table + tile_id
        results_table_tile = results_table + tile_id
        isolated_cluster_table_tile = isolated_cluster_table + tile_id
        
        pg_engine.run_sql(f"""
            DROP TABLE IF EXISTS {temp_buffer_table};
            DROP TABLE IF EXISTS {center_cluster_table};
            DROP TABLE IF EXISTS {unnest_tile};  
            DROP TABLE IF EXISTS {tmp_cluster_gcid}; 
            DROP TABLE IF EXISTS {intersection_cluster_gcid_table_tile};
            DROP TABLE IF EXISTS {isolated_cluster_table_tile};
            DROP TABLE IF EXISTS {isolated_cluster_gcid_table_tile};
            DROP TABLE IF EXISTS {results_table_tile};
            """)  

    print(f'all tables of the distance of {d}m were dropped')     

    

if __name__ == '__main__':

    # check sys args
    d = check_args.get_dropper_args(sys.argv[1:]) 
   
    # start time
    start_time = datetime.datetime.now()
    
    # list of extent tiles for iterating
    extents = pg_engine.get_sql(f"""select tile_id from {extent_table} ;""")

    # get list only of tile names
    tile_ids =  [str(extent[0]) for extent in extents]
   
    ## create partitions and tables
    drop_all_tables(tile_ids,d)
    
    # total run time
    total_time = (datetime.datetime.now() - start_time).seconds/60
    print(f"Finished dropping tables in {str(round(total_time,2))} minutes") 