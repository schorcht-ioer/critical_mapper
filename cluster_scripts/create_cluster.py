from multiprocessing import Pool
import datetime
import time
import os
import sys
from osgeo import gdal
from osgeo import ogr
from osgeo import osr

import check_args, config, pg_engine, parallel_runner as pr

# print every N step
global print_step
print_step =  1

######################## BEGIN TABLE NAMES ######################################

### PERMANENT TABLES
# name of inupt table
input_table = config.input_table
# name of extent table
extent_table = config.extent_table
# results table
resutls_table = config.results_table

### TEMP TABLES
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

######################## END TABLE NAMES ######################################

class ClusterTile:
    def __init__(self, tile_name, tile_names, tile_id, tile_type, distance):
        self.name = tile_name
        self.names = tile_names
        self.id = tile_id
        self.type = tile_type
        self.d = distance


################################
##       CLUSTER TILES        ##
################################

def cluster_tiles(tile):
    
    # set table names
    temp_buffer_table = 'tmp_buffer_' + str(tile.id)
    cluster_table_tile = tile_cluster_table + str(tile.id)
    
    # checker for succes
    global succeed
    succeed = False
    
    def cluster_tile():
        pg_engine.run_sql(f"""
            -- buffer polygons
            DROP TABLE IF EXISTS {temp_buffer_table};
            create TEMP table {temp_buffer_table} as (
                SELECT poly_id, (ST_Buffer(geom_simpel::geography, {str(tile.d/2)})::geometry ) as geom, tile_id 
                FROM {input_table}
                where tile_id = {tile.id});
            ALTER TABLE {temp_buffer_table} SET (autovacuum_enabled = false);     
            CREATE INDEX idx_{temp_buffer_table}_gist ON {temp_buffer_table} USING gist (geom);
                        
            -- create tile clusters
            DROP TABLE IF EXISTS {cluster_table_tile};          
            create UNLOGGED table {cluster_table_tile} as (
                select 'tileid_{str(tile.id)}_cid_' || ST_ClusterIntersectingWin(geom) OVER () AS tile_cluster_id, poly_id, geom, tile_id from {temp_buffer_table});
            ALTER TABLE {cluster_table_tile} SET (autovacuum_enabled = false);     
                
            -- create indices
            CREATE INDEX idx_tc_{cluster_table_tile}_geom ON {cluster_table_tile} USING gist (geom);
            CREATE INDEX idx_tc_{cluster_table_tile}_tcid on {cluster_table_tile}(tile_cluster_id);
            drop table {temp_buffer_table};            
            """)  

        # validate cluster size
        cluster_too_big = pg_engine.get_sql(f"""
            -- cluster extent
            with cluster_extent as (
                select ST_SetSRID(ST_Extent(geom),4326) as geom from {cluster_table_tile}
            ),
            -- tile extent, slightly larger
            tile_extent as (
                select ST_Buffer(geom, 0.001) as geom from {extent_table} where tile_id = {tile.id}

            ),
            -- non-adjacent extents
            non_adjacent_extents as (
                select a.geom from {extent_table} a, tile_extent b 
                where ST_Intersects(a.geom,b.geom) = FALSE

            )
            -- check if cluster extent intersects non-adjacent extents
            select ST_Intersects(a.geom,b.geom) from cluster_extent a, non_adjacent_extents b;

            """)

        if cluster_too_big != [] and cluster_too_big[0][0] :
            print(f'WARNING at tile_id {tile.id}: The by distance buffered Polygons are intersecting non-adjacent tiles!')    
            print('Hint: Increase the tile size parameter (-g) on import or decrease the cluster distance.')  
            print('Hint: Also split Polygons into smaller parts could solve the error.')
            print('Hint: Another reason could be that buffered polygons cross the dateline! Increase the distance to dateline parameter (-l) on import.')


        global succeed    
        succeed = True       
    
    try:
        cluster_tile()
    
    # strange "invalid page in block" error occurs frequently (caused by faulty amd raid) 
    # --> try redo in this case :\
    # also randomly getting in recovery mode :\ 
    # --> wait in this case
    
    # meanwhile the amd raid has been disabled and the errors no longer occurs :)
    

    except:

        check_recovery()

        print(f"redo {tile.name}")
        try:
            cluster_tile()
            print(f"redo {tile.name} was successful")
        except Exception as error:
            print(f"ERROR ON {tile.name}", error)
            print(f"ERROR ON {tile.name}")                 
        
    if succeed == False:
        print(f"ERROR ON {tile.name}")        
        
    # list of extents for showing progeress    
    total_count = len(tile.names)
    this_count = tile.names.index(tile.name) + 1
    if (this_count % print_step == 0):
        print(f"Finished tile clustering [{str(this_count)}/{str(total_count)}]")


################################
##       COMBINE CLUSTER      ##
################################        

def intersect_tiles_cluster(tile):
    
    # checker for succes
    global succeed
    succeed = False
    
    # center cluster table
    center_cluster_table = tile_cluster_table + str(tile.id)
    def intersect_clusters():
        # get neighbours of center
        neighbours = pg_engine.get_sql(f"""
            with center_extent as (
                select ST_Translate(geom, 0.001, -0.001) as geom, tile_id, 'center' as c from {extent_table} where tile_id = {tile.id}
            )
            select b.tile_id from center_extent a, {extent_table} b where ST_Intersects(a.geom,b.geom) and b.tile_id != {tile.id}
            """)
            
        # intersect center with neighbours
        for n in neighbours:
            neighbour_cluster_table = tile_cluster_table + str(n[0]) 
            pg_engine.run_sql(f""" 
                INSERT INTO tmp_tile_intersections{str(tile.d)} (ids)
                select array[a.tile_cluster_id,b.tile_cluster_id] as ids
                from {center_cluster_table} a, {neighbour_cluster_table} b
                where ST_Intersects(a.geom,b.geom)
                group by a.tile_cluster_id,b.tile_cluster_id;
            """)
        global succeed    
        succeed = True   
    try:
        intersect_clusters()
    except:
        check_recovery()
        print(f"redo {tile.name}")
        intersect_clusters()
        print(f"redo {tile.name} was successful")
        
    if succeed == False:
        print(f"ERROR ON {tile.name}")       
        
    # list of extents for showing progeress
    total_count = len(tile.names)
    this_count = tile.names.index(tile.name) + 1
    if (this_count % print_step == 0):
        print(f"Finished cluster intersection [{str(this_count)}/{str(total_count)}]")
        
def combine_intersection_clusters(d):

    check_recovery()

    # combine intersecting tile clusters
    pg_engine.run_sql(f""" 
        INSERT INTO global_clusters_temp_d{str(d)} (ids)
        select ids
        from tmp_tile_intersections{str(d)};
    """)
    
    # unnest global intersection cluster ids
    pg_engine.run_sql(f""" 
        DROP TABLE IF EXISTS tmp_global_ids_unnest;
        create UNLOGGED table tmp_global_ids_unnest as (
            with unnest_ids as(
                select global_cluster_id, unnest(ids) as id                    
                from global_clusters_ids_d{str(d)}
            )
            select global_cluster_id,id,
                split_part(id, '_', 2)::int as tile_id
            from unnest_ids
        );        
        CREATE INDEX idx_global_unnest_tile_id_btree on tmp_global_ids_unnest(tile_id);
        drop table tmp_tile_intersections{str(d)};""")
        
     
    
def join_global_id_of_intersection(tile):
    
    # checker for succes
    global succeed
    succeed = False

    # center cluster table
    center_cluster_table = tile_cluster_table + str(tile.id)
    
    # select unnnest as tile subselection
    unnest_tile = 'tmp_unnest_tile_' + str(tile.id)
    
    def join_global():
        # join gcid to cluster buffers
        intersection_cluster_gcid_table_tile = intersection_cluster_gcid_table + str(tile.id)
        pg_engine.run_sql(f"""
                drop table if exists  {intersection_cluster_gcid_table_tile};
                create UNLOGGED table {intersection_cluster_gcid_table_tile} as (
                    with {unnest_tile} as (
                        select id, global_cluster_id
                        from tmp_global_ids_unnest
                        where tile_id = {tile.id} 
                    )
                    select tile_cluster_id, global_cluster_id, poly_id, geom, tile_id             
                    from {center_cluster_table} a
                    left join {unnest_tile} b
                    on tile_cluster_id = id);
                    ALTER TABLE {intersection_cluster_gcid_table_tile} SET (autovacuum_enabled = false);   
            """)
            
        # create index
        pg_engine.run_sql(f"""
            CREATE INDEX idx_{intersection_cluster_gcid_table_tile}_tcid ON {intersection_cluster_gcid_table_tile}(tile_cluster_id, tile_id);
            CREATE INDEX idx_{intersection_cluster_gcid_table_tile}_gcid ON {intersection_cluster_gcid_table_tile}(global_cluster_id);
            """)

        
        # get isolated clusters
        isolated_cluster_table_tile = isolated_cluster_table + str(tile.id)  
        pg_engine.run_sql(f"""
            drop table if exists {isolated_cluster_table_tile};
            create UNLOGGED table {isolated_cluster_table_tile} as (  
                select tile_cluster_id, tile_id 
                from {intersection_cluster_gcid_table_tile}            
                where global_cluster_id  IS NULL
                group by tile_cluster_id, tile_id);
            ALTER TABLE {isolated_cluster_table_tile} SET (autovacuum_enabled = false);      
            DROP INDEX idx_{intersection_cluster_gcid_table_tile}_tcid;    
            CREATE INDEX idx_{intersection_cluster_gcid_table_tile}_tcid ON {intersection_cluster_gcid_table_tile}(tile_cluster_id);    
            """) 
        global succeed    
        succeed = True   
    try:
        join_global()
    except:
        check_recovery()
        print(f"redo {tile.name}")
        join_global()
        print(f"redo {tile.name} was successful")
        
    # drop tables
    pg_engine.run_sql(f"""
        DROP TABLE IF EXISTS {center_cluster_table};   
        --DROP TABLE IF EXISTS {unnest_tile}; 
        """)    
        
    if succeed == False:
        print(f"ERROR ON {tile.name}")       
         
      
    # list of extents for showing progeress
    total_count = len(tile.names)
    this_count = tile.names.index(tile.name) + 1
    if (this_count % print_step == 0):
        print(f"Finished join gcid of intersections [{str(this_count)}/{str(total_count)}]")
    
def fill_missing_global_id(d, tile_ids, max_gcid):  

    check_recovery()

    if max_gcid is None:
        max_gcid = 0
    
    gcid_counter = 0
    this_count = 0 
    total_count = len(tile_ids)
    for tile_id in tile_ids:        
        isolated_cluster_table_tile = isolated_cluster_table + str(tile_id) 
        isolated_cluster_gcid_table_tile = isolated_cluster_gcid_table + str(tile_id)   
        # fill missing gcid
        pg_engine.run_sql(f"""
            drop table if exists {isolated_cluster_gcid_table_tile};
            create UNLOGGED table {isolated_cluster_gcid_table_tile} as (      
                select tile_cluster_id, ROW_NUMBER() OVER() + {max_gcid} + {gcid_counter} as global_cluster_id, tile_id
                from {isolated_cluster_table_tile});
        """)
        
        gcid_counter += int(pg_engine.get_sql(f"""select count(*) from {isolated_cluster_table_tile}""")[0][0]) 
        
        # drop temp table
        pg_engine.run_sql(f"""            
            drop table {isolated_cluster_table_tile};
            """)

        this_count += 1        
        if (this_count % print_step == 0):
            print(f"Finished fill missing ids [{str(this_count)}/{str(total_count)}]")
            
def join_complete_global_id(tile):

    # checker for succes
    global succeed
    succeed = False    
    
    intersection_cluster_gcid_table_tile = intersection_cluster_gcid_table + str(tile.id)
    isolated_cluster_gcid_table_tile = isolated_cluster_gcid_table + str(tile.id)
    
    def join_complete():
        pg_engine.run_sql(f"""
            CREATE INDEX IF NOT EXISTS idx_{isolated_cluster_gcid_table_tile}_tcid ON {isolated_cluster_gcid_table_tile}(tile_cluster_id);

            insert into tmp_cluster_gcid (poly_id,global_cluster_id) 
                select poly_id, 
                    CASE
                        WHEN b.global_cluster_id IS NULL THEN a.global_cluster_id
                        ELSE b.global_cluster_id
                        END global_cluster_id
                from {intersection_cluster_gcid_table_tile} a left join {isolated_cluster_gcid_table_tile} b
                on a.tile_cluster_id = b.tile_cluster_id;
            """)
    
        global succeed    
        succeed = True   
    try:
        join_complete()
    except:
        check_recovery()
        print(f"redo {tile.name}")
        join_complete()
        print(f"redo {tile.name} was successful")
        
    # drop tables
    pg_engine.run_sql(f""" 
        DROP TABLE IF EXISTS {intersection_cluster_gcid_table_tile};
        DROP TABLE IF EXISTS {isolated_cluster_gcid_table_tile};  
        """)      
        
    if succeed == False:
        print(f"ERROR ON {tile.name}")              
        
    # list of extents for showing progeress    
    total_count = len(tile.names)
    this_count = tile.names.index(tile.name) + 1
    if (this_count % print_step == 0):
        print(f"Finished join complete global id [{str(this_count)}/{str(total_count)}]") 

def update_results(d):

    check_recovery()

    # CHECK COUNTS of final table (result_count) and new cluster table (new_count)
    result_count = int(pg_engine.get_sql(f"""select count(*) from {resutls_table}""")[0][0])
    new_count = int(pg_engine.get_sql(f"""select count(*) from tmp_cluster_gcid""")[0][0])

    if result_count != new_count:
        print(f"!!!!!!!!!!!!!!!!!!!!!! ERROR ON COUNT VALIDATION ON RANGE {str(d)} !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        # drop temp results
        pg_engine.run_sql(f"""            
            drop table if exists tmp_cluster_gcid;""")  
    else:
        # new gcid column
        new_gcid = f"gcid_d{str(d)}"

        # drop gcid column if exists
        pg_engine.run_sql(f""" ALTER TABLE {resutls_table} DROP COLUMN IF EXISTS {new_gcid};""") 

        # drop new results
        pg_engine.run_sql(f"""DROP TABLE IF EXISTS results_new;""")

        # get column names of result table
        cols = pg_engine.get_sql(f"""SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
            AND table_name   = '{resutls_table}'
            ;""")

        col_names = ''.join(c for c in str(cols) if c not in '\'()[],').replace(' ',',')  

        # create new results table
        pg_engine.run_sql(f""" 
            create table results_new as (
                select a.{col_names}, global_cluster_id as {new_gcid} 
                from {resutls_table} a inner join tmp_cluster_gcid b 
                on a.poly_id = b.poly_id 
                );    
            """)

        # validate counts again
        join_count = int(pg_engine.get_sql(f"""select count(*) from results_new""")[0][0])

        if join_count != result_count:
            print(f"!!!!!!!!!!!!!!!!!!!!!! ERROR ON COUNT VALIDATION AFTER JOINING ON RANGE {str(d)} !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
            # drop temp results
            pg_engine.run_sql(f"""            
                DROP TABLE IF EXISTS results_new;
                drop table if exists tmp_cluster_gcid;""")  
                
        else:
            # drop old results
            pg_engine.run_sql(f"""
                DROP TABLE IF EXISTS {resutls_table};
                drop table if exists tmp_cluster_gcid;""")  

            # rename new table
            pg_engine.run_sql(f"""ALTER TABLE results_new RENAME TO {resutls_table};""")

            print(f"update final table was successful at distance {str(d)}")


################################
##       PREPROCESSING        ##
################################
    
def preprocessing(d):

    print(f"Starting clustering with distance of {d} meters")

    # drop old tables
    pg_engine.run_sql(f""" 
        drop table if exists tmp_tile_intersections{str(d)};
        drop table if exists global_clusters_ids_d{str(d)};
        drop table if exists global_clusters_temp_d{str(d)};
        drop table if exists tmp_cluster_gcid;
        """)
        
    # create table for global clusters
    pg_engine.run_sql(f"""
        CREATE UNLOGGED TABLE tmp_tile_intersections{str(d)}(ids text [] );
        CREATE UNLOGGED TABLE global_clusters_ids_d{str(d)}(global_cluster_id serial, ids text [] );
        CREATE UNLOGGED TABLE global_clusters_temp_d{str(d)}(global_cluster_id serial, ids text [] );""")
    
    # create combine table function
    pg_engine.run_sql(f"""
        CREATE OR REPLACE FUNCTION  on_insert_cluster() RETURNS TRIGGER AS $f$
            DECLARE sum_array text[];
            BEGIN
                SELECT ARRAY (SELECT UNNEST(ids) FROM global_clusters_ids_d{str(d)} gc WHERE gc.ids && NEW.ids) INTO sum_array;
                sum_array := sum_array || NEW.ids;
                SELECT ARRAY(SELECT DISTINCT UNNEST(sum_array)) INTO sum_array;
                DELETE FROM global_clusters_ids_d{str(d)} gc WHERE gc.ids && sum_array;
                INSERT INTO global_clusters_ids_d{str(d)}(ids) SELECT sum_array;
                --DELETE FROM global_clusters_temp_d{str(d)} WHERE global_cluster_id = NEW.global_cluster_id;
                RETURN OLD;
            END
            $f$ LANGUAGE plpgsql;""")
                
    # add trigger
    pg_engine.run_sql(f"""CREATE TRIGGER on_insert_cluster AFTER INSERT ON global_clusters_temp_d{str(d)} FOR EACH ROW EXECUTE PROCEDURE on_insert_cluster();""")
    
    # create indices
    pg_engine.run_sql(f"""
        CREATE INDEX idx_global_clusters_ids_d{str(d)} on global_clusters_ids_d{str(d)} USING GIN (ids);
        --CREATE INDEX idx_global_clusters_temp_d{str(d)} on global_clusters_temp_d{str(d)} USING GIN (ids);""")    

    # create temp cluster gcid table  
    pg_engine.run_sql(f"""
        CREATE UNLOGGED TABLE tmp_cluster_gcid (poly_id int, global_cluster_id int);""")

def print_time(start_time, end_time, process):
    total_seconds = (end_time - start_time).total_seconds()
    total_minutes = total_seconds / 60
    total_hours = total_minutes / 60
    print_str =''
    print_time =''
    
    # print as hours
    if (total_hours >= 1): 
        hours = int(total_hours)
        minutes = int(total_minutes) % 60
        print_str  = f"  {process} finished in:"
        print_time = f"   {hours} h, {minutes} min"
    
    # print as minutes    
    elif (total_minutes >= 1): 
        minutes = int(total_minutes)
        seconds = int(total_seconds) % 60
        print_str = f"  {process} finished in:"
        print_time = f"    {minutes} min, {seconds} sec"
    
    # print as seconds      
    else:
        seconds = int(total_seconds)
        print_str = f"  {process} finished in:"
        print_time = f"    {seconds} sec"
        
    print("----------------------------------------------") 
    print(print_str) 
    print(print_time) 
    print("----------------------------------------------") 

def check_recovery():

    time.sleep(10)       

    r = None

    try:
        r = pg_engine.get_sql("select pg_is_in_recovery();")[0][0]
    except:        
        print(f"waiting for finishing recovery mode ...")
        time.sleep(600)   
        r = False

    if r == True:
        print(f"waiting for finishing recovery mode ...")
        time.sleep(600)   

def validate_distance(d):

    # get shortest tile edge
    min_width = pg_engine.get_sql(f"""select min(tile_width) from {extent_table} ;""")[0][0] 

    if d >= min_width:
        print(f"Error: The current distance ({str(d)}) is too great as it extends over the shortest edge of a tile!")   
        print(f"Hint: Increase the size of tiles or decrease the distance.")   
        sys.exit()

def clusterize(d, print_step_param = 1):

    # start time
    start_time = datetime.datetime.now()

    #set print_step
    global print_step
    print_step = print_step_param

    # validate if distance is too great
    validate_distance(d)

    # get list of files for iterating
    extents = pg_engine.get_sql(f"""select tile_name, tile_id, tile_type from {extent_table} ;""")

    # get list only of tile names
    tile_names =  [extent[0] for extent in extents]

    # get list only of tile_ids
    tile_ids =  [extent[1] for extent in extents]

    # list of extents with distance parameter
    tiles = [ClusterTile(extent[0], tile_names, extent[1], extent[2], d ) for extent in extents]
    
    ## create funtion for combining tiles and tables
    preprocessing(d)
    
    # preprocessing time
    prep_time = datetime.datetime.now()
    print_time(start_time, prep_time, 'Preprocessing')
    
    ################################
    ##       CLUSTER TILES        ##
    ################################
    
    # run cluster tiles
    pr.run_parallel(cluster_tiles, tiles)
        
    # Clustering time
    clust_time = datetime.datetime.now()
    print_time(prep_time, clust_time, 'Clustering each tile')
        
    ################################
    ##       COMBINE CLUSTER      ##
    ################################
    
    # run intersect tile clusters parallel
    pr.run_parallel(intersect_tiles_cluster, tiles)
        
    # intersect time
    inters_time = datetime.datetime.now()
    print_time(clust_time, inters_time, 'Intersecting cluster tiles')
    
    # combine clusters
    combine_intersection_clusters(d)
    
    # combine time
    comb_time = datetime.datetime.now()
    print_time(inters_time, comb_time, 'Combining intersected clusters')
    
    # run join global intersection clusters parallel
    pr.run_parallel(join_global_id_of_intersection, tiles)
        
    # Join gcid of intersections
    joininters_time = datetime.datetime.now()
    print_time(comb_time, joininters_time, 'Join gcid of intersections') 

    # get max global cluster id    
    max_gcid = pg_engine.get_sql(f"""select max(global_cluster_id) from global_clusters_ids_d{str(d)}""")[0][0] 
    
    fill_missing_global_id(d,tile_ids,max_gcid)  
    
    # Fill missing ids
    fill_time = datetime.datetime.now()
    print_time(joininters_time, fill_time, 'Fill missing ids') 
    
    # run join global complete clusters parallel
    pr.run_parallel(join_complete_global_id, tiles)
        
    # Join all gcids to poly
    joinglobal_time = datetime.datetime.now()
    print_time(fill_time, joinglobal_time, 'Join all gcids to poly') 

    # update results table     
    update_results(d)
    update_time = datetime.datetime.now()
    print_time(joinglobal_time, update_time, 'Update results table') 

    # delete ALL tmp
    pg_engine.run_sql(f"""   
        DROP TABLE IF EXISTS tmp_tile_intersections{str(d)};
        DROP TABLE IF EXISTS global_clusters_ids_d{str(d)};
        DROP TABLE IF EXISTS global_clusters_temp_d{str(d)};
        DROP TABLE IF EXISTS tmp_global_ids_unnest;
        """)        
      

    # total run time
    end_time = datetime.datetime.now()
    print_time(start_time, end_time, f'Total clusterting with distance of {d} meters') 


if __name__ == '__main__':

    # check sys args (distance, print_step)
    d, print_step_param = check_args.get_cluster_args(sys.argv[1:])

    clusterize(d, print_step_param)
