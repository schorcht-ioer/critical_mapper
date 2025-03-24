from multiprocessing import Pool
import subprocess
import datetime
import os
import sys

import config, pg_engine, check_args, parallel_runner as pr

os.environ['PGPASSWORD'] = "%(pw)s" % config.CLUSTER_POSTGRES 

### PERMANENT TABLES
# name of inupt table
input_table = config.input_table
# name of extents table
extent_table = config.extent_table
# results table
results_table = config.results_table 
# file type info
input_type = config.input_type


class InputFile:
    def __init__(self, file_name, file_names, file_id, dateline_distance):
        self.name = file_name
        self.names = file_names
        self.id = file_id
        self.path = config.JSON_INPUT_PATH + '/' + file_name
        self.dateline_distance = dateline_distance + 500

def import_json(file):      

    import_table = 'temp_input_' + str(file.id) 
    
    # delete old temp import
    pg_engine.run_sql(f"""drop table if exists {import_table}""")
    
    # import geojson
    cmd = config.OGR2OGR_PATH + ' -f "PostgreSQL" PG:"dbname=%(db)s user=%(user)s password=%(pw)s" ' % config.CLUSTER_POSTGRES + f'-t_srs EPSG:4326 {file.path} -nln {import_table} -nlt GEOMETRY -lco GEOMETRY_NAME=geom' # -makevalid' 
    subprocess.call(cmd, shell=True, stdout=open(os.devnull, 'wb'))

    # create spatial index
    pg_engine.run_sql(f"""CREATE INDEX idx_{import_table}_geom_gist ON {import_table} USING gist (geom); """)

    # list of geojsons for showing progeress
    total_count = len(file.names)
    this_count = file.names.index(file.name) + 1
    print(f"Finished importing {file.name} [{str(this_count)}/{str(total_count)}]")

def create_results_table(files):

    # create results table
    pg_engine.run_sql(f"""
        drop table if exists {results_table};
        CREATE TABLE {results_table} (             
            poly_id SERIAL,
            file_id INT,
            geom_point GEOMETRY(GEOMETRY,4326),
            like temp_input_0);
    """) 

    # get column names of results table
    cols = pg_engine.get_sql(f"""SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
        AND table_name   = 'temp_input_0'
        ;""")
    col_names = ''.join(c for c in str(cols) if c not in '\'()[],').replace(' ',',') 

    # insert all input data
    for file in files:
        import_table = 'temp_input_' + str(file.id)

        # repair geoms
        pg_engine.run_sql(f""" 
            update {import_table} set geom =
                case
                    when ST_IsValid(geom) = 'f' then ST_CollectionExtract(ST_MakeValid(geom))
                    else geom
                end
            """)

        # insert into results_table and drop objects near dateline
        pg_engine.run_sql(f"""
            insert into {results_table} (file_id, geom_point, {col_names})
            with dateline as (
                -- define narrow polygones at dateline
                select ST_Union(
                    ST_Buffer(ST_Segmentize(ST_LineFromText('LINESTRING(179.999999 -89, 179.999999 89)',4326)::geography,1000000), {file.dateline_distance},'side=left')::geometry,
                    ST_Buffer(ST_Segmentize(ST_LineFromText('LINESTRING(-179.999999 -89, -179.999999 89)',4326)::geography,1000000), {file.dateline_distance},'side=right')::geometry
                    ) as dateline_geom
            )
            select 
                {file.id} as file_id,
                ST_PointOnSurface(geom) as geom_point,
                {col_names}
            from {import_table} a,dateline b
            where ST_Intersects(a.geom, b.dateline_geom) = false;
        """) 

        # delete temp input table
        pg_engine.run_sql(f"""drop table if exists {import_table};""")

     # create index on point and file_id
    pg_engine.run_sql(f"""
        CREATE INDEX idx_{results_table}_point_gist ON {results_table} USING gist (geom_point);
        CREATE INDEX idx_{results_table}_file_id_btree ON {results_table} (file_id);
        """)   

    print(f"Finished create results table")

def create_extents_grid(grid_size):

    # check dimension
    max_width = pg_engine.get_sql(f"""
        with x_lengths as (
            select (st_xmax(geom)-st_xmin(geom)) as x_length from {results_table}
        )
        select max(x_length) from x_lengths;""")[0][0] 
    
    max_height = pg_engine.get_sql(f"""
        with y_lengths as (
            select (st_ymax(geom)-st_ymin(geom)) as y_length from {results_table}
        )
        select max(y_length) from y_lengths;""")[0][0] 
    
    if max_width > grid_size or max_height > grid_size:
        print('WARNING: At least one polygon is bigger than the tile size!')
        print('Hint: Define a tile size that is much larger than the width or height of the largest polygon.')
        print('Hint: Also split Polygons into smaller parts could solve this problem.')
        pg_engine.run_sql(f"""
            DROP TABLE IF EXISTS {input_table};
            DROP TABLE IF EXISTS {results_table};
            DROP TABLE IF EXISTS {extent_table};
            """)
        sys.exit()

    # create grid
    pg_engine.run_sql(f""" 
        with 
        extents as (
            select (ST_SquareGrid({grid_size}, ST_SetSRID(ST_Extent(geom_point),4326) )).*
            from {results_table}

        )
        insert into {extent_table} (tile_type, tile_width, geom)
        select distinct
            -- type of tile 
            'geojson' as tile_type,

            -- get tile_width (shortest Latitude)
            Least (
                -- southern Latitude
                ST_Distance(
                    CONCAT('SRID=4326;POINT(', ST_XMin(a.geom), ' ' , ST_YMin(a.geom),  ')')::geography,
                    CONCAT('SRID=4326;POINT(', ST_XMax(a.geom), ' ' , ST_YMin(a.geom),  ')')::geography
                ),

                -- nothern Latitude
                ST_Distance(
                    CONCAT('SRID=4326;POINT(', ST_XMin(a.geom), ' ' , ST_YMax(a.geom),  ')')::geography,
                    CONCAT('SRID=4326;POINT(', ST_XMax(a.geom), ' ' , ST_YMax(a.geom),  ')')::geography
                )
            ) as tile_width,

            -- get geom from extent
            a.geom
        from extents a, {results_table} b    
        where ST_Intersects(a.geom, b.geom_point); 

     -- set tile_name
     update {extent_table} set tile_name = 'Tile ' || tile_id::text;    
     
    """)

    print(f"Finished create grid of tile extents")

def split_data(file):

    # insert into input table
    pg_engine.run_sql(f""" 
        insert into {input_table} (poly_id, geom_simpel, tile_id)
        select a.poly_id, a.geom as geom_simpel, extent.tile_id
        from {results_table} a        
            LEFT JOIN LATERAL (
                SELECT tile_id
                FROM {extent_table} b
                WHERE ST_Intersects(a.geom_point, b.geom)
                LIMIT 1) extent ON true        
        where a.file_id = {file.id};      
    """)

    # list of geojsons for showing progeress
    total_count = len(file.names)
    this_count = file.names.index(file.name) + 1
    print(f"Finished split data {file.name} [{str(this_count)}/{str(total_count)}]")
    
def preprocessing():

    # create table for extents
    pg_engine.run_sql(f"""
        DROP TABLE IF EXISTS {extent_table};
        CREATE TABLE {extent_table} (
        tile_id SERIAL,
        tile_type TEXT,        
        tile_width INT,
        tile_name TEXT,
        geom GEOMETRY(GEOMETRY,4326));""")
        
    # create parent table for input polygons
    pg_engine.run_sql(f"""
        DROP TABLE IF EXISTS {input_table};
        CREATE TABLE {input_table} (
        poly_id INT,
        geom_simpel GEOMETRY(GEOMETRY,4326),
        tile_id INT);""") 

    # create table for infos about type of imported files
    pg_engine.run_sql(f"""
        DROP TABLE IF EXISTS {input_type};
        CREATE TABLE {input_type} (
        itype VARCHAR(12) NOT NULL);""")   


if __name__ == '__main__':

    # check sys args
    grid_size, dateline_distance = check_args.get_import_geojson_args(sys.argv[1:]) 

    # start time
    start_time = datetime.datetime.now()
    
    # list of geojsons to import 
    jsons = [f for f in os.listdir(config.JSON_INPUT_PATH) if f.endswith(".json") or f.endswith(".geojson")] 
    if len(jsons) == 0:
        print(f'No *.json or *.geojson file found! There should be at least one geojson as input in {config.JSON_INPUT_PATH}!')
        sys.exit()  
    
    # import files    
    input_files = [InputFile(json,jsons,jsons.index(json), dateline_distance) for json in jsons]
    
    # create partitions
    preprocessing()
    
    # run import parallel
    pr.run_parallel(import_json, input_files)

    # create resultstable
    create_results_table(input_files)    

    # create extents for splitting data in tiles
    create_extents_grid(grid_size)

    # split data into tiles
    pr.run_parallel(split_data, input_files)

    # insert file type info
    pg_engine.run_sql(f"""
        insert into {input_type} (itype)
        VALUES ('geojson');
    """) 

    # drop points
    pg_engine.run_sql(f""" ALTER TABLE {results_table} DROP COLUMN geom_point; """)   

    # create pk and indeces
    pg_engine.run_sql(f"""        
        ALTER TABLE {input_table} ADD PRIMARY KEY (poly_id);
        --ALTER TABLE {results_table} ADD PRIMARY KEY (poly_id);
        CREATE INDEX idx_{input_table}_tile_id_btree ON {input_table} (tile_id);
        --CREATE INDEX idx_{results_table}_filename_btree ON {results_table} (filename);
        CREATE INDEX idx_{extent_table}_geom_gist ON {extent_table} USING gist (geom);""")
    
    # total run time
    total_time = (datetime.datetime.now() - start_time).seconds/60
        
    print(f"Import finished in {str(round(total_time,2))} minutes")