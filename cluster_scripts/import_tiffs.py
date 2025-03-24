from multiprocessing import Pool
import subprocess
import datetime
import os
import sys
from osgeo import gdal

import check_args, config, pg_engine, split_raster, parallel_runner as pr


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
    def __init__(self, file_name, file_names, file_id, pxl_size, nodata, dateline_distance):
        self.name = file_name
        self.names = file_names
        self.id = file_id
        self.path = config.RASTER_INPUT_TEMP_PATH + '/tiles/' + file_name
        self.pxl_size = pxl_size
        self.nodata = nodata
        self.dateline_distance = dateline_distance + 500
        self.stats = self.get_stats()
        self.is_empty = self.check_if_empty()

        
    def get_stats(self):

        src_ds = gdal.Open(self.path)
        srcband = src_ds.GetRasterBand(1)

        # get stats
        stats = srcband.GetStatistics(True, True)

        return stats

    def check_if_empty(self):
        if self.stats[0] == self.nodata and self.stats[1] == self.nodata:
            return True
        else:
            return False        


def get_raster_info(tiff):
    gdal.UseExceptions()

    tiff_path = config.RASTER_INPUT_TEMP_PATH + '/tiles/' + tiff

    # open raster
    src_ds = gdal.Open(tiff_path)

    # get noData value
    srcband = src_ds.GetRasterBand(1)
    nodata = srcband.GetNoDataValue()

    # get min pixel size
    gt = src_ds.GetGeoTransform()
    pxl_size_x = gt[1]
    pxl_size_y =-gt[5]
    pxl_size_min = min(pxl_size_x,pxl_size_y)    

    # close raster
    src_ds = None

    return nodata, pxl_size_min


def import_tiffs(file): 

    total_count = len(file.names)
    this_count = file.names.index(file.name) + 1

    # only import if tiff is not empty
    if file.is_empty==False: 

        # temp table of input
        import_table = 'temp_raster_' + str(file.id)

        # delete temp_raster
        pg_engine.run_sql(f"""drop table if exists {import_table}""")

        # import raster
        cmd = config.raster2pgsql_PATH + f" -s 4326 -t 500x500 -Y 5000 -F -n filename -c " + file.path + " " + import_table + " | " + config.psql_PATH + " -U %(user)s -d %(db)s -h %(host)s -p %(port)s" % config.CLUSTER_POSTGRES
        subprocess.call(cmd, shell=True, stdout=open(os.devnull, 'wb'))

        # create extents
        pg_engine.run_sql(f"""
            with extents as (
                SELECT ST_Envelope(ST_Collect(ST_Envelope(rast))) as geom, '{file.name}' as  tile_name    
                FROM {import_table}
                GROUP BY tile_name     
            )
            INSERT INTO {extent_table} (tile_type, tile_width, tile_name, geom)
            SELECT
                'raster' as tile_type,

                -- get tile_width (shortest Latitude)
                Least (
                    -- southern Latitude
                    ST_Distance(
                        CONCAT('SRID=4326;POINT(', ST_XMin(geom), ' ' , ST_YMin(geom),  ')')::geography,
                        CONCAT('SRID=4326;POINT(', ST_XMax(geom), ' ' , ST_YMin(geom),  ')')::geography
                    ),

                    -- nothern Latitude
                    ST_Distance(
                        CONCAT('SRID=4326;POINT(', ST_XMin(geom), ' ' , ST_YMax(geom),  ')')::geography,
                        CONCAT('SRID=4326;POINT(', ST_XMax(geom), ' ' , ST_YMax(geom),  ')')::geography
                    )
                ) as tile_width,

                tile_name,
                geom    
            FROM extents;     
            """)

        # get extent id
        tile_id = pg_engine.get_sql(f"""select tile_id from {extent_table} where tile_name = '{file.name}' ;""")[0][0]

        # vectorize raster (geom_poly)    
        # buffer polygons to mid (geom_mid)
        # simplify mid polygons (geom_simpel)
        # polygon inside point (geom_point)
        # count pixels per polygon (pxl_cnt)     


        pg_engine.run_sql(f"""
            -- vectorize input raster
            CREATE TEMP TABLE {import_table}_polys as (        
                SELECT geom as geom_poly,
                    ST_Buffer(geom, (-{str(file.pxl_size)})/2*0.9999, 'endcap=flat join=mitre mitre_limit=2.0') as geom_mid,
                    ROUND( ST_Area(geom) / {str(file.pxl_size ** 2)} ) as pxl_cnt,                
                    {tile_id} as tile_id       
                FROM {import_table}, LATERAL ST_DumpAsPolygons(ST_SetBandNoDataValue(rast,{str(file.nodata)})) AS dp
                );

            -- create spatial index
            CREATE INDEX idx_{import_table}_polys_gist ON {import_table}_polys USING gist (geom_poly);

            -- insert polygons into intput talbe 
            -- and check distance to dateline
            INSERT INTO {input_table} (geom_poly,geom_mid,geom_simpel,geom_point,pxl_cnt,pxl_area,tile_id)
            
                with dateline as (
                    -- define narrow polygones at dateline
                    select ST_Union(
                        ST_Buffer(ST_Segmentize(ST_LineFromText('LINESTRING(179.999999 -89, 179.999999 89)',4326)::geography,1000000), {file.dateline_distance},'side=left')::geometry,
                        ST_Buffer(ST_Segmentize(ST_LineFromText('LINESTRING(-179.999999 -89, -179.999999 89)',4326)::geography,1000000), {file.dateline_distance},'side=right')::geometry
                        ) as geom_dateline
                )

                SELECT geom_poly,
                    geom_mid,
                    CASE
                        WHEN pxl_cnt = 1 THEN ST_Centroid(geom_poly)
                        WHEN pxl_cnt = 2 OR pxl_cnt = 3 THEN ST_LineMerge(ST_ApproximateMedialAxis(geom_poly))
                        ELSE ST_SimplifyPreserveTopology(geom_mid, {str(file.pxl_size)})
                    END geom_simpel,    
                    ST_PointOnSurface(geom_poly) as geom_point,
                    pxl_cnt,
                    ROUND( ST_Area(geom_poly::geography)) as pxl_area,                
                    tile_id
                FROM {import_table}_polys a, dateline b
                where ST_Intersects(a.geom_poly, b.geom_dateline) = false;
            """)        

        # delete temp_raster
        pg_engine.run_sql(f"""drop table {import_table}""")

        # list of tiffs for showing progeress
        print(f"Finished importing {file.name} [{str(this_count)}/{str(total_count)}]")

    else:
        print(f'Skipping {file.name}, because it is empty (only nodata values) ... [{str(this_count)}/{str(total_count)}]')
    
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
        poly_id SERIAL,
        geom_poly GEOMETRY(GEOMETRY,4326),
        geom_mid GEOMETRY(GEOMETRY,4326),
        geom_simpel GEOMETRY(GEOMETRY,4326),
        geom_point GEOMETRY(GEOMETRY,4326),
        pxl_cnt INT,
        pxl_area INT,
        tile_id INT);""") 

    # create parent table for results polygons
    pg_engine.run_sql(f"""
        DROP TABLE IF EXISTS {results_table};
        CREATE TABLE {results_table} (
        poly_id INT,
        geom GEOMETRY(GEOMETRY,4326),
        pxl_cnt INT,
        pxl_area INT,
        tile_id INT);""")   

    # create table for infos about type of imported files
    pg_engine.run_sql(f"""
        DROP TABLE IF EXISTS {input_type};
        CREATE TABLE {input_type} (
        itype VARCHAR(12) NOT NULL);""")   

def postprocessing():
    # create placeholder for results    
    pg_engine.run_sql(f"""INSERT INTO {results_table} (poly_id,geom,pxl_cnt,pxl_area, tile_id)
        SELECT poly_id, geom_poly as geom, pxl_cnt, pxl_area, tile_id FROM {input_table};""")

    # insert file type info
    pg_engine.run_sql(f"""
        insert into {input_type} (itype)
        VALUES ('tiff');
    """) 
    
    # create pk and indeces
    pg_engine.run_sql(f"""        
        ALTER TABLE {input_table} ADD PRIMARY KEY (poly_id);
        CREATE INDEX idx_{input_table}_tile_id_btree ON {input_table} (tile_id);
        CREATE INDEX idx_{extent_table}_geom_gist ON {extent_table} USING gist (geom);""")  
        

def set_nodata(nodata_meta, nodata_param):

    if nodata_param is not None:
        print(f"nodata value was set by input parameter: {str(nodata_param)}")
        return nodata_param
    
    elif nodata_meta is not None:
        print(f"nodata value was read from metadata: {str(nodata_meta)}")
        return nodata_meta

    else:
        print(f"nodata value was set to '0' (default)")
        return 0

        
if __name__ == '__main__':

    # get tilesize and nodata value from script parameter
    tilesize, nodata_param, dateline_distance =  check_args.get_import_tiff_args(sys.argv[1:]) 

    # split input raster    
    split_raster.raster_tiler(tilesize)

    # start time
    start_time = datetime.datetime.now()
    print('Starting to import splittet raster to db')
    
    # list of splitted tiffs to import
    tiffs = os.listdir(config.RASTER_INPUT_TEMP_PATH + '/tiles')

    # get global raster info of first tiff
    nodata_meta, pxl_size_min = get_raster_info(tiffs[0])

    # set final nodata value
    nodata = set_nodata(nodata_meta, nodata_param)

    # list for parallel processing 
    input_files = [InputFile(tiff,tiffs,tiffs.index(tiff), pxl_size_min, nodata, dateline_distance) for tiff in tiffs] 

    # create partitions
    preprocessing()
    
    # run parallel
    pr.run_parallel(import_tiffs, input_files)

    # run post processing
    postprocessing()
    
    # total run time
    total_time = (datetime.datetime.now() - start_time).seconds/60        

    print(f"----------------------------------------------")
    print(f"  Complete import finished in:")
    print(f"    {str(round(total_time,2))} minutes")
    print(f"----------------------------------------------")