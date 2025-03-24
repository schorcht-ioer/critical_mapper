from multiprocessing import Pool
import datetime, time, os, sys, shutil
from osgeo import gdal
from osgeo import ogr
from osgeo import osr

import check_args, config, pg_engine, parallel_runner as pr

# sen env var for proj lib
os.environ["PROJ_LIB"] = config.proj_lib

# results table of clustered polygons
resutls_table = config.results_table
# name of extent table
extent_table = config.extent_table
# name of temp results table for rastering
results_table = 'cluster_raster_results_'

# print every N step
global print_step
print_step =  1


class OutputFile:
    def __init__(self, args):
        self.name, self.names, self.id, self.d = args
        self.ref_ras =  config.RASTER_INPUT_TEMP_PATH + '/tiles/' + self.name
        self.out_path = config.RASTER_OUTPUT_PATH + f'/temp/distance_{str(self.d)}'    
        self.out_ras = self.out_path + f'/{self.name[:-4]}_gcid_d{str(self.d)}.tif'
        # run export
        self.export_raster()

    def export_raster(self):
        try:
        
            # temp tile results table 
            results_table_tile = results_table + str(self.id)
    
            # create tile results table  as layer
            pg_engine.run_sql(f"""
                drop table if exists {results_table_tile};
                create UNLOGGED table {results_table_tile} as (      
                    select gcid_d{str(self.d)} as global_cluster_id, geom                
                    from {resutls_table}
                    where tile_id = {self.id});
            """) 
        
            # open reference raster 
            ras_ds = gdal.Open(self.ref_ras)
            geot = ras_ds.GetGeoTransform()
            prj_wkt = ras_ds.GetProjection()
            
            # connect to db
            connString = 'PG: dbname=%(db)s host=%(host)s user=%(user)s password=%(pw)s port=%(port)s'  % config.CLUSTER_POSTGRES       
            conn = ogr.Open(connString)
            lyr = conn.GetLayer(results_table_tile)
    
            # setup new raster
            drv_tiff = gdal.GetDriverByName("GTiff") 
            chn_ras_ds = drv_tiff.Create(self.out_ras, ras_ds.RasterXSize, ras_ds.RasterYSize, 1, gdal.GDT_UInt32, options=['COMPRESS=LZW'] ) # if 32bit is not enough, change to GDT_Float64!!!
            chn_ras_ds.SetGeoTransform(geot)
            chn_ras_ds.SetProjection(prj_wkt)
            
            # create raster
            gdal.RasterizeLayer(chn_ras_ds, [1], lyr, options=['ATTRIBUTE=global_cluster_id'])
            chn_ras_ds.GetRasterBand(1).SetNoDataValue(0.0) 
            chn_ras_ds = None
            
            # delete tmp results table
            pg_engine.run_sql(f"""         
                DROP TABLE IF EXISTS {results_table_tile};
                """)

            # print progeress    
            total_count = len(self.names)
            this_count = self.names.index(self.name) + 1
            if (this_count % print_step == 0):
                print(f"Export of Tiff '{self.name}' finished [{str(this_count)}/{str(total_count)}]")

        except Exception as error:
            print(f"ERROR ON {self.name}: ", error)              


def combine_tiles(out_path,d):

    # mosaik path
    mosaik_path = f"{config.RASTER_OUTPUT_PATH}/temp/mosaic_d{str(d)}.vrt"
    input_file_name = os.listdir(config.RASTER_INPUT_PATH)[0][:-4]
    combined_tif_path = f"{config.RASTER_OUTPUT_PATH}/{input_file_name}_epsg4326_d{str(d)}.tif"

    print('Starting combine tiled tiffs')

    # create virtual mosaik
    os.system(f"gdalbuildvrt {mosaik_path} {out_path}/*.tif")

    os.system(f'{config.GDALTRANSLATE_PATH} --config GDAL_CACHE_MAX 2048 -of GTiff -co "COMPRESS=DEFLATE" -co "TILED=YES" -co "BIGTIFF=YES" {mosaik_path} {combined_tif_path}')

    # cleaning the temp files
    shutil.rmtree(config.RASTER_OUTPUT_PATH + '/temp')   


def tiff_exporter(d, print_step_param = 1):

    # start time
    start_time = datetime.datetime.now()

    #set print_step
    global print_step
    print_step = print_step_param

    # get list of files for iterating
    extents = pg_engine.get_sql(f"""select tile_name, tile_id from {extent_table} ;""")

    # get list only of tile names
    tile_names =  [extent[0] for extent in extents]

    # list of extents with tuple arguments
    tiles = [(extent[0], tile_names, extent[1], d) for extent in extents]

    # create index
    pg_engine.run_sql(f"""CREATE INDEX if not exists idx_tile_id_results_d{str(d)} on {resutls_table} (tile_id);""")   

    # create output raster folder if not exists
    os.makedirs(config.RASTER_OUTPUT_PATH, exist_ok=True)     
    
    # create out raster temp path
    out_path = config.RASTER_OUTPUT_PATH + f'/temp/distance_{str(d)}'
    # create temp folder if not exists
    os.makedirs(out_path, exist_ok=True)
    
    # run export tiles parallel
    pr.run_parallel(OutputFile, tiles)

    # combine tiles
    combine_tiles(out_path,d)

    total_time = (datetime.datetime.now() - start_time).seconds/60
    print(f"----------------------------------------------")
    print(f"  Complete export of Tiffs with distance of {str(d)} meters finished in:")
    print(f"    {str(round(total_time,2))} minutes")
    print(f"----------------------------------------------")


# make it also runable as standalone script
if __name__ == '__main__':    

    # check sys args (distance, print_step)
    d, print_step_param = check_args.get_cluster_args(sys.argv[1:])

    # run exporter
    tiff_exporter(d, print_step_param)