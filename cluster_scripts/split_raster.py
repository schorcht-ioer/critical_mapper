from osgeo import gdal, osr
import sys, os, shutil, datetime

import check_args, config, parallel_runner as pr

class InputRaster:

    def __init__(self, file_name, tilesize):
        self.name = file_name
        self.path = config.RASTER_INPUT_PATH + '/' + file_name
        self.tilesize = tilesize 
        self.raster = self.get_rater()
        self.srcband = self.raster.GetRasterBand(1)  
        self.proj = self.get_proj()
        self.num_bands = self.raster.RasterCount 
        self.check_bands()
        self.check_proj()
        self.check_size()
        self.get_split_jobs()



    def get_rater(self):    
        # open raster
        src_ds = gdal.Open(self.path)

        # validate if tiff is readable
        if src_ds is None:
            print(f'Unable to open {self.path}. Either it is not a valid tiff or does not exists!')
            sys.exit(1)

        return src_ds   

    def get_proj(self):        
        proj = osr.SpatialReference(wkt=self.raster.GetProjection())
        return proj.GetAttrValue('AUTHORITY',1)

    def reproj(self, reproj_tif_dir, reproj_tif_name):
        # remove old temp proj data
        if os.path.isdir(reproj_tif_dir):
            shutil.rmtree(reproj_tif_dir)

        # and create new one
        os.makedirs(reproj_tif_dir, exist_ok=True)  

        # starting reprojection      
        print('Starting reprojection to EPSG: 4326')
        output_raster = reproj_tif_dir + '/' + reproj_tif_name

        #raster_reproj = gdal.Warp(output_raster,self.raster,dstSRS='EPSG:4326')

        gdalwarp_params = f'--config GDAL_CACHEMAX 2048 -t_srs EPSG:4326 -co TILED=YES -co compress=DEFLATE -multi -wo NUM_THREADS={config.NUM_PROC}'
        gdalstring = f'{config.GDALWARP_PATH} {gdalwarp_params} "{self.path}" "{output_raster}"'
        os.system(gdalstring)

        # redefine as new raster
        self.name = reproj_tif_name
        self.path = output_raster
        self.raster = self.get_rater()

        print('')
        print('Reprojection finsihed')
        print('The reprojected raster is saved at:', output_raster)
        # ... 

    def check_bands(self):
        if self.num_bands != 1:
            prin('Error: The number of bands of the input tiff must be one!')
            sys.exit()

    def check_proj(self):
        if self.proj != '4326':
            print('The input tiff is not WGS84 (EPSG: 4326)')
            reproj_tif_dir = config.RASTER_INPUT_TEMP_PATH + '/proj' 
            reproj_tif_name = 'raster_reproj_epsg4326.tif'
            self.reproj(reproj_tif_dir, reproj_tif_name)

    def check_size(self):
        ulx, xres, xskew, uly, yskew, yres  = self.raster.GetGeoTransform()
        lrx = ulx + (self.tilesize * xres)

        width_deg = lrx - ulx

        if width_deg < 0.002:
            print(f'Error: The resulting width of {str(round(width_deg,7))}... degree per tile is far too small ( < 0.002 degree)!')
            print('Hint: Significantly increase the number of pixels using the tile_size parameter to obtain larger tiles, like: -t 10000.')
            sys.exit()

    def get_split_jobs(self):

        split_jobs = []
        tiles_dir = config.RASTER_INPUT_TEMP_PATH + '/tiles'

        # remove old temp tiles data
        if os.path.isdir(tiles_dir):
            shutil.rmtree(tiles_dir) 
        # and create new one
        os.makedirs(tiles_dir, exist_ok=True) 

        # get split jobs
        width = self.raster.RasterXSize
        height = self.raster.RasterYSize
        for i in range(0, width, self.tilesize):
            for j in range(0, height, self.tilesize):
                w = min(i+self.tilesize, width) - i
                h = min(j+self.tilesize, height) - j
                gdaltranString = f'{config.GDALTRANSLATE_PATH} -of GTIFF -co "COMPRESS=DEFLATE" -srcwin {str(i)} {str(j)} {str(w)} {str(h)} {self.path} {tiles_dir}/tile_{str(i)}_{str(j)}.tif'
                split_jobs.append(gdaltranString)
                #os.system(gdaltranString)

        return split_jobs   

    def close_raster(self):
        self.raster = None


def split_raster(gdaltranString):
    os.system(gdaltranString)


def raster_tiler(tilesize):
    
    # start time
    start_time = datetime.datetime.now()
    print('Starting to split input raster')

    # get input raster
    tiff = [f for f in os.listdir(config.RASTER_INPUT_PATH) if f.endswith(".tif")]
    if len(tiff) > 1:
        print(f'To many input rasters! There should be just one raster as input in {config.RASTER_INPUT_PATH}!')
        sys.exit()   
    elif len(tiff) == 0:
        print(f'No *.tif file found! There should be at least one raster (*.tif) as input in {config.RASTER_INPUT_PATH}!')
        sys.exit()  

    # init input raster
    raster = InputRaster(tiff[0], tilesize)

    # get the jobs for splitting
    split_jobs = raster.get_split_jobs()

    # run parallel
    pr.run_parallel(split_raster, split_jobs)

    # close raster
    raster.close_raster()

    total_time = (datetime.datetime.now() - start_time).seconds/60
    print(f"----------------------------------------------")
    print(f"  Finished splitting input raster in:")
    print(f"    {str(round(total_time,2))} minutes")
    print(f"----------------------------------------------")

if __name__ == '__main__':
    
    # get tilesize from args
    tilesize = check_args.get_splitter_args(sys.argv[1:])

    # run raster tiler
    raster_tiler(tilesize)




