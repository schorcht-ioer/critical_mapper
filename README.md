# CriticaL Mapper
This tool offers two main functionalities:
- **Spatial Clustering of big data**: We have developed a tile-based algorithm that can be used to generate huge global clusters. Both raster data (GeoTIFF) and vector data (GeoJSON) can be used as input. 
- **Calculation of the Critical Distance**: The Critical Distance is a structural measurement that comes from percolation theory (which originates from statistical physics) and describes the phase transition between fragmented clusters and a giant cluster. This methodical approach is applied to geodata (like buildings). 

The tool is easy to use in a Docker environment or is also runable in a local native environment.

## Background Information

The *CriticaL Mapper* tool was developed as part of the project *Landscape Criticality in the Anthropocene - Biodiversity, Renewables and Settlements (CriticaL)*, in which, among other things, the determination of a Critcal Distance plays a role, see: [https://www.ioer.de/en/projects/critical](https://www.ioer.de/en/projects/critical)

The corresponding paper is still in progress, but information on the percolation theory and the Critical Distance for Germany can be found in a paper that has already been published (https://doi.org/10.1016/j.landurbplan.2019.103631).

Since existing cluster functions could not handle the scope of a global raster data set, this tool was developed. At its core, Postgis is used, but this also has the limitation that no bigger data sets can be processed, as RAM errors occur above a certain size. Therefore, a tile-based algorithm was developed with which the objects of the tiles are first clustered and then combined into larger clusters that could span multiple tiles. The tile-based calculation has also the advantage that less working memory is required and most functions can be executed in parallel.

Using the CriticaL Mapper tool presented here, for example, clusters of 60 different distances were calculated for the global raster dataset of the *World Settlement Footprint (WSF 2019)*. This raster has a pixel size of 10 m $\times$ 10 m (near the equator) with a total resolution of 4 $\times$ 1.5 million pixels. On a PC with 64 cores and 512 GB RAM, the global calculation of a cluster distance of 100 m took about 4 hours and of 30,000 m about 16 hours.

The algorithm was developed for small objects (like buildings) and cluster distances of up to ~100 km. The distance between the objects to be clustered is determined using geodesic buffers. In this buffering process, a reprojection into the best possible planar projection is carried out object by object. For smaller objects or distances (< 100 km), one can assume quite high accuracies plus good performance. Greater cluster distances (> 1,000 km) can lead to inaccurate results due to deviations. 


## Quickstart

>[!IMPORTANT]
>If your PC has less than 4 CPU cores (or threads) or 16 GB RAM, adjust the [configuration](#config) accordingly!

```bash
### clone repo and create container
cd my_project_folder
git clone https://github.com/schorcht-ioer/critical_mapper
cd critical_mapper
cp .env.example .env
docker-compose up -d

### cluster data
# copy example raster to input folder 
cp input_example/raster/test_raster.tif input/raster
# import raster
docker exec cluster_postgis_container python3.9 /cluster/cluster_scripts/import_tiffs.py -t 15000
# clusterize raster
docker exec cluster_postgis_container python3.9 /cluster/cluster_scripts/loop_cluster.py -d 100,500,1000,1500,2000

### calculate Critical Distance
# copy example border to input folder 
cp input_example/borders/borders.json input/borders
# start script for calculating
docker exec cluster_postgis_container python3.9 /cluster/cluster_scripts/calc_critical_distance.py
# --> result json with Critical Distances will be on output/borders/borders.json
```
In this short example, the Docker Compose project is first installed, then a raster is imported and automatically split into tiles (with a resolution of 15,000 $\times$ 15,000). Afterwards, the data is clustered for distances of 100, 500, 1000, 1500 and 2000  meters. Finally, the Critical Distances are calculated for the respective regions based on the five cluster distances.

You should test this example to see whether the tool works correctly.

>[!TIP]
>  If you use *Git Bash*, you must add a ‘/’ in front of any path in the docker exec command, like: 
>  docker exec cluster_postgis_container python3.9 **/**/cluster/cluster_scripts/anyscript.py


## Run with Docker

The following shows how the tool is run in a Docker environment.
Alternatively, it can also be set up natively on your local environment (see: [Run on a native environment](#native)).

### 1. Clone the repo
Create a project folder, go to this folder and clone the repo:
```bash
cd my_project_folder
git clone https://github.com/schorcht-ioer/critical_mapper
```
### 2. Edit .env file
If desired, you can customize the .env file:
```bash
cd critical_mapper
cp .env.example .env
vim .env
```
Here we use vim for editing, but you can of course also use another text editor.

### 3. Create and start the container
```bash
docker-compose up -d
```
On the first run this command creates the container *cluster_postgis_container* in your compose project (named *critical_mapper* by default) and installs the PostgreSQL database, Postgis extension and other requirements.

After setting up, check whether the container is running:
```bash
docker ps
```

####  A few words about the folder structure

The `input` and `output` folders are mountet to your local project folder so that you can simply copy and paste files without having to go into the container. The folder with the scripts (`cluster_scripts`) is as well mounted in order that configuration files (`config.py`, `sql_conf.py`) can be adapted if necessary. Furthermore, the database folder (`dbdata`) is also mounted to the project folder so the Docker partition does not have to be extended in case of larger amounts of data (especially when *Docker for Windows* is used).

### <a name="config"></a>4. Configuration
After setting up, you can adjust the number of CPU cores and RAM to be used. For example, if you want to use 8 cores and 16 GB RAM, you can run the following:

```bash
docker exec cluster_postgis_container python3.9 /cluster/cluster_scripts/configurator.py --cpu 8 --ram 16
```
#### *required parameters*:
- -c, --cpu		:  \<integer> Set the number of CPU cores/threads you want to use
- -r, --ram		:  \<integer> Set the amount of **GB** of your RAM that you want to use

After executing the script, your DB container should be restarted automatically to apply the changes to the configuration.

>[!IMPORTANT]
>It is recommended to provide a maximum of 80% of your resources. If you have many cores and less RAM, it is also advisable not to specify more cores than GB RAM (Number of cores <= GB RAM), as the processes have to share the RAM. 

### 5. Import the data
You can import GeoTIFF files (raster data) or GeoJSON files (vector geometries) in any projection. The import scripts read the data and automatically reproject it to WGS84 (EPSG: 4326) and split it into quadratic tiles. 

#### What is the most suitable tile size?
However, the size of the tiles depends on the level of detail or amount of data the files contain. Therefore, you must define the edge length of the tiles yourself. The edge length is specified in pixels for raster data (*tile size*) and in degrees for vector data (*grid size*). 

The tile size must not be smaller than the largest cluster distance. It would generally be advisable to define a tile size that is as large as possible and that is many times greater than the largest cluster distance.

For example, for the raster of the *World Settlement Footprint* dataset, which has a pixel size of ~ 10 m $\times$ 10 m (near the equator), a *tile size* of 25,000 pixels is a good guideline (which means that the tiles have a resolution of 25,000 $\times$ 25,000 pixels). For building vector data, a *grid size* of 1 to 2 degrees would be recommended. It is also advisable to have at least as many tiles as CPU cores are used to ensure optimum parallel processing.

Tiles that are too small have the disadvantage during the clustering process that combining the tiles takes a long time. Tiles that are too large, on the other hand, lead to RAM errors. You should test whether the clustering process runs without errors in a part of your data set with high density.

If you receive a PostgreSQL error message during the cluster process like the following, you have selected a size that is too large.
```bash sql
ERROR:  array size exceeds the maximum allowed (1073741823)
```
OR
```bash sql
ERROR:  invalid memory alloc request size 4194181076 
```
In this case, reduce the *tile size* or *grid size*.

#### Limitations regarding the dateline
It should be noted that clusters cannot be formed across the dateline. Objects that are close to the dateline may lead to errors (as geodesic buffers do not work across the dateline). You therefore have the option of specifying a minimum distance to the dateline. Objects that are within this distance of the dateline are excluded during the import process so that they can no longer cause errors. If you have objects near the dateline, it is recommended to set the maximum cluster distance as the distance to the dateline (as `-l` argument).

>[!NOTE]
> Only one data set can be stored in the database at a time. If, for example, a GeoTIFF is imported first and then a GeoJSON, the imported objects of the raster are overwritten.

### 5.1 Import Raster data
When importing raster data, those pixels that are **not** nodata are vectorized and used later in the clustering process. It is recommended to have **only one pixel value**, as is the case in the example data (see: `input_example/raster/test_raster.tif`). Several pixel values would be possible, but would significantly slow down the clustering process.

Copy your GeoTIFF file into the input folder for raster data (`input/raster`) and start the script `import_tiffs.py`:
```bash
# copy raster to input folder for rasters 
cp /path/to/very_big_raster.tif input/raster
# run raster importer
docker exec cluster_postgis_container python3.9 /cluster/cluster_scripts/import_tiffs.py \
  --tile_size 25000 \
  --nodata 0 \
  --dateline_distance 10000  
```
>[!NOTE]
>You can only import one raster file at a time. Make sure that there are no other raster files in the input folder, e.g. from previous import processes!
#### *required parameters*:
- -t, --tile_size :  \<integer> Set the number of pixels to split the input raster into tiles (resolution will be t * t)

#### *optional parameters*:
- -n, --nodata	:  \<float> Set nodata value if it is not already correctly defined in the metadata (default: 0).
- -l, --dateline_distance :  \<integer> Distance to dateline **in meters**. This should be the maximum cluster distance.

>[!IMPORTANT]
>It is important that you enter the correct value for nodata pixels if it is not already defined in the metadata of the GeoTIFF. If neither a value is specified as argument `-n` and no value is contained in the metadata, the value is set to 0.

### 5.2 Import Vector data
Alternatively, GeoJSON can also be imported, which works in a very similar way. If only clustering is used, the input geometries can be points, lines and polygons (or all together). However, if Critical Distances are also to be calculated, **only polygons** should be imported (if necessary, buffer points or lines with smaller distances beforehand in order to get polygons). If objects are modeled as mutlipart geometries, these are retained. In contrast to the raster import, several files can be imported at once. However, they must all have the same schema (identical field names and number of fields)!

Copy your GeoJSON file(s) to the input folder for vector data (`input/geojson`) and start the script `import_geojsons.py`:
```bash
# copy input files to input folder for geojsons
# (here we copy two files: buildings_dresden.json, buildings_pirna.json)
cp input_example/geojson/* input/geojson
# run geojson importer
docker exec cluster_postgis_container python3.9 /cluster/cluster_scripts/import_geojsons.py \
  --grid_size 0.05 \
  --dateline_distance 10000  
```
>[!NOTE]
>Make sure that there are no "old" files in the input folder, e.g. from previous import processes!
#### *required parameters*:
- -g, --grid_size	:  \<float> Set the grid size value **in degree** to separate the input objects into tiles.

#### *optional parameters*:
- -l, --dateline_distance		:  \<integer> Distance to dateline **in meters**. This should be the maximum cluster distance.


### 6. Clustering
Once the raster **or** vector data has been imported, clustering can be started using the `loop_cluster.py` script:
```bash
docker exec cluster_postgis_container python3.9 /cluster/cluster_scripts/loop_cluster.py \
  --distances 100,200,300 \
  --print_steps 10 \
  --export true \
  --delete_temp false    
```
>[!TIP]
>If you only want to cluster your data, set `--export true`, so that the resulting clusters are exported as a file to the /`output/...` folder!
#### *required parameters*:
- -d, --distances : \<list[integer]>  Set one [or more] distance[s] **in meters** as comma separated list (without spaces!).
**or**
- -r, --range : \<list[integer]>  Set start,stop,step **in meters** as comma separated list (without spaces!).

#### *optional parameters*:
- -p, --print_steps	: \<integer> Prints the status of every N tiles (default: 1).
- -e, --export	: \<boolean> Export the clustered data (default: false).
- -del, --delete_temp : \<boolean>  Delete all temporary data, like projected or tiled tiffs and db-data (default: false).

If several distances are specified (e.g. `-d 100,250,500`), several cluster processes are executed one after the other. However, you can also specify just one distance (e.g. `-d 100`). 

Alternatively, a range for distances can also be specified by defining start, stop and step, by using the `-r` parameter. E.g. a list of distances from 100 m to inclusive 5000 m in steps of 100 m is generated with `-r 100,5000,100` and will result in a distance list of `100,200,...,4900,5000`.

If you have enabled the export of cluster results (`--export true`), all cluster results are exported as a file to the /`output/...` folder and are reprojected to WGS84 (EPSG:4326). If the input data was a raster, the output data will also be a raster accordingly. The result raster[s] is [are] stored in the `output\raster` folder with the suffix *...epsg4326_d**** for each distance. If GeoJSON[s] was used as input instead, the resulting GeoJSON[s] is [are] stored in the `output\geojson` folder, where the cluster IDs are appended as a column for each distance (gcid_d***).

>[!TIP]
>`loop_cluster.py` can be executed as often as required. So, the database is further enriched with each new cluster process. For example, `-d 100,200` produces the same result as starting twice with `-d 100` and `-d 200`. This is particularly helpful if a larger number of different cluster distances are to be used to calculate the Critical Distance. However, if the argument *delete_temp* is set to *true*, `loop_cluster.py` cannot be run again. A new data set must then be imported first.

### 7. Critical Distance
Now the Critical Distance can be calculated, which is based on the various cluster distances in the previous step. 
The finer the intervals of the cluster distances are selected, the more precisely the Critical Distance can be determined, although this is associated with a longer calculation time.

However, a boundary geometry is required to calculate the Critical Distances, which could be administrative regions or country borders, for example. A separate Critical Distance is determined for each individual boundary polygon. If only a single Critical Distance is required for the entire study area, a polygon of the bounding box of the input data set can be used as the border geometry instead.

Only a json file containing polygons as boundary geometries is accepted as input (see e.g. `input_example/borders/borders.json`).

To start the script for calculating the Critical Distances, execute the following:

```bash
# copy border json to input folder for borders
cp input_example/borders/borders.json input/borders
# start script for calculating
docker exec cluster_postgis_container python3.9 /cluster/cluster_scripts/calc_critical_distance.py \
  --print_steps 5
```
#### *optional parameters*:
- -p, --print_steps	: \<integer> Prints the status of every N border (default: 1).

The resulting border geometries with the calculated Critical Distances are exported to `output/borders/` under the file name of your input border file.

#### *output data description*:
The resulting layer with the Critical Distances have the following attributes (including the forwarded input attributes):
- critical_id : \<integer> Global unique identifier
- critical_distance : \<integer> Critical Distance in meters
- border_area_sqkm : \<float> Geodetic area of the border polygon in square kilometer
- cluster_area_sqkm : \<float> Summed geodetic area of the contained cluster polygons in square kilometer
- share : \<float> Share of cluster area in border area
- status : \<text> Status of the calculation of Critical Distance with the following codes:
  - nodata : No cluster area is inside the border are, so no Critical Distance is calculated
  - not percolated : There was no percolation, so no Critical Distance could be calculated (see note below)
  - percolated : Percolation happened, the clusters snapped together at the Critical Distance

>[!NOTE]
>`not percolated` means, that no mega cluster was formed, as either the objects were already grouped into one cluster at the smallest cluster distance, or the largest cluster distance was not large enough to group the objects into a mega cluster.

## <a name="native"></a>Run on a native environment

The scripts in the folder `cluster_scripts` are also working on linux or windows environments without docker. Copy this folder to your machine.

>[!NOTE]
>Although operation in a native environment is faster, it is more time-consuming to set up than the Docker environment (more for advanced users).

### 1. Install requirements
Install the following:
- Python 3.9+ (3.6+ should also work, but is not tested)
-- including modules: SQLAlchemy v1.4.41, psycopg2 v2.9.3, osgeo gdal v3.2.2
- PostgreSQL 15+
-- psql
- Postgis v3.4.0+
-- including extensions: postgis_raster, postgis_sfcga
- GDAL 3.2.2
-- including tools: ogr2ogr, raster2pgsql, gdalwarp, gdal_translate

Check if everything is running. Especially check in python if `from osgeo import gdal` and `import sqlalchemy` is working. Check whether the Postgis function *ST_ClusterIntersectingWin* exists and whether the *postgis*, *postgis_sfcga* and *postgis_raster* extensions are also created in the database you want to use!

For Linux systems, you can take a look at the Dockerfile in the `postgis_with_cli` folder to get an idea of how to install the requirements. In most cases, the GDAL tools come with the Postgis installer, but it depends on which installer you use. If some of the GDAL tools are missing, you can also install [OSGeo4W](https://www.osgeo.org/projects/osgeo4w/) (for Windows) or [QGIS](https://www.qgis.org/) and use the GDAL tools that come with it. 

### 2. Configuration
#### 2.1 Edit config.py
In the folder `cluster_scripts` you will find the file `config.py`. All relevant paths are defined in this file. Replace the connection details to your PostgreSQL DB and set all paths to the tools.

If you dont know the path to *proj_lib*, you can run inside psql this: `SELECT PostGIS_PROJ_Version();`. Then you will see something like *DATABASE_PATH=/usr/share/proj/proj.db*. Take this path (without */proj.db*) as the value for the *proj_lib* parameter (like `proj_lib =  r"/usr/share/proj"`).  

#### 2.2 Set CPU and RAM usage
The configuration of CPU and RAM usage is quite similar to the Docker configuration (see: [configuration](#config)).

For example, with a usage of 8 cores and 16 GB RAM, you can run the following:
```bash
cd cluster_scripts
python configurator.py --cpu 8 --ram 16
```

If PostgreSQL does not restart automatically, restart it manually. You can check in psql whether the value shown for shared_buffers has been successfully applied via `SHOW shared_buffers;`.

### 3. Run Import, Clustering and Critical Distance
The next steps are identical to the description for the docker environment. You just need to import and cluster your data (and optional calculate the Critical Distance), as described after the [configuration](#config) part.

Of course, instead of `docker exec cluster_postgis_container python3.9 .../anyscript.py` you will start the scripts with `python anyscript.py`.

## Example Data Licenses
- GeoJSONS: 
The example data of the geojsons are derived polygons from LoD1 and are taken from "Landesamt für Geobasisinformation Sachsen (GeoSN)" with the license "Datenlizenz Deutschland – Namensnennung – Version 2.0":
Source: **GeoSN**, LoD1, **[dl-de/by-2-0](https://www.govdata.de/dl-de/by-2-0 "Link öffnet in neuem Fenster")** [URI: https://www.geodaten.sachsen.de/downloadbereich-digitale-3d-stadtmodelle-4875.html]
- GeoTIFF:
The example GeoTIFF data is a part of the "World Settlement Footprint (WSF®) 2019" and are taken from "Deutsches Zentrum für Luft- und Raumfahrt (DLR)" with the license CC-BY-4.0:
Source: **DLR**, WSF2019, **[CC-BY-4.0](https://creativecommons.org/licenses/by/4.0/)** [DOI: [10.15489/twg5xsnquw84](https://doi.org/10.15489/twg5xsnquw84)]


## Troubleshooting
### Valid Geometries
Make sure that you use valid geometries (OGC compliant) as input in the case of GeoJSON. Although validity is checked and repaired if necessary during import, but this can lead to undesirable effects. For example, the GeoJSONs in `input_example/geojson` have invalid geometries which are successfully repaired during the import. However, this cannot be guaranteed.

### PostgreSQL logs
The logging function of the PostgreSQL Docker container is disabled by default. To enable logs start your compose project without the detach mode (`docker-compose up`). Then you will see the logs on your console. 

If *Docker for Windows* is used, the container *cluster_postgis_container* can also be opened (click on view details), where the logs are then displayed in the Logs tab.

You could also enable logging to files by script:
```bash
docker exec cluster_postgis_container python3.9 /cluster/cluster_scripts/set_logging.py
```
Then you will find the log files in the folder `.../dbdata/log`

### Inspecting the tables
It could be helpful to inspect the tables of your PostgreSQL DB. For this you can use pgAdmin. There is a Docker Compose yaml file including pgAdmin, wich can be run via `docker-compose -f docker-compose_with_PGAdmin.yaml up -d`. pgAdmin can then be reached via your browser on `localhost:5050`.  To log in to pgAdmin, use `admin@admin.com` as *Email Adress* and `root` as *Password*. Register a new server with the following connection settings (taken from the default .env file):

- Hostname: cluster_postgis_container
- Port: 5432
- Database: cluster_db
- Username: postgres
- Password: secret

If you do not need the pgAdmin container that comes with the Docker Compose file above, you can of course use any other inspection tool in your local environment (e.g. QGIS or an already installed pgAdmin). But then the PostgreSQL DB is connectable via the hostname `localhost` with the port `5431` (note the port, normally it is 5432). 

In the table *cluster_inputs* you see the imported objects. In the table *cluster_extents* are the extents of the tiles stored. And in the table *cluster_results* you will find the clustered objects. If you have succesfully clustered some data you should see a column like *gcid_d100*, whereby 100 stands for 100 meters.

### Cleaning the DB
If the clustering process crashes, you may have hundreds or even thousands of tables in your DB. If you want to get rid of these, you can use the script `__drop_all_tables.py` to delete all the intermediate tables. The “core tables” (*cluster_inputs*, *cluster_extents* and *cluster_results*) remain untouched.
```bash
docker exec cluster_postgis_container python3.9 /cluster/cluster_scripts/__drop_all_tables.py -d 100
```
The `-d` argument must specify the distance at which the clustering process crashed.

### PC utilization
Take a look at your CPU, RAM and disk usage to ensure that they are not fully utilized. 

>[!TIP]
>The little postgis elephant is very hungry and will eat the RAM you give him. But the greedy little glutton won't just give it back! The only way to free up the RAM is to stop/restart PostgreSQL (or the pg container).

### Safety note
>[!CAUTION]
>Do not use the tool in a web application without further hardening. Mainly plain sql commands are executed, which are not protected against sql injections!

### Create an Issue
If you find a bug or have problems, you can of course create an issue.
