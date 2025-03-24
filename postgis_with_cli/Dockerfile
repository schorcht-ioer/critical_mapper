FROM postgis/postgis:15-3.4

# install client tools (raster2pgsql, shp2pgsql)
RUN apt-get update -y
RUN apt-get install postgis -y
RUN apt-get clean
RUN rm -rf /var/cache/apt/lists

# install gdal (ogr2ogr)
RUN apt-get install gdal-bin -y

# create raster extension
COPY postgis_with_cli/create-extension.sql /docker-entrypoint-initdb.d/

# install pip
RUN apt-get install python3-pip -y

# install sqlalchemy
RUN pip3 install psycopg2-binary==2.9.3 SQLAlchemy==1.4.41

