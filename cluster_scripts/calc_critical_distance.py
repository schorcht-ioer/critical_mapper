from multiprocessing import Pool
import multiprocessing 
import datetime
import os
import sys

import check_args, config, pg_engine, parallel_runner as pr, import_borders as i, export_borders as e

# print every N portion
global print_step
print_step =  1

# input cluster
cluster_results_table = config.results_table

# borders:
border_table = config.borders_table
border_results = config.crit_dist_table

# set numbers of borders
global borders_cnt
borders_cnt = 0

def get_borders_cnt():
    return int(pg_engine.get_sql(f"""select count(*) from {border_table}""")[0][0])

# column names of allready calculatet gcid
def get_dist_columns():

    col_names = pg_engine.get_sql(f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
        AND table_name   = '{cluster_results_table}'
        ORDER BY NULLIF(regexp_replace(column_name, '\D','','g'), '')::numeric
        ;""")

    dist_col_names = ''

    for col in col_names:
        if ('gcid_d' in col[0]):
            dist_col_names += ', ' + col[0]

    dist_col_names = dist_col_names[2:]

    return dist_col_names  

# column names of border input
def get_border_columns():

    col_names = pg_engine.get_sql(f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
        AND table_name   = '{border_table}'
        ORDER BY NULLIF(regexp_replace(column_name, '\D','','g'), '')::numeric
        ;""")

    border_col_names = ''

    for col in col_names:
        if (col[0] != 'critical_id'):
            border_col_names += ', ' + col[0]

    border_col_names = border_col_names[2:]

    return border_col_names      

dist_columns_str = get_dist_columns()
dist_columns_list = dist_columns_str.split(', ')


def analyze_windows(this_proc):

    #### create function
    perc_dist_f_name = 'get_perc_dist_p' + str(this_proc) 
    calc_area_f_name = 'calc_area_p' + str(this_proc)
    calc_overlap_f_name = 'calc_overlap_p' + str(this_proc)

    pg_engine.run_sql(f"""
        drop function if exists {perc_dist_f_name};
        drop function if exists {calc_area_f_name};
        drop function if exists {calc_overlap_f_name};
    """)


    # queries for calc perc_dist
    def query_biggest():
        q = ""
        for r in dist_columns_list:
            r_name = r[6:]
            q+=f"""
                biggest_2to4_{r_name} as (
                SELECT SUM(pxl_area) as areasum
                FROM tmp_intersect
                GROUP BY {r}
                ORDER BY areasum desc
                OFFSET 1 LIMIT 3
                ),
            """	
        return q
    
    def query_mean():
        q = f"""
            means as (
                select"""

        for r in dist_columns_list:
            r_name = r[6:]
            q+=f"""
                (select COALESCE( SUM(areasum)/3, 0 ) from biggest_2to4_{r_name}) as mean_2to4_{r_name},"""	
        q = q[:-1] + "),"	

        return q

    def query_drop_off():
        q = f"""
            drop_offs as (
                select 0 as drop_off, {dist_columns_list[0][6:]} as distance from means
                union"""

        for r in dist_columns_list[:-1]:
            r_name = r[6:]
            next_index = dist_columns_list.index(r) + 1
            r_next_name = dist_columns_list[next_index][6:]
            q+=f"""			
                select (mean_2to4_{r_name} - mean_2to4_{r_next_name}) as drop_off, {r_next_name} as distance from means
                union"""
        q = q[:-5] + ")"	

        return q


    # create function for perculation distance
    create_per_dist_f = f"""
        CREATE OR REPLACE FUNCTION {perc_dist_f_name}() RETURNS integer AS $$
            with 
                {query_biggest()} 
                {query_mean()} 
                {query_drop_off()}
            select 
                distance
                from drop_offs
                order by drop_off desc, distance limit 1;
        $$ LANGUAGE SQL;    
     """

    # create calc area function
    create_calc_area_f = f"""
        CREATE OR REPLACE FUNCTION {calc_area_f_name}() RETURNS float AS $$ 
			SELECT SUM(pxl_area) as total_area FROM tmp_intersect
        $$ LANGUAGE SQL;    
    """

    create_calc_overlap_f = f"""
        CREATE OR REPLACE FUNCTION {calc_overlap_f_name}(_total_area float) RETURNS NUMERIC(8, 5) AS $$ 			

            with sum_border_area as (
            	select ST_Area(geom::geography)/(1000*1000) as border_area from {border_table} WHERE critical_id = {this_proc}
            ),
			overlap_ratio as (
				select ROUND((100.0 / NULLIF(border_area,0) * _total_area)::numeric, 5) as overlap from sum_border_area
			)	
            select case 
				when overlap is NULL then NULL
				when overlap > 100 then 100.0
                when overlap < 0 then 0.0
				when overlap >= 0 and overlap <= 100 then overlap
				end overlap
			from overlap_ratio

        $$ LANGUAGE SQL;    
    """
    # create buffer window around center point
    pg_engine.run_sql(f"""

        ---- select cluster inside investigation area
        create temp table tmp_intersect as
            with inters_cluster as (                
                SELECT critical_id, {dist_columns_str}, ST_Intersection(a.geom, ST_MakeValid(b.geom)) as geom  
                from {border_table}_subdiv a, {cluster_results_table} b
                WHERE a.critical_id = {this_proc}
                AND ST_Intersects(a.geom,b.geom)
                )
            -- calc area    
            select critical_id, {dist_columns_str}, ST_Area(geom::geography)/(1000*1000) as pxl_area from inters_cluster;
            
        ---- create functions for this proc_n
        {create_per_dist_f}
        {create_calc_area_f}
        {create_calc_overlap_f}

        ---- save results
        -- calc perc_dist and pxl_area
        with pxl_areas as (
            select 
                critical_id, {perc_dist_f_name}() as perc_dist, {calc_area_f_name}() as total_area, geom 
            from {border_table} where critical_id = {this_proc}
        )
        -- calc overlap
        insert into border_results_temp (critical_id, perc_dist, total_area_sqkm, overlap, geom)
            select 
                critical_id, perc_dist, total_area as total_area_sqkm, {calc_overlap_f_name}(total_area) as overlap, geom 
            from pxl_areas;

        drop function if exists {perc_dist_f_name};
        drop function if exists {calc_area_f_name};
        drop function if exists {calc_overlap_f_name};
    

    """)

    ###### print status  
    # print progress in percent
    if (this_proc % print_step == 0):
        print(f"Finished border {str(this_proc)} of {str(borders_cnt)} [~ {int(100/borders_cnt*this_proc)}%]")    

def preprocessing():

    # add spatial index on cluster results 
    pg_engine.run_sql(f"""
        CREATE INDEX IF NOT EXISTS idx_{cluster_results_table}_poly_gist ON {cluster_results_table} USING gist (geom);""")

    print("Create spatial index on input clusters finished")       

    # create subdivided borders
    pg_engine.run_sql(f"""
        DROP TABLE IF EXISTS {border_table}_subdiv;
        CREATE TABLE {border_table}_subdiv as 
            select critical_id, ST_Subdivide(geom) as geom from {border_table};
        CREATE INDEX IF NOT EXISTS idx_{border_table}_subdiv_gist ON {border_table}_subdiv USING gist (geom);   
        CREATE INDEX IF NOT EXISTS idx_{border_table}_subdiv_critical_id ON {border_table}_subdiv (critical_id);  
        """)  
    print("Subdivide borders finished")    

    # create results table
    pg_engine.run_sql(f"""
        DROP TABLE IF EXISTS border_results_temp;
        CREATE TABLE border_results_temp (
        critical_id INT,
        perc_dist INT,
        total_area_sqkm FLOAT,
        overlap NUMERIC(8, 5),
        geom GEOMETRY(GEOMETRY,4326));""")  

def postprocessing():

    # create final results table 
    pg_engine.run_sql(f"""
        DROP TABLE IF EXISTS {border_results};
        CREATE TABLE {border_results} as 
            with results_temp as (
                select critical_id, perc_dist, total_area_sqkm, overlap, 
                CASE
				    when overlap is NULL then 'nodata'
				    when overlap > 0 and perc_dist = {dist_columns_list[0][6:]} then 'not percolated'
                    when overlap > 0 and perc_dist > {dist_columns_list[0][6:]} then 'percolated'
				end status

                from border_results_temp
            )
            SELECT {get_border_columns()}, a.critical_id, perc_dist as critical_distance, ST_Area(geom::geography)/(1000*1000) as border_area_sqkm, total_area_sqkm as cluster_area_sqkm, overlap as share, status from {border_table} a, results_temp b where a.critical_id = b.critical_id ;
        """)

    # remove spatial index and temp tables
    pg_engine.run_sql(f"""
        DROP INDEX idx_{cluster_results_table}_poly_gist; 
        DROP TABLE IF EXISTS {border_table}_subdiv;  
        DROP TABLE IF EXISTS border_results_temp;        
        """)


if __name__ == '__main__':

    # check sys args
    print_step_param = check_args.get_crit_dist_args(sys.argv[1:])

    ##set print_step
    if print_step_param != 1:    
        print_step = print_step_param

    # start time
    start_time = datetime.datetime.now() 

    # import border to db
    i.import_borders()

    # set borders count
    borders_cnt = get_borders_cnt()

    # run db preprocessing
    preprocessing()  

    # get process list
    proc_list = list(range(1,borders_cnt+1))

    # run analyze windows
    pr.run_parallel(analyze_windows, proc_list)

    # delete subdivided borders
    # and remove spatial index of clusters
    postprocessing()

    # export results_borders as json
    e.export_borders()

    # total run time
    run_time = (datetime.datetime.now() - start_time)
        
    print(f"************ analyzing finished in {run_time.days} days, {run_time.seconds//3600} hours, {(run_time.seconds//60)%60} minutes (total {run_time.days *24*60 + run_time.seconds//60} minutes) ***************")