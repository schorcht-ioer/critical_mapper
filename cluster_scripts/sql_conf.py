### settings only for processing state

sql_proccesing_conf="""SET max_parallel_workers = 1;
SET max_parallel_workers_per_gather = 0;
SET min_parallel_index_scan_size = '12kB';
SET min_parallel_table_scan_size ='12kB';
SET work_mem TO '1.0GB';
SET effective_cache_size TO '16.0GB';
SET maintenance_work_mem TO '1.6GB';
SET random_page_cost TO '1';
SET temp_buffers TO '1.0GB';
--SET min_wal_size = '4GB';
--SET max_wal_size = '16GB';

"""

# sql_default_conf="""SET max_parallel_workers = 8;
# SET max_parallel_workers_per_gather = 2;
# SET min_parallel_index_scan_size = '512kB';
# SET min_parallel_table_scan_size ='8MB';
# SET work_mem TO '512MB';
# SET effective_cache_size TO '4GB';
# SET maintenance_work_mem TO '64MB';
# SET random_page_cost TO '4';
# """