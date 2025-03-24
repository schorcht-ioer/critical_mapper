import os
from pathlib import Path
import time
import fileinput
import check_args as ca
import argparse, config

def file_path(path):
    if os.path.isfile(path):
        if path.split('.')[-1] == 'conf':
            return path
        else:
            raise argparse.ArgumentTypeError(f"Error on postgresql_conf: {path} is not a *.conf file")
    else:
        raise argparse.ArgumentTypeError(f"Error on postgresql_conf: {path} is not a valid path")


def get_args():

    parser = argparse.ArgumentParser('configurator')
    optional = parser._action_groups.pop()
    required = parser.add_argument_group('required arguments')
    parser._action_groups.append(optional)

    required.add_argument(
        "-c",
        "--cpu",     
        type=ca.check_positive_int,
        help='Set number of usable CPUs',
        required=True
    )

    required.add_argument(
        "-r",
        "--ram",     
        type=ca.check_positive_int,
        help='Set ammount of RAM to get used (in GB)',
        required=True
    )

    optional.add_argument(
        "-u",
        "--username",   
        default = config.CLUSTER_POSTGRES['user'],
        help='Set the username for PostgreSQL'
    )


    args = parser.parse_args()

    return args.cpu, args.ram, args.username

def replace_in_file(line_start, replace_line, file_path):

    with fileinput.input(file_path, inplace=True) as file:
        for line in file:
            if line.startswith(line_start):
                print(replace_line)
            else: 
                print(line, end='')


if __name__ == '__main__':

    cpu, ram, username = get_args()

    shared_buffers = ram * 0.20

    maintenance_work_mem = ram * 0.05 if (ram * 0.05) <= 1.9 else 1.9 

    work_mem = ram * 0.25 / cpu if (ram * 0.25 / cpu) <= 2.0 else 2.0

    temp_buffers = ram * 0.25 / cpu if (ram * 0.25 / cpu) <= 1.0 else 1.0

    effective_cache_size = ram * 0.5  # - (work_mem * cpu)


    print('\nThe following configurations wil be set:')
    print('cpu_number:',cpu, 'GB')
    print('work_mem:',work_mem, 'GB')
    print('temp_buffers:',temp_buffers, 'GB')
    print('effective_cache_size:',effective_cache_size, 'GB')
    print('maintenance_work_mem:',maintenance_work_mem, 'GB')

    print('\nThe following changes set in postgresql_conf:')
    print('shared_buffers:',shared_buffers, 'GB')
    print('max_wal_size: 16 GB')
    print('checkpoint_timeout: 120min')

    print('\nusing:')
    print('postgresql_conf:',config.PG_CONF_PATH)
    print('username:',username)

    # change to current directory
    os.chdir(Path(__file__).resolve().parent)
 
    # replace cpu in config.py
    replace_in_file('NUM_PROC = ', f'NUM_PROC = {cpu}', 'config.py')

    # replace shared buffers in postgresql.conf
    replace_in_file('shared_buffers = ', f'shared_buffers = {round(shared_buffers*1024)}MB			# min 128kB', config.PG_CONF_PATH)

    # replace max_wal_size in postgresql.conf
    replace_in_file('max_wal_size = ', f'max_wal_size = 16GB', config.PG_CONF_PATH)

    # replace checkpoint_timeout in postgresql.conf
    replace_in_file('#checkpoint_timeout = ', f'checkpoint_timeout = 120min		# range 30s-1d', config.PG_CONF_PATH)
 
    # replace the others params sql_conf.py
    for param in [work_mem, temp_buffers, effective_cache_size, maintenance_work_mem]:
        param_name = [ k for k,v in locals().items() if v is param][0]
        replace_in_file(f'SET {param_name} TO ', f"SET {param_name} TO '{param}GB';", 'sql_conf.py')

    # restart postgresql
    if os.name != 'nt':
        try:
            os.system(f"runuser -l {username} -c '{config.PG_CTL_PATH} -D {config.PG_DATA_PATH} restart'")
        except:
            print('Could not restart PostgreSQL. Please restart PostgreSQL manually for the new conf settings to take effect!')           
    else:
        print('OS is Windows. Please restart PostgreSQL manually for the new conf settings to take effect!')    

 










