import os
from pathlib import Path
import check_args as ca
import argparse, config

def get_args():

    parser = argparse.ArgumentParser('configurator')
    optional = parser._action_groups.pop()
    required = parser.add_argument_group('required arguments')
    parser._action_groups.append(optional)

    optional.add_argument(
        "-e",
        "--enable",     
        type=ca.check_bool,
        default='True',
        help='Set True to enable logging'
    )

    optional.add_argument(
        "-u",
        "--username",   
        default = config.CLUSTER_POSTGRES['user'],
        help='Set the username for PostgreSQL'
    )


    args = parser.parse_args()

    return args.enable, args.username


if __name__ == '__main__':

    enable, username = get_args()

    # change to current directory
    os.chdir(Path(__file__).resolve().parent)
    
    #logging_collector
    if enable:
        os.system(f"""sed -i.bak 's/#logging_collector = off/logging_collector = on/g' {config.PG_CONF_PATH}""")
    else:
        os.system(f"""sed -i.bak 's/logging_collector = on/#logging_collector = off/g' {config.PG_CONF_PATH}""")

    # restart postgresql
    os.system(f"runuser -l {username} -c '{config.PG_CTL_PATH} -D {config.PG_DATA_PATH} restart'")