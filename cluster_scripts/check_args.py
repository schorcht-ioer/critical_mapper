import sys
import argparse

################## type definitions

def check_positive_int(value):
    try:
        ivalue = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError("%r is an invalid positive integer value!" % (value,))

    if ivalue <= 0 or str(int(value)) != value:
        raise argparse.ArgumentTypeError("%s is an invalid positive integer value!" % value)
    return ivalue       

def check_positive_float(value):
    try:
        ivalue = float(value)
    except ValueError:
        raise argparse.ArgumentTypeError("%r is an invalid positive floating-point value!" % (value,))
        
    if ivalue <= 0:
        raise argparse.ArgumentTypeError("%s is an invalid positive floating-point value!" % value)
    return ivalue  

def check_limited_float(value):
    try:
        ivalue = float(value)
    except ValueError:
        raise argparse.ArgumentTypeError("%r is an invalid positive floating-point value!" % (value,))
        
    if ivalue <= 0.002:
        raise argparse.ArgumentTypeError("%s floating-point value should be greater than 0.002 !" % value)
    return ivalue  

def distance_list(arg):
    return list(map(int, arg.split(',')))

def distance_range(arg):
    if len(arg.split(',')) != 3:
        raise argparse.ArgumentTypeError(f"Argument for --range was '{arg}' does not have 3 values (start,stop,step)!")
    else:
        start, stop, step = arg.split(',')        
    return list(range(int(start), int(stop)+1, int(step)))

def check_bool(v):
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError(f"Argument for --export was '{v}', but Boolean value is expected!")

################## arguments definitions

def arg_picker(parser, name):

    if name == 'distance':
        parser.add_argument(
            "-d",
            "--distance",     
            type=check_positive_int,
            help='Set positive distance value in meters, like: -d 100',
            required=True
        )

    elif name == 'distances':
        parser.add_argument(
            "-d",
            "--distances",     
            type=distance_list,
            help='Set one [or more] distance[s] in meters as comma separated list, like: -d 100 [or: -d 100,200]',
            required=True
        )

    elif name == 'distances_opt':
        parser.add_argument(
            "-d",
            "--distances",     
            type=distance_list,
            help='Set one [or more] distance[s] in meters as comma separated list, like: -d 100 [or: -d 100,200]'
        )

    elif name == 'distances_range':
        parser.add_argument(
            "-r",
            "--range",     
            type=distance_range,
            help='Set range of distances in meters with start, stop and step, like: -r 100,1100,100'
        )

    elif name == 'nodata':
        parser.add_argument(
            "-n",
            "--nodata",     
            type=float,
            help='Set nodata value (if it is not already correctly defined in the metadata), like: -n 255'
        )

    elif name == 'grid_size':
        parser.add_argument(
            "-g",
            "--grid_size",     
            type=check_limited_float,
            help='Set grid size value in degree, like: -g 2.5',
            required=True
        )

    elif name == 'print_steps':
        parser.add_argument(
            "-p",
            "--print_steps",     
            type=check_positive_int,
            default=1,
            help='Prints every N steps informations about the status of the tiles, like: -p 10'
        )

    elif name == 'export':
        parser.add_argument(
            "-e",
            "--export",     
            type=check_bool,
            default='False',
            help='Export the clustered data'
        )

    elif name == 'delete_temp':
        parser.add_argument(
            "-del",
            "--delete_temp",     
            type=check_bool,
            default='False',
            help='Delete temporary data (projected or tiled tiffs and db-data)'
        )

    elif name == 'tile_size':
        parser.add_argument(
            "-t",
            "--tile_size",     
            type=check_positive_int,
            help='Set the number of pixels to split the input raster into tiles, like: -t 250000',
            required=True
        )

    elif name == 'dateline_distance':
        parser.add_argument(
            "-l",
            "--dateline_distance",     
            type=check_positive_int,
            default=1,
            help='Distance to dateline in meters. This should be the maximum cluster distance, like: -l 5000'
        )
    else:
        print(f'Error: cant find argument {name} in arg_picker!')
        sys.exit()  

    return parser


################## set argument combos

def get_dropper_args(args):

    parser = argparse.ArgumentParser('table dropper')
    optional = parser._action_groups.pop()
    required = parser.add_argument_group('required arguments')
    parser._action_groups.append(optional)

    arg_picker(required,'distance')

    args = parser.parse_args(args)

    return args.distance

def get_import_tiff_args(args):
 
    parser = argparse.ArgumentParser('import tiff files') 
    optional = parser._action_groups.pop()
    required = parser.add_argument_group('required arguments')
    parser._action_groups.append(optional)

    arg_picker(required,'tile_size')   
    arg_picker(optional,'nodata')
    arg_picker(optional,'dateline_distance')
    args = parser.parse_args(args)

    return args.tile_size, args.nodata, args.dateline_distance

def get_import_geojson_args(args):
 
    parser = argparse.ArgumentParser('import geojson files')
    optional = parser._action_groups.pop()
    required = parser.add_argument_group('required arguments')
    parser._action_groups.append(optional)

    arg_picker(required,'grid_size')
    arg_picker(optional,'dateline_distance')

    args = parser.parse_args(args)

    return args.grid_size, args.dateline_distance

def get_cluster_args(args):

    parser = argparse.ArgumentParser('cluster data')
    optional = parser._action_groups.pop()
    required = parser.add_argument_group('required arguments')
    parser._action_groups.append(optional)

    arg_picker(required,'distance')

    arg_picker(optional,'print_steps')

    args = parser.parse_args(args)

    return args.distance, args.print_steps

def get_loop_cluster_args(args):
    parser = argparse.ArgumentParser('Clustering data with multiple distances')
    optional = parser._action_groups.pop()
    # add either or parameter
    either_or = parser.add_mutually_exclusive_group(required=True)
    parser._action_groups.append(optional)

    arg_picker(either_or,'distances_opt')
    arg_picker(either_or,'distances_range')

    arg_picker(optional,'print_steps')
    arg_picker(optional,'export')
    arg_picker(optional,'delete_temp')

    args = parser.parse_args(args)

    return args.distances, args.range, args.print_steps, args.export, args.delete_temp

def get_splitter_args(args):

    parser = argparse.ArgumentParser('raster tile')
    optional = parser._action_groups.pop()
    required = parser.add_argument_group('required arguments')
    parser._action_groups.append(optional)

    arg_picker(required,'tile_size')

    args = parser.parse_args(args)

    return args.tile_size

def get_crit_dist_args(args):

    parser = argparse.ArgumentParser('critical distance')
    optional = parser._action_groups.pop()
    parser._action_groups.append(optional)

    arg_picker(optional,'print_steps')

    args = parser.parse_args(args)

    return args.print_steps    