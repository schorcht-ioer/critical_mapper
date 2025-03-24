import config
from multiprocessing import Pool

def run_parallel(func, arg_list):

    with Pool(processes=config.NUM_PROC) as pool:        
        # issue tasks to the process pool
        pool.imap_unordered(func, arg_list)
        # shutdown the process pool
        pool.close()
        # wait for all issued task to complete
        pool.join()
