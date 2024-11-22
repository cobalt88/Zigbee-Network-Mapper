import sys

from utils.ingest_data import process_data
from utils.merge_files import merge_files
from utils.znac import znac
from utils.reports import lqi_comp_report

args = sys.argv[1:]



def main(*args):

    if "ingest" in args[0] and "hist_lqi_comp" in args[0]:
        print("Ingesting data and calculating LQI")
        process_data()
        lqi_comp_report()
    
    if "merge" in args[0]:
        print("Merging files")
        # merge_files()

    if "znac" in args[0]:
        print("Calculating ZNAC")
        znac()

    # if "hist_lqi_comp" in args[0]:
    #     print("Calculating LQI")
        

main(args)