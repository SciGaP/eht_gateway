#!/usr/bin/env python3
"""
    input data:
        -- ehtdata: EHT HDF5 Data: torus.out0.05992.h5,torus.out0.05993.h5,torus.out0.05994.h5
        -- rr: Rhigh-Ratio: 160
        -- tva: Theta-Viewing-Angle: 30
        -- rdm: rho0-density-normalization: 1.4705331615886175e+18
    positioned parameters:
        python eht_submit.py ehtdata rr tva rdm
    a single job example
        python eht_submit.py torus.out0.05992.h5  160 30 1.4705331615886175e+18
    a multiple job example
        python eht_submit.py torus.out0.05992.h5,torus.out0.05993.h5 140,150,160 10:100:10 1.4705331615886175e+18
"""


import sys
import json
import os
import csv
from datetime import datetime
import argparse
import itertools

workdir = "./"
# md5 is from GRMHD_kharma-v3/md5/md5_Ma+0.94_w5.tsv
md5s = {
"torus.out0.05990.h5":	"b9e0afbecddda32ef5db0a9e62b615c4",
"torus.out0.05991.h5":	"5a9937884a8d9fb0c25216630357cdd8",
"torus.out0.05992.h5":	"958f170bcca33a9d5781f1517e95a3b2",
"torus.out0.05993.h5":	"af523f603eb619945acd7427c345b17c",
"torus.out0.05994.h5":	"f49e41fd33d7e9cf83eab6d5ac620e36",
"torus.out0.05995.h5":	"02632c573489f2dcfa1c91d54d2780f4",
"torus.out0.05996.h5":	"3574d138e63a7f4cdc6d1fd01f0ada56",
"torus.out0.05997.h5":	"f5658f308936d9e90ca0724487b25950",
"torus.out0.05998.h5":	"3263ce053a165dae8ac437b28ceadfce",
"torus.out0.05999.h5":	"cc2a5705eede5415dbd92bd4406eb1c1"
}
# h5 file folder
h5folder = os.path.expanduser("~/GRMHD_kharma-v3/Ma+0.94_w5/") 


def generate_timestamp_string():
    """
    Generates a timestamp string in the format YYYY-MM-DD_HH-MM-SS.

    Returns:
    A string representing the current timestamp.
    """
    now = datetime.now()
    year = now.year
    month = now.month
    day = now.day
    hour = now.hour
    minute = now.minute
    second = now.second

    # Pad single-digit values with zeros
    #timestamp_string = f"{year:04d}-{month:02d}-{day:02d}_{hour:02d}-{minute:02d}-{second:02d}"
    timestamp_string = datetime.datetime.now().isoformat()
    
    return timestamp_string

def print_input_parameters():
    """
    Prints all the input parameters passed to the script.
    """

    job_name = "job.json"
    job_json = os.path.join(workdir,job_name)
    with open(job_json, "w") as f:
        json.dump({f"arg_{i+1}": arg for i, arg in enumerate(sys.argv)},f)

def parse_arguments():
    """
    Parses command-line arguments using argparse.

    Returns:
        Namespace object containing parsed arguments.
    """
    parser = argparse.ArgumentParser(description="My function description")
    parser.add_argument("ehtdata", type=str, help="ehtdata input files")
    parser.add_argument("rr", type=str, help="Rhigh-Ratio")
    parser.add_argument("tva", type=str, help="Theta-Viewing-Angle")
    parser.add_argument("rdm", type=str, help="rho0-density-normalization")
    return parser.parse_args()

def parse_values(astr):
    """parse three type of inputs
        single value
        a list of values: ","
        a range: start:stop:step
    """

    if ":" in astr:
        start, stop, step = map(int, astr.split(":"))
        alist = list(range(start,stop,step))
        if not stop in alist:
            alist.append(stop)
        alist = map(str, alist)
    elif "," in astr:
        alist = astr.split(",")
    else:
        alist = [astr]
    
    return alist

def generate_alljobs(paras):
    """ generate a list of the combination of all parameters"""

    V = {}
    for key, value in vars(paras).items():
        #print(key, value)
        value_list = parse_values(value)
        #print(value_list)
        V[key] = value_list

    # use the name, and make sure the parameters are in the order
    listoflists = [V["ehtdata"],V["rr"],V["tva"],V["rdm"]]
    para_combines = itertools.product(*listoflists)
    return list(para_combines)

def generate_batch(joblist):
    '''generate BATCH.ALL'''

    parfolder = os.path.join(workdir,"par")
    par_all = "BATCH.ALL"
    par_d00 = "BATCH.DOO"

    batchdata = []
    for job in joblist:
        # inputh5,md5,namepart, Rhigh, inclination, rho0
        inputh5 = job[0]
        md5 = md5s[inputh5]
        namepart = inputh5.split(".")[2]
        rhigh,inclination, rho0 = job[1:]
        batchdata.append([inputh5,md5,namepart,rhigh,inclination,rho0])

    # write the list to file
    with open(os.path.join(parfolder,par_all), 'w', newline='') as file:
        writer = csv.writer(file,delimiter=',')
        writer.writerows(batchdata)    


def main():
    # dump input parameter to a json file
    #print_input_parameters()

    try:
        args = parse_arguments()
    except TypeError:
        args.print_help()
        sys.exit()

    alljobs = generate_alljobs(args)
    print(alljobs)
    print("total: ",len(alljobs))
    generate_batch(alljobs)

if __name__ == "__main__":
    main()