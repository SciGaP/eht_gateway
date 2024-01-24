"""
    input data:
        -- ehtdata: EHT HDF5 Data: torus.out0.05992.h5,torus.out0.05993.h5,torus.out0.05994.h5
        -- rr: Rhigh-Ratio: 160
        -- tva: Theta-Viewing-Angle: 30
        -- rdm: rho0-density-normalization: 1.4705331615886175e+18
    positioned parameters:
    python eht_submit.py ehtdata rr tva rdm
    python eht_submit.py torus.out0.05992.h5,torus.out0.05993.h5 140,150,160 10:100:10 1.4705331615886175e+18
"""


import sys
import json
import os
from datetime import datetime
import argparse
import itertools

workdir = "./"

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
    timestamp_string = f"{year:04d}-{month:02d}-{day:02d}_{hour:02d}-{minute:02d}-{second:02d}"

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

def main():
    # dump input parameter to a json file
    print_input_parameters()

    try:
        args = parse_arguments()
    except TypeError:
        args.print_help()
        sys.exit()

    alljobs = generate_alljobs(args)
    print(alljobs)
    print("total: ",len(alljobs))

if __name__ == "__main__":
    main()