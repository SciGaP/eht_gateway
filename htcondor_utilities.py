"""
    htcondor_utilities.py
        -- assistant function to connect to osg node
        -- job submission
        -- job status report
    
    configuration:
        .env contains ssh connection parameters
            put it the values for the following keys:
                - userA,sshA,keyA,passA
                - userB,sshB,keyB,passB
"""

import os
import sys

from dotenv import load_dotenv
from fabric import Connection
import re

# load server setup
load_dotenv()


def run_ssh_cmd(cmd_str):
    """run a command in ssh"""

    gateway = {
        "host": os.getenv("sshA"),
        "user": os.getenv("userA"),
        "connect_kwargs": {
            "key_filename": os.path.expanduser(os.getenv("keyA")),
            "passphrase": os.getenv("passA"),
        },
    }

    target_host = {
        "host": os.getenv("sshB"),
        "user": os.getenv("userB"),
        "connect_kwargs": {
            "key_filename": os.path.expanduser(os.getenv("keyB")),
            "passphrase": os.getenv("passB"),
        },
    }

    # Connect to the target host via the gateway
    with Connection(**target_host, gateway=Connection(**gateway)) as conn:
        result = conn.run(cmd_str, hide=True)
        return result


def extract_numbers_from_line(line):
    """find all the numbers from line"""
    numbers = re.findall(r"\d+", line)
    return list(map(int, numbers))


def htcondor_status():
    """query the htcondor status"""

    condor_q = run_ssh_cmd("condor_q")
    status_line = [x for x in condor_q.stdout.split("\n") if "Total for query" in x]
    if status_line == []:
        print("job status is not available, try it later.")
        return
    # "Total for query: 50 jobs; 40 completed, 2 removed, 3 idle, 2 running, 2 held, 1 suspended"
    status_line = status_line[0]
    numbers = extract_numbers_from_line(status_line)
    keys = ["jobs", "completed", "removed", "idle", "running", "held", "suspended"]
    status = dict(zip(keys, numbers))
    return status

def job_submission(paras,dryrun=False):
    """submit a job with paras"""

    from jobsubmission.eht_submit import alljobs_dict
    all_jobs =alljobs_dict(paras)

    if dryrun:
        print("total jobs: ", len(all_jobs))
        print("ready to submit")
        return
    
    script = os.getenv("script")
    ehtdata = paras['ehtdata']
    rr = paras['rr']
    tva = paras['tva']
    rdm = paras['rdm']
    submitcmd = f"{script} {ehtdata} {rr} {tva} {rdm}"
    output = run_ssh_cmd(submitcmd)
    print(output.stdout)

    return

def run_testjob(submit=False):
    """a test case"""
    job_paras = {}    
    job_paras["ehtdata"] = "torus.out0.05992.h5,torus.out0.05993.h5,torus.out0.05994.h5,torus.out0.05995.h5,torus.out0.05996.h5"
    job_paras["rr"] = "10:100:10"
    job_paras["tva"] = "30,50,60,70,80,90"
    job_paras["rdm"] = "4.266338570441294e+17"

    print("A test case with the following parameters:")
    print("Ehtdata: ", job_paras["ehtdata"])
    print("Rhigh-Ratio: ", job_paras["rr"])
    print("Theta-Viewing-Angle: ", job_paras["tva"])
    print("Rho0-Density-Normalization: ", job_paras["rdm"])
    
    dryrun = True
    if submit:
        dryrun = False
    job_submission(job_paras, dryrun=dryrun)

def main():
    """test the routines"""
    output = run_ssh_cmd("hostname")
    print("stdout:", output.stdout)
    print("stderr:", output.stderr)

    status = htcondor_status()
    print(status)

    run_testjob(submit=False)

if __name__ == "__main__":
    sys.exit(main())
