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


def main():
    """test the routines"""
    output = run_ssh_cmd("hostname")
    print("stdout:", output.stdout)
    print("stderr:", output.stderr)

    status = htcondor_status()
    print(status)


if __name__ == "__main__":
    sys.exit(main())
