"""
    htcondor_utilities.py
        -- assitant function to connect to osg node
        -- job submission
        -- job status report
"""

import os
import sys
from dotenv import load_dotenv
from fabric import Connection

# load server setup
load_dotenv()

def run_ssh_cmd(cmd_str):
    """run a command in ssh"""

    gateway = {
        'host': os.getenv("sshA"),
        'user': os.getenv("userA"),
        'connect_kwargs' : {'key_filename':os.path.expanduser(os.getenv("keyA")),'passphrase':os.getenv("passA")}
    }
    
    target_host = {
        'host': os.getenv("sshB"),
        'user': os.getenv("userB"),
        'connect_kwargs' : {'key_filename':os.path.expanduser(os.getenv("keyB")),'passphrase':os.getenv("passB")}
    }
    
    # Connect to the target host via the gateway
    with Connection(**target_host,  gateway=Connection(**gateway)) as conn:
        result = conn.run(cmd_str, hide=True)
        print(result.stdout)

def main():
    """ test the routines """
    run_ssh_cmd('hostname')

if __name__ == '__main__':
    sys.exit(main())