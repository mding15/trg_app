# -*- coding: utf-8 -*-
"""
cp_remote_client_files.py — Copy a client's data folder from a remote AWS
server to local config['CLIENT_DIR'].

Copies /home/ec2-user/api/data/clients/{client_id} on the remote server to
config['CLIENT_DIR']/{client_id} locally, via utils.scp_utils.copy_client_portfolio().

Usage:
    python maintenance/cp_remote_client_files.py 1018
    python maintenance/cp_remote_client_files.py 1018 --server dev2
"""
import argparse

from utils import scp_utils


def main():
    parser = argparse.ArgumentParser(
        description="Copy a client's files from a remote AWS server to local config['CLIENT_DIR']."
    )
    parser.add_argument("client_id", help="client_id whose folder to copy")
    parser.add_argument("--server", choices=["prod2", "dev2"], default="prod2",
                         help="remote server to copy from (default: prod2)")
    args = parser.parse_args()

    scp_utils.copy_client_portfolio(args.client_id, server=args.server)


if __name__ == "__main__":
    main()
