#!/bin/bash

LOG_FILE="/home/ec2-user/brokers/mssb/log/mssb_sftp.log"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"; }

log "Starting MSSB SFTP job..."

/home/ec2-user/brokers/mssb/bin/pull_ms_file.sh >> "$LOG_FILE" 2>&1

log "MSSB SFTP job completed."
